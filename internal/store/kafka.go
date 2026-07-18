package store

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/segmentio/kafka-go"
)

const (
	defaultProducerQueueSize  = 65536
	defaultProducerBatchMax   = 512
	defaultProducerFlushEvery = 500 * time.Millisecond
	defaultProducerWriteRetry = 3
)

// spanProducer batches enriched spans and publishes them to Kafka as JSON
// arrays. The ingestor-backend consumes these and bulk-inserts into ClickHouse.
type spanProducer struct {
	writer         *kafka.Writer
	queue          chan *models.Span
	done           chan struct{}
	wg             sync.WaitGroup
	closeOnce      sync.Once
	enqueueTimeout time.Duration
	writeRetries   int
	batchMax       int
	flushEvery     time.Duration
}

func newSpanProducer(brokers, topic string) *spanProducer {
	var addrs []string
	for _, b := range strings.Split(brokers, ",") {
		if b = strings.TrimSpace(b); b != "" {
			addrs = append(addrs, b)
		}
	}
	if len(addrs) == 0 || topic == "" {
		return nil
	}

	queueSize := getEnvInt("KAFKA_PRODUCER_QUEUE_SIZE", defaultProducerQueueSize)
	batchMax := getEnvInt("KAFKA_PRODUCER_BATCH_MAX", defaultProducerBatchMax)
	flushMs := getEnvInt("KAFKA_PRODUCER_FLUSH_MS", int(defaultProducerFlushEvery/time.Millisecond))
	enqueueTimeoutMs := getEnvInt("KAFKA_ENQUEUE_TIMEOUT_MS", 250)
	if queueSize <= 0 {
		queueSize = defaultProducerQueueSize
	}
	if batchMax <= 0 {
		batchMax = defaultProducerBatchMax
	}
	if flushMs <= 0 {
		flushMs = int(defaultProducerFlushEvery / time.Millisecond)
	}
	if enqueueTimeoutMs < 0 {
		enqueueTimeoutMs = 0
	}
	requiredAcks := parseRequiredAcks(getEnvDefault("KAFKA_REQUIRED_ACKS", "all"))
	allowAutoTopic := getEnvBoolDefault("KAFKA_ALLOW_AUTO_TOPIC_CREATION", false)

	p := &spanProducer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(addrs...),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			BatchTimeout:           200 * time.Millisecond,
			RequiredAcks:           requiredAcks,
			AllowAutoTopicCreation: allowAutoTopic,
		},
		queue:          make(chan *models.Span, queueSize),
		done:           make(chan struct{}),
		enqueueTimeout: time.Duration(enqueueTimeoutMs) * time.Millisecond,
		writeRetries:   max(0, getEnvInt("KAFKA_WRITE_RETRIES", defaultProducerWriteRetry)),
		batchMax:       batchMax,
		flushEvery:     time.Duration(flushMs) * time.Millisecond,
	}
	p.wg.Add(1)
	go p.run()
	log.Printf("[kafka] span producer enabled: brokers=%v topic=%s acks=%s autoTopic=%v queue=%d batch=%d", addrs, topic, getEnvDefault("KAFKA_REQUIRED_ACKS", "all"), allowAutoTopic, queueSize, batchMax)
	return p
}

// enqueue queues a span for publishing. It waits briefly during pressure so
// transient bursts do not immediately drop telemetry.
func (p *spanProducer) enqueue(span *models.Span) {
	select {
	case <-p.done:
		return
	default:
	}

	select {
	case p.queue <- span:
		return
	case <-p.done:
		return
	default:
	}

	if p.enqueueTimeout <= 0 {
		log.Printf("[kafka] producer queue full, dropping span %s", span.SpanID)
		return
	}

	timer := time.NewTimer(p.enqueueTimeout)
	defer timer.Stop()
	select {
	case p.queue <- span:
	case <-timer.C:
		log.Printf("[kafka] producer queue full for %v, dropping span %s", p.enqueueTimeout, span.SpanID)
	case <-p.done:
	}
}

func (p *spanProducer) run() {
	defer p.wg.Done()
	batch := make([]*models.Span, 0, p.batchMax)
	ticker := time.NewTicker(p.flushEvery)
	defer ticker.Stop()

	for {
		select {
		case sp := <-p.queue:
			batch = append(batch, sp)
			if len(batch) >= p.batchMax {
				p.flush(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				p.flush(batch)
				batch = batch[:0]
			}
		case <-p.done:
			for {
				select {
				case sp := <-p.queue:
					batch = append(batch, sp)
					if len(batch) >= p.batchMax {
						p.flush(batch)
						batch = batch[:0]
					}
				default:
					if len(batch) > 0 {
						p.flush(batch)
					}
					return
				}
			}
		}
	}
}

func (p *spanProducer) flush(batch []*models.Span) {
	data, err := json.Marshal(batch)
	if err != nil {
		log.Printf("[kafka] marshal %d spans: %v", len(batch), err)
		return
	}

	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := p.writer.WriteMessages(ctx, kafka.Message{Value: data})
		cancel()
		if err == nil {
			return
		}

		log.Printf("[kafka] write %d spans failed attempt=%d: %v", len(batch), attempt, err)
		if attempt > p.writeRetries {
			log.Printf("[kafka] dropping %d spans after %d failed kafka writes", len(batch), attempt)
			return
		}
		if !sleepOrDone(p.done, producerBackoff(attempt)) {
			return
		}
	}
}

func (p *spanProducer) Close() error {
	p.closeOnce.Do(func() { close(p.done) })
	p.wg.Wait()
	return p.writer.Close()
}

func parseRequiredAcks(value string) kafka.RequiredAcks {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "none", "0":
		return kafka.RequireNone
	case "one", "1":
		return kafka.RequireOne
	default:
		return kafka.RequireAll
	}
}

func producerBackoff(attempt int) time.Duration {
	backoff := time.Duration(1<<min(attempt-1, 5)) * 250 * time.Millisecond
	if backoff > 5*time.Second {
		return 5 * time.Second
	}
	return backoff
}

func sleepOrDone(done <-chan struct{}, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-done:
		return false
	case <-timer.C:
		return true
	}
}

func getEnvBoolDefault(key string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(getEnvDefault(key, ""))) {
	case "true", "1", "yes", "y", "on":
		return true
	case "false", "0", "no", "n", "off":
		return false
	default:
		return def
	}
}
