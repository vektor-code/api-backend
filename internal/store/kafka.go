package store

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/kubetrace/api-backend/internal/models"
	"github.com/segmentio/kafka-go"
)

const (
	producerQueueSize  = 65536
	producerBatchMax   = 512
	producerFlushEvery = 500 * time.Millisecond
)

// spanProducer batches enriched spans and publishes them to Kafka as JSON
// arrays. The ingestor-backend consumes these and bulk-inserts into ClickHouse.
type spanProducer struct {
	writer *kafka.Writer
	queue  chan *models.Span
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

	p := &spanProducer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(addrs...),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			BatchTimeout:           200 * time.Millisecond,
			RequiredAcks:           kafka.RequireOne,
			AllowAutoTopicCreation: true,
		},
		queue: make(chan *models.Span, producerQueueSize),
	}
	go p.run()
	log.Printf("[kafka] span producer enabled: brokers=%v topic=%s", addrs, topic)
	return p
}

// enqueue queues a span for publishing. Drops when the queue is full so the
// OTLP receive path never blocks on Kafka.
func (p *spanProducer) enqueue(span *models.Span) {
	select {
	case p.queue <- span:
	default:
		log.Printf("[kafka] producer queue full, dropping span %s", span.SpanID)
	}
}

func (p *spanProducer) run() {
	batch := make([]*models.Span, 0, producerBatchMax)
	ticker := time.NewTicker(producerFlushEvery)
	defer ticker.Stop()

	for {
		select {
		case sp := <-p.queue:
			batch = append(batch, sp)
			if len(batch) >= producerBatchMax {
				p.flush(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				p.flush(batch)
				batch = batch[:0]
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.writer.WriteMessages(ctx, kafka.Message{Value: data}); err != nil {
		log.Printf("[kafka] write %d spans failed: %v", len(batch), err)
	}
}
