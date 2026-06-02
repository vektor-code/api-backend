package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kubetrace/api-backend/internal/api"
	"github.com/kubetrace/api-backend/internal/collector"
	"github.com/kubetrace/api-backend/internal/k8s"
	"github.com/kubetrace/api-backend/internal/store"
)

func main() {
	var (
		addr          = flag.String("addr", ":8080", "HTTP listen address")
		minioEndpoint = flag.String("minio-endpoint", getEnv("MINIO_ENDPOINT", "minio.default.svc.cluster.local:9000"), "MinIO Endpoint")
		minioAccess   = flag.String("minio-access", getEnv("MINIO_ACCESS_KEY", "minioadmin"), "MinIO Access Key")
		minioSecret   = flag.String("minio-secret", getEnv("MINIO_SECRET_KEY", "minioadmin"), "MinIO Secret Key")
		minioBucket   = flag.String("minio-bucket", getEnv("MINIO_BUCKET", "kubetrace-spans"), "MinIO Bucket Name")
		minioSSL      = flag.Bool("minio-ssl", getEnvBool("MINIO_USE_SSL", false), "Use SSL for MinIO")
		kubeconfig    = flag.String("kubeconfig", getEnv("KUBECONFIG", ""), "Path to kubeconfig (empty = in-cluster)")
		demoMode      = flag.Bool("demo", getEnvBool("KUBETRACE_DEMO", true), "Enable demo data generation")
		demoIntervalS = flag.Int("demo-interval", getEnvInt("KUBETRACE_DEMO_INTERVAL", 3), "Demo trace interval seconds")
		samplingLimit = flag.Int64("sampling-limit", int64(getEnvInt("KUBETRACE_SAMPLING_LIMIT", 1000)), "Target spans per second for adaptive sampling (0 to disable)")
	)
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Println("=== KubeTrace Server ===")
	log.Printf("addr=%s minio=%s bucket=%s demo=%v", *addr, *minioEndpoint, *minioBucket, *demoMode)

	traceStore, err := store.New(*minioEndpoint, *minioAccess, *minioSecret, *minioBucket, *minioSSL)
	if err != nil {
		log.Fatalf("init store: %v", err)
	}
	defer traceStore.Close()

	var watcher *k8s.Watcher
	watcher, err = k8s.NewWatcher(*kubeconfig)
	if err != nil {
		log.Printf("[warn] k8s watcher unavailable: %v (running without k8s metadata)", err)
		watcher = nil
	} else {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := watcher.Start(ctx); err != nil {
			log.Printf("[warn] k8s watcher start failed: %v", err)
		}
	}

	var sampler *collector.AdaptiveSampler
	if *samplingLimit > 0 {
		sampler = collector.NewAdaptiveSampler(*samplingLimit, 1*time.Second)
		defer sampler.Stop()
		log.Printf("adaptive sampler enabled: target=%d spans/sec", *samplingLimit)
	}

	handler := api.NewHandler(traceStore, watcher)
	receiver := collector.NewReceiver(traceStore, sampler, handler.OnSpan)

	if *demoMode {
		demo := collector.NewDemoGenerator(traceStore, handler.OnSpan)
		demo.Start(time.Duration(*demoIntervalS) * time.Second)
	}

	app := fiber.New(fiber.Config{
		AppName:               "KubeTrace",
		ReadTimeout:           30 * time.Second,
		WriteTimeout:          30 * time.Second,
		IdleTimeout:           120 * time.Second,
		DisableStartupMessage: false,
		ReduceMemoryUsage:     true,
	})

	api.SetupRouter(app, handler, receiver)

	go func() {
		if err := app.Listen(*addr); err != nil {
			log.Fatalf("server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down...")
	app.Shutdown()
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
		return n
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	switch os.Getenv(key) {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	}
	return def
}
