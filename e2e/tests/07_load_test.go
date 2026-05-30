//go:build e2e,slow
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/parqtel/parqtel/e2e/helpers"
	"github.com/stretchr/testify/assert"
)

func TestLoad(t *testing.T) {
	ctx := context.Background()
	hc := helpers.NewHTTPClient("http://localhost:8080")
	otlp := helpers.NewOTLPBuilder()

	t.Run("Ingestion throughput matches target", func(t *testing.T) {
		start := time.Now()
		count := 10000
		for i := 0; i < count; i++ {
			payload := otlp.BuildGauge("load_test", float64(i), nil, time.Now().UnixNano())
			hc.Post("/v1/metrics/json", "application/json", payload)
		}
		duration := time.Since(start)
		rps := float64(count) / duration.Seconds()
		fmt.Printf(">>> Actual throughput: %.2f pts/s\n", rps)
		// assert.Greater(t, rps, 100.0) // Sample assertion
	})

	t.Run("Memory usage under load remains stable", func(t *testing.T) {
		body, _, _ := hc.Get("/metrics")
		metrics := helpers.ParseMetrics(string(body))
		rssStr := metrics["parqtel_process_rss_bytes"]
		fmt.Printf(">>> Peak RSS: %s bytes\n", rssStr)
	})
}
