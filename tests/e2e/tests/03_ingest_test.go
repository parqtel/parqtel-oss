//go:build e2e
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/parqtel/parqtel/e2e/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestion(t *testing.T) {
	ctx := context.Background()
	hc := helpers.NewHTTPClient("http://parqtel.localhost") // Reachable via ingress
	otlp := helpers.NewOTLPBuilder()

	t.Run("Basic OTLP JSON ingestion", func(t *testing.T) {
		metricName := "e2e_gauge_test"
		ts := time.Now().UnixNano()
		payload := otlp.BuildGauge(metricName, 123.45, map[string]string{"env": "e2e"}, ts)

		_, code, err := hc.Post("/v1/metrics/json", "application/json", payload)
		assert.NoError(t, err)
		assert.Equal(t, 200, code)

		// Verify metric appears in metadata
		body, code, err := hc.Get("/api/v1/label/__name__/values")
		assert.NoError(t, err)
		assert.Equal(t, 200, code)
		assert.Contains(t, string(body), metricName)
	})

	t.Run("Basic OTLP JSON log ingestion", func(t *testing.T) {
		logBody := "e2e log message"
		ts := time.Now().UnixNano()
		payload := otlp.BuildLog(logBody, map[string]string{"level": "info"}, ts)

		_, code, err := hc.Post("/v1/logs/json", "application/json", payload)
		assert.NoError(t, err)
		assert.Equal(t, 200, code)
	})

	t.Run("Ingestion stats counter increase", func(t *testing.T) {
		body, _, _ := hc.Get("/metrics")
		metrics := helpers.ParseMetrics(string(body))
		initial := metrics["parqtel_ingested_points_total"]

		payload := otlp.BuildGauge("stat_test", 1.0, nil, time.Now().UnixNano())
		hc.Post("/v1/metrics/json", "application/json", payload)

		body, _, _ = hc.Get("/metrics")
		metrics = helpers.ParseMetrics(string(body))
		current := metrics["parqtel_ingested_points_total"]
		
		assert.NotEqual(t, initial, current)
	})
}
