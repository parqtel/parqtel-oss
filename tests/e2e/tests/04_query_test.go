//go:build e2e
package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/parqtel/parqtel/e2e/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuerying(t *testing.T) {
	ctx := context.Background()
	hc := helpers.NewHTTPClient("http://parqtel.localhost")
	otlp := helpers.NewOTLPBuilder()

	// Pre-condition: Ingest data
	metricName := "e2e_query_test"
	now := time.Now().Unix()
	for i := 0; i < 5; i++ {
		payload := otlp.BuildGauge(metricName, float64(i*10), map[string]string{"id": fmt.Sprintf("%d", i)}, (now-int64(i*60))*1_000_000_000)
		hc.Post("/v1/metrics/json", "application/json", payload)
	}

	t.Run("Range query returns correct series count", func(t *testing.T) {
		query := fmt.Sprintf("%s", metricName)
		url := fmt.Sprintf("/api/v1/query_range?query=%s&start=%d&end=%d&step=15s", query, now-600, now)
		
		body, code, err := hc.Get(url)
		assert.NoError(t, err)
		assert.Equal(t, 200, code)

		var res map[string]interface{}
		json.Unmarshal(body, &res)
		data := res["data"].(map[string]interface{})
		result := data["result"].([]interface{})
		
		assert.Equal(t, 5, len(result))
	})

	t.Run("Label filtering works", func(t *testing.T) {
		query := fmt.Sprintf("%s{id=\"2\"}", metricName)
		url := fmt.Sprintf("/api/v1/query_range?query=%s&start=%d&end=%d&step=15s", query, now-600, now)
		
		body, _, _ := hc.Get(url)
		var res map[string]interface{}
		json.Unmarshal(body, &res)
		result := res["data"].(map[string]interface{})["result"].([]interface{})
		
		assert.Equal(t, 1, len(result))
	})
}
