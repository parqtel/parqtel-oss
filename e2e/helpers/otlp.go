package helpers

import (
	"encoding/json"
)

type OTLPBuilder struct{}

func NewOTLPBuilder() *OTLPBuilder {
	return &OTLPBuilder{}
}

func (b *OTLPBuilder) BuildGauge(name string, value float64, labels map[string]string, timestampNS int64) []byte {
    // Simplified JSON OTLP for E2E tests
    // Using snake_case as required by our robust decoder
	payload := map[string]interface{}{
		"resource_metrics": []interface{}{
			map[string]interface{}{
				"resource": map[string]interface{}{
					"attributes": []interface{}{
						map[string]interface{}{"key": "service.name", "value": map[string]interface{}{"string_value": "e2e-test"}},
					},
				},
				"scope_metrics": []interface{}{
					map[string]interface{}{
						"metrics": []interface{}{
							map[string]interface{}{
								"name": name,
								"gauge": map[string]interface{}{
									"data_points": []interface{}{
										map[string]interface{}{
											"time_unix_nano": timestampNS,
											"as_double":      value,
											"attributes":     b.mapToAttributes(labels),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	data, _ := json.Marshal(payload)
	return data
}

func (b *OTLPBuilder) BuildCounter(name string, value int64, labels map[string]string, timestampNS int64) []byte {
	payload := map[string]interface{}{
		"resource_metrics": []interface{}{
			map[string]interface{}{
				"resource": map[string]interface{}{
					"attributes": []interface{}{
						map[string]interface{}{"key": "service.name", "value": map[string]interface{}{"string_value": "e2e-test"}},
					},
				},
				"scope_metrics": []interface{}{
					map[string]interface{}{
						"metrics": []interface{}{
							map[string]interface{}{
								"name": name,
								"sum": map[string]interface{}{
									"aggregation_temporality": 1,
									"is_monotonic":            true,
									"data_points": []interface{}{
										map[string]interface{}{
											"time_unix_nano":       timestampNS,
											"start_time_unix_nano": timestampNS - 300_000_000_000,
											"as_int":               value,
											"attributes":           b.mapToAttributes(labels),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	data, _ := json.Marshal(payload)
	return data
}

// BuildHistogram creates an OTel cumulative histogram payload.
// bounds: bucket upper bounds, counts: per-bucket + overflow count (len = len(bounds)+1)
func (b *OTLPBuilder) BuildHistogram(name string, bounds []float64, counts []uint64, sum float64, timestampNS int64) []byte {
	total := uint64(0)
	for _, c := range counts {
		total += c
	}
	// Convert counts to int for JSON
	icounts := make([]interface{}, len(counts))
	for i, c := range counts {
		icounts[i] = c
	}
	payload := map[string]interface{}{
		"resource_metrics": []interface{}{
			map[string]interface{}{
				"resource": map[string]interface{}{
					"attributes": []interface{}{
						map[string]interface{}{"key": "service.name", "value": map[string]interface{}{"string_value": "e2e-test"}},
					},
				},
				"scope_metrics": []interface{}{
					map[string]interface{}{
						"metrics": []interface{}{
							map[string]interface{}{
								"name": name,
								"histogram": map[string]interface{}{
									"aggregation_temporality": 1,
									"data_points": []interface{}{
										map[string]interface{}{
											"time_unix_nano":       timestampNS,
											"start_time_unix_nano": timestampNS - 300_000_000_000,
											"count":                total,
											"sum":                  sum,
											"explicit_bounds":      bounds,
											"bucket_counts":        icounts,
											"attributes":           []interface{}{},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	data, _ := json.Marshal(payload)
	return data
}

func (b *OTLPBuilder) BuildLog(body string, labels map[string]string, timestampNS int64) []byte {
	payload := map[string]interface{}{
		"resource_logs": []interface{}{
			map[string]interface{}{
				"resource": map[string]interface{}{
					"attributes": []interface{}{
						map[string]interface{}{"key": "service.name", "value": map[string]interface{}{"string_value": "e2e-test"}},
					},
				},
				"scope_logs": []interface{}{
					map[string]interface{}{
						"log_records": []interface{}{
							map[string]interface{}{
								"time_unix_nano": timestampNS,
								"body":           map[string]interface{}{"string_value": body},
								"attributes":     b.mapToAttributes(labels),
							},
						},
					},
				},
			},
		},
	}
	data, _ := json.Marshal(payload)
	return data
}

func (b *OTLPBuilder) mapToAttributes(labels map[string]string) []interface{} {
	var attrs []interface{}
	for k, v := range labels {
		attrs = append(attrs, map[string]interface{}{
			"key": k,
			"value": map[string]interface{}{
				"string_value": v,
			},
		})
	}
	return attrs
}
