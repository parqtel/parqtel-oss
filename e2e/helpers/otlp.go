package helpers

import (
	"encoding/json"
	"fmt"
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
