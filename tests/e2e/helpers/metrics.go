package helpers

import (
	"strings"
)

func ParseMetrics(data string) map[string]string {
	metrics := make(map[string]string)
	lines := strings.Split(data, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, " ")
		if len(parts) >= 2 {
			metrics[parts[0]] = parts[1]
		}
	}
	return metrics
}
