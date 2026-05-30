# parqtel Load Generator

This service continuously generates synthetic OpenTelemetry metric data and sends it to parqtel via OTLP HTTP.

## Metric Kinds Produced

- **Counters**: HTTP requests, errors, and throughput.
- **Gauges**: System health metrics (CPU, Memory) with realistic noise and trends.
- **Histograms**: Latency distributions for various service types.
- **Summaries**: Quantile-based observations.

## Modes

### Normal Mode (Default)
Targets 1,000 distinct series and approximately 167 samples per second.
Good for general exploration and dashboard testing.

### Load Test Mode
Activated by setting `LOAD_TEST_MODE=true`.
Targets 10,000 distinct series and up to 17,000 samples per second (1 million/min).
Includes a 60-second ramp-up period to prevent cold-start bottlenecks.

## Configuration

Controlled via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `PARQTEL_URL` | Base URL of the parqtel server | `http://parqtel:9090` |
| `GENERATOR_NORMAL_SERIES` | Series count in normal mode | `1000` |
| `GENERATOR_NORMAL_RPS` | Samples/sec in normal mode | `167` |
| `GENERATOR_LOAD_SERIES` | Series count in load test mode | `10000` |
| `GENERATOR_LOAD_RPS` | Samples/sec in load test mode | `17000` |
| `LOAD_TEST_MODE` | Enable high-volume mode | `false` |

## Output

In load test mode, performance data is written to `/results/load_test_results.csv` every 10 seconds.
