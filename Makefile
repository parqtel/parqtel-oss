.PHONY: help build release test lint bench docker run inspect load clean

# Default target
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Build the project in debug mode
	cargo build

release: ## Build the project in release mode (optimised)
	cargo build --release

test: ## Run the full test suite
	cargo test --workspace

lint: ## Run rustfmt and clippy
	cargo fmt --check
	cargo clippy --workspace -- -D warnings

bench: ## Run benchmarks (requires nightly for some criterion features, or just standard cargo bench)
	cargo bench

docker: ## Build a local Docker image and report its size
	docker build -t parqtel:local .
	@echo "Image size:"
	@docker images parqtel:local --format "{{.Size}}"

local-up: ## Spin up local environment with docker-compose
	docker-compose -f deploy/compose/docker-compose.yml up -d

local-down: ## Tear down local environment
	docker-compose -f deploy/compose/docker-compose.yml down

local-logs: ## Tail local environment logs
	docker-compose -f deploy/compose/docker-compose.yml logs -f

k8s-install: ## Install parqtel on the current K8s cluster using Helm
	bash deploy/k8s/install.sh

k8s-validate: ## Run E2E tests against the current K8s cluster (requires Go)
	bash scripts/validate.sh
	bash scripts/validate-hpa.sh

k8s-sre-validate: ## Run shell-based SRE validation against the current K8s cluster
	bash scripts/sre-validate.sh

k8s-undeploy: ## Remove parqtel from the current K8s cluster
	helm uninstall parqtel -n parqtel --ignore-not-found

local-k3d-up: ## Setup local k3d cluster and deploy parqtel
	bash deploy/k8s/setup.sh

local-k3d-down: ## Tear down local k3d cluster
	bash deploy/k8s/teardown.sh

local-k3d-status: ## Show k3d cluster status
	$(MAKE) -C deploy/k8s cluster-status

run: ## Start the server locally with default configuration
	cargo run --bin parqtel -- serve

inspect: ## Inspect the local storage index
	cargo run --bin parqtel -- inspect

test-api: ## Run a simple API test against the local environment (port 9090)
	@echo "Querying label values..."
	@curl -s -f http://localhost:9090/api/v1/label/__name__/values | jq . || (echo "API test failed (make sure local-up is running)" && exit 1)

load: ## Send 10,000 synthetic data points to the local environment (port 9090)
	python3 scripts/load-test.py http://localhost:9090 10000

# Default Load Parameters
LOAD_RATE ?= 1000
LOAD_TIME ?= 1
TARGET_URL ?= http://localhost:8080
LOAD_TYPE ?= all
LOAD_SCRIPT ?= scripts/load_gen.py

.PHONY: load-test
load-test:
	@echo "========================================================================"
	@echo "🚀 Initiating Parqtel Unified Load Test Engine"
	@echo "🎯 Target Endpoint: $(TARGET_URL)"
	@echo "📊 Target Load Rate: $(LOAD_RATE) samples/minute"
	@echo "⏱️ Total Run Duration: $(LOAD_TIME) minute(s)"
	@echo "📈 Telemetry Type: $(LOAD_TYPE)"
	@echo "========================================================================"
	@python3 -m venv .venv && \
	. .venv/bin/activate && \
	pip install --upgrade pip && \
	pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-http && \
	python3 $(LOAD_SCRIPT) --endpoint $(TARGET_URL) --rate $(LOAD_RATE) --duration $(LOAD_TIME) --type $(LOAD_TYPE)

.PHONY: perf-audit
perf-audit: release ## Run performance audit (builds release binary and runs load test)
	@echo "========================================================================"
	@echo "📊 Running Parqtel Performance Audit"
	@echo "========================================================================"
	@bash scripts/run_perf_audit.sh

clean: ## Clean build artifacts and local data directory
	cargo clean
	rm -rf data
