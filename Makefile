.PHONY: help dev-setup build release test lint bench docker docker-smoke \
        local-up local-down local-logs local-ps local-rebuild test-api \
        run inspect load load-test perf-audit clean \
        k8s-install k8s-validate k8s-sre-validate k8s-undeploy \
        local-k3d-up local-k3d-down local-k3d-status

# ─── Help ──────────────────────────────────────────────────────────────────────
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─── Onboarding ────────────────────────────────────────────────────────────────
dev-setup: ## First-time setup: copy .env.example → .env, then start the full stack
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "✅ Created .env from .env.example — edit it to add MCP API keys (optional)"; \
	else \
		echo "ℹ️  .env already exists, skipping copy"; \
	fi
	@if [ ! -f docker-compose.override.yml ] && [ -f docker-compose.override.yml.example ]; then \
		echo "ℹ️  To customise ports/mounts copy: cp docker-compose.override.yml.example docker-compose.override.yml"; \
	fi
	$(MAKE) local-up
	@echo ""
	@echo "🚀 Stack is up! Services:"
	@echo "   Parqtel    → http://localhost:$$(grep PARQTEL_PORT .env | cut -d= -f2 || echo 9090)"
	@echo "   Grafana    → http://localhost:$$(grep GRAFANA_PORT .env | cut -d= -f2 || echo 3000)  (admin / parqtel-dev)"
	@echo "   Prometheus → http://localhost:$$(grep PROMETHEUS_PORT .env | cut -d= -f2 || echo 9091)"
	@echo ""
	@echo "Run 'make test-api' to verify, 'make local-logs' to tail logs."

# ─── Rust build & test ─────────────────────────────────────────────────────────
build: ## Debug build
	cargo build

release: ## Optimised release build (LTO, stripped)
	cargo build --release

test: ## Run all workspace tests
	cargo test --workspace

lint: ## rustfmt check + clippy
	cargo fmt --check
	cargo clippy --workspace -- -D warnings

bench: ## Run benchmarks
	cargo bench

# ─── Docker ────────────────────────────────────────────────────────────────────
docker: ## Build production Docker image (distroless) and report size
	docker buildx build --load -t parqtel:local .
	@echo "Image size:"
	@docker images parqtel:local --format "{{.Size}}"

docker-smoke: ## Run built image and verify HEALTHCHECK reaches 'healthy'
	@test -n "$$(docker images -q parqtel:local)" || { echo "error: parqtel:local missing — run 'make docker' first"; exit 1; }
	@CID=$$(docker run -d --name parqtel-smoke -p 127.0.0.1:8099:8080 parqtel:local serve); \
	trap 'docker rm -f parqtel-smoke >/dev/null 2>&1' EXIT; \
	echo "Waiting for container to become healthy..."; \
	for i in $$(seq 1 40); do \
	  if [ "$$(docker inspect -f '{{.State.Running}}' parqtel-smoke 2>/dev/null)" != "true" ]; then \
	    echo "container exited early:"; docker logs --tail 20 parqtel-smoke; exit 1; fi; \
	  st=$$(docker inspect -f '{{.State.Health.Status}}' parqtel-smoke 2>/dev/null); \
	  if [ "$$st" = "healthy" ]; then echo "OK: healthy after ~$$((i*2))s"; exit 0; fi; \
	  if [ "$$st" = "unhealthy" ]; then echo "FAILED: marked unhealthy"; docker logs --tail 20 parqtel-smoke; exit 1; fi; \
	  sleep 2; \
	done; \
	echo "FAILED: timed out waiting for healthy"; docker logs --tail 20 parqtel-smoke; exit 1

# ─── Local dev (Docker Compose) ────────────────────────────────────────────────
local-up: ## Start full local stack (Parqtel + Grafana + Prometheus + load-generator)
	docker compose up -d

local-down: ## Stop and remove containers
	docker compose down

local-rebuild: ## Force rebuild images and restart (use after source changes)
	docker compose up -d --build

local-logs: ## Tail logs from all services
	docker compose logs -f

local-ps: ## Show status of all compose services
	docker compose ps

test-api: ## Smoke-test the local API (requires local-up)
	@echo "Querying label values..."
	@curl -s -f http://localhost:$$(grep PARQTEL_PORT .env 2>/dev/null | cut -d= -f2 || echo 9090)/api/v1/label/__name__/values \
		| jq . || (echo "❌ API test failed — is 'make local-up' running?" && exit 1)
	@echo "✅ API is healthy"

# ─── Local run (from source) ───────────────────────────────────────────────────
run: ## Start the server locally from source (default config)
	cargo run --bin parqtel -- serve

inspect: ## Inspect local storage index
	cargo run --bin parqtel -- inspect

# ─── Load testing ──────────────────────────────────────────────────────────────
load: ## Send 10,000 synthetic data points to localhost:9090
	python3 scripts/load-test.py http://localhost:9090 10000

LOAD_RATE   ?= 1000
LOAD_TIME   ?= 1
TARGET_URL  ?= http://localhost:9090
LOAD_TYPE   ?= all
LOAD_SCRIPT ?= scripts/load_gen.py

load-test: ## Full load test (LOAD_RATE, LOAD_TIME, TARGET_URL, LOAD_TYPE overrideable)
	@echo "========================================================"
	@echo "🚀 Parqtel Load Test — $(TARGET_URL)"
	@echo "   Rate: $(LOAD_RATE) samples/min  Duration: $(LOAD_TIME) min  Type: $(LOAD_TYPE)"
	@echo "========================================================"
	@python3 -m venv .venv && \
	. .venv/bin/activate && \
	pip install --quiet --upgrade pip && \
	pip install --quiet opentelemetry-sdk opentelemetry-exporter-otlp-proto-http && \
	python3 $(LOAD_SCRIPT) --endpoint $(TARGET_URL) --rate $(LOAD_RATE) --duration $(LOAD_TIME) --type $(LOAD_TYPE)

perf-audit: release ## Release build + full performance audit
	@echo "========================================================"
	@echo "📊 Parqtel Performance Audit"
	@echo "========================================================"
	bash scripts/run_perf_audit.sh

# ─── Kubernetes ────────────────────────────────────────────────────────────────
k8s-install: ## Install Parqtel on the current K8s cluster via Helm
	bash scripts/k8s-install.sh

k8s-validate: ## Run E2E tests against the current K8s cluster (requires Go)
	bash scripts/validate.sh
	bash scripts/validate-hpa.sh

k8s-sre-validate: ## Shell-based SRE validation against the current K8s cluster
	bash scripts/sre-validate.sh

k8s-undeploy: ## Uninstall Parqtel from the current K8s cluster
	helm uninstall parqtel -n parqtel --ignore-not-found

local-k3d-up: ## Provision a local k3d cluster and deploy Parqtel
	bash scripts/k8s-setup.sh

local-k3d-down: ## Destroy the local k3d cluster
	bash scripts/k8s-teardown.sh

local-k3d-status: ## Show k3d cluster status
	kubectl get pods -n parqtel

# ─── E2E / Functional tests ────────────────────────────────────────────────────
PARQTEL_E2E_URL ?= http://localhost:9090

e2e-promql: ## Run PromQL functional validation tests against the compose stack (requires local-up)
	@echo "Running PromQL functional tests against $(PARQTEL_E2E_URL) ..."
	cd e2e && PARQTEL_URL=$(PARQTEL_E2E_URL) go test -v -count=1 -tags promql ./tests/ \
		-run TestPromQLFunctions \
		-timeout 120s

# ─── Cleanup ───────────────────────────────────────────────────────────────────
clean: ## Remove build artefacts and local data directory
	cargo clean
	rm -rf data
