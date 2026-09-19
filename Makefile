.PHONY: help dev-setup build release test lint bench docker docker-smoke \
        mcp-image mcp-up mcp-smoke \
        local-up local-down local-purge local-logs local-ps local-rebuild \
        local-verify test-api test-aggregations test-functions test-builder \
        test-builder-ui test-presets-static test-alert-presets tools \
        run inspect load load-test perf-audit clean \
        k8s-install k8s-validate k8s-sre-validate k8s-undeploy \
        local-k3d-up local-k3d-down local-k3d-status

# ─── Global config ─────────────────────────────────────────────────────────────
# Docker CLI (docker / docker-compose) — override with `make DOCKER=... <target>`
DOCKER       ?= docker
COMPOSE      ?= $(DOCKER) compose

# Ports (mirror .env; only used for printed hints and smoke checks)
PARQTEL_PORT ?= 9090
GRAFANA_PORT ?= 3000
PROM_PORT    ?= 9091

# ─── Restricted-host fallbacks (no-ops on normal hosts) ────────────────────────
# Sandboxed dev boxes, hardened CI runners, and read-only-$HOME environments
# often break `make docker/build/test` in confusing ways (buildx can't write
# its activity file, cargo can't write the registry cache, protoc missing).
# Each guard below probes the default location first and only falls back to a
# repo-local path when the default is unusable — stock hosts are untouched.

# buildx: state under ~/.docker/buildx — redirect to .buildx/ when unwritable
# (symptom: "failed to update builder last activity time: read-only file system").
ifeq ($(shell test -w "$(HOME)/.docker/buildx" 2>/dev/null && echo yes),yes)
  BUILDX_ROOT :=
else
  BUILDX_ROOT := $(CURDIR)/.buildx
  export BUILDX_CONFIG := $(BUILDX_ROOT)
endif

# cargo: registry cache under ~/.cargo — redirect to .cargo-home/ when unwritable.
# ("test -w" is false for missing dirs, so create-then-probe, and keep the user's
# explicit CARGO_HOME untouched.)
ifeq ($(CARGO_HOME),)
  ifeq ($(shell mkdir -p "$(HOME)/.cargo/registry" 2>/dev/null && test -w "$(HOME)/.cargo/registry" && echo yes),yes)
    CARGO_FALLBACK :=
  else
    CARGO_FALLBACK := $(CURDIR)/.cargo-home
    export CARGO_HOME := $(CARGO_FALLBACK)
  endif
endif

# protoc: prost-build requires it. Prefer system protoc, then .tools/bin/protoc
# (fetched via `make tools`). Export only when actually found.
PROTOC_PATH := $(shell command -v protoc 2>/dev/null)
ifeq ($(PROTOC_PATH),)
  PROTOC_PATH := $(wildcard $(CURDIR)/.tools/bin/protoc)
  ifneq ($(PROTOC_PATH),)
    export PROTOC := $(PROTOC_PATH)
  endif
endif

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
build: | tools ## Debug build (auto-fetches protoc into .tools/ if missing)
	cargo build

release: | tools ## Optimised release build (LTO, stripped)
	cargo build --release

test: | tools ## Run all workspace tests
	cargo test --workspace

lint: | tools ## rustfmt check + clippy
	cargo fmt --check
	cargo clippy --workspace -- -D warnings

bench: | tools ## Run benchmarks
	cargo bench

run: | tools ## Start the server locally from source (default config)
	cargo run --bin parqtel -- serve

inspect: | tools ## Inspect local storage index
	cargo run --bin parqtel -- inspect

# ─── Docker ────────────────────────────────────────────────────────────────────
docker: ## Build production Docker image (distroless) and report size
	$(DOCKER) buildx build --load -t parqtel:local .
	@echo "Image size:"
	@$(DOCKER) images parqtel:local --format "{{.Size}}"

docker-smoke: ## Run built image and verify HEALTHCHECK reaches 'healthy'
	@test -n "$$($(DOCKER) images -q parqtel:local)" || { echo "error: parqtel:local missing — run 'make docker' first"; exit 1; }
	@CID=$$($(DOCKER) run -d --name parqtel-smoke -p 127.0.0.1:8099:8080 parqtel:local serve); \
	trap '$(DOCKER) rm -f parqtel-smoke >/dev/null 2>&1' EXIT; \
	echo "Waiting for container to become healthy..."; \
	for i in $$(seq 1 40); do \
	  if [ "$$($(DOCKER) inspect -f '{{.State.Running}}' parqtel-smoke 2>/dev/null)" != "true" ]; then \
	    echo "container exited early:"; $(DOCKER) logs --tail 20 parqtel-smoke; exit 1; fi; \
	  st=$$($(DOCKER) inspect -f '{{.State.Health.Status}}' parqtel-smoke 2>/dev/null); \
	  if [ "$$st" = "healthy" ]; then echo "OK: healthy after ~$$((i*2))s"; exit 0; fi; \
	  if [ "$$st" = "unhealthy" ]; then echo "FAILED: marked unhealthy"; $(DOCKER) logs --tail 20 parqtel-smoke; exit 1; fi; \
	  sleep 2; \
	done; \
	echo "FAILED: timed out waiting for healthy"; $(DOCKER) logs --tail 20 parqtel-smoke; exit 1

# ─── MCP image (Parqtel self-MCP server) ───────────────────────────────────────
mcp-image: ## Build the standalone Parqtel MCP image (parqtel-mcp-parqtel:local)
	$(DOCKER) build -f compose/mcp/Dockerfile.parqtel -t parqtel-mcp-parqtel:local .
	@echo "Image size:"
	@$(DOCKER) images parqtel-mcp-parqtel:local --format "{{.Size}}"

mcp-up: ## Start Parqtel + MCP server (compose) and wait until healthy
	$(COMPOSE) up -d --build parqtel mcp-parqtel
	@echo "Waiting for mcp-parqtel to become healthy..."
	@for i in $$(seq 1 30); do \
	  st=$$($(DOCKER) inspect -f '{{.State.Health.Status}}' mcp-parqtel 2>/dev/null); \
	  if [ "$$st" = "healthy" ]; then echo "OK: mcp-parqtel healthy"; \
	    echo "  MCP endpoint: http://localhost:$${MCP_PARQTEL_PORT:-3007}/tools/list"; exit 0; fi; \
	  sleep 2; \
	done; \
	echo "FAILED: mcp-parqtel did not become healthy"; $(DOCKER) logs --tail 30 mcp-parqtel; exit 1

mcp-smoke: ## List the MCP server's tools (needs mcp-up)
	@curl -fsS "http://localhost:$${MCP_PARQTEL_PORT:-3007}/health" | head -c 200; echo; \
	curl -fsS "http://localhost:$${MCP_PARQTEL_PORT:-3007}/tools/list" \
	  | python3 -c 'import json,sys; t=json.load(sys.stdin)["tools"]; print(f"tools: {len(t)}"); [print(" -", x["name"]) for x in t]'


# ─── Local dev (Docker Compose) ────────────────────────────────────────────────
local-up: ## Start full local stack and wait until healthy (Parqtel + Grafana + Prometheus + load-generator)
	$(COMPOSE) up -d --wait
	@$(MAKE) --no-print-directory local-verify

local-down: ## Stop and remove containers
	$(COMPOSE) down

local-purge: ## Tear down the stack AND delete volumes + build cache (destructive)
	$(COMPOSE) down -v --remove-orphans
	rm -rf .buildx
	@echo "Stack, volumes, and buildx cache purged"

local-rebuild: ## Force rebuild images and restart (use after source changes)
	$(COMPOSE) up -d --build --wait
	@$(MAKE) --no-print-directory local-verify

local-logs: ## Tail logs from all services
	$(COMPOSE) logs -f

local-ps: ## Show status of all compose services
	$(COMPOSE) ps

local-verify: ## Verify every published endpoint + scrape pipeline end-to-end
	@echo "─── Stack verification ─────────────────────────────"
	@$(COMPOSE) ps
	@echo ""
	@fail=0; \
	for ep in "parqtel:$(PARQTEL_PORT)/health" \
	          "grafana:$(GRAFANA_PORT)/api/health" \
	          "prometheus:$(PROM_PORT)/-/healthy"; do \
	  name=$${ep%%:*}; rest=$${ep#*:}; port=$${rest%%/*}; path=/$${rest#*/}; \
	  code=$$(curl -s -o /dev/null -w '%{http_code}' -m 5 "http://localhost:$$port$$path" || echo 000); \
	  if [ "$$code" = "200" ]; then \
	    echo "OK  $$name  http://localhost:$$port$$path -> 200"; \
	  else \
	    echo "FAIL $$name  http://localhost:$$port$$path -> $$code"; fail=1; \
	  fi; \
	done; \
	curl -s -m 5 "http://localhost:$(PARQTEL_PORT)/api/v1/label/__name__/values" \
	  | jq -e '.data | length > 0' >/dev/null 2>&1 \
	  || { echo "FAIL no metric names visible — is the load-generator running?"; fail=1; }; \
	if [ "$$fail" = "0" ]; then \
	  echo ""; echo "Stack fully verified — all endpoints healthy, data flowing"; \
	else \
	  echo ""; echo "Verification failed — check 'make local-logs'"; exit 1; \
	fi

test-api: ## Smoke-test the local API (requires local-up)
	@echo "Querying label values..."
	@curl -s -f http://localhost:$$(grep PARQTEL_PORT .env 2>/dev/null | cut -d= -f2 || echo 9090)/api/v1/label/__name__/values \
		| jq . || (echo "API test failed — is 'make local-up' running?" && exit 1)
	@echo "✅ API is healthy"

test-aggregations: ## PromQL aggregation conformance suite (count/sum/avg grouping, rate, topk; requires local-up)
	@./scripts/test_aggregations.sh http://localhost:$(PARQTEL_PORT)

test-functions: ## Full PromQL function conformance suite (every function family; requires local-up)
	@./scripts/test_functions.sh http://localhost:$(PARQTEL_PORT)

test-builder: ## E2E validation of every UI query-builder PromQL shape (requires local-up)
	@./scripts/test_builder.sh http://localhost:$(PARQTEL_PORT)

test-builder-ui: ## Headless-browser E2E of the builder UI incl. high-card autocomplete (requires local-up, node, google-chrome)
	@./scripts/test_builder_ui.sh http://localhost:$(PARQTEL_PORT)

test-presets-static: ## Validate every preset alert rule parses + its query is executable (CI-safe, no server)
	@cargo test -p parqtel-alert --test preset_rules

test-alert-presets: ## End-to-end: preset rules fire on threshold crossings and recover (builds binary if needed; curl, jq, python3)
	@./scripts/test_alert_presets.sh

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

# ─── Toolchain helpers ──────────────────────────────────────────────────────────
# Host toolchains are sometimes minimal (containers, CI, dev sandboxes) and the
# Rust build needs protoc for prost-build. Fetch a pinned protoc into .tools/
# (git-ignored, workspace-local, never installed system-wide) when missing.
PROTOC_VERSION ?= 29.3
PROTOC_DIR     := .tools
PROTOC_BIN     := $(PROTOC_DIR)/bin/protoc

tools: ## Fetch pinned protoc into .tools/ if no system protoc exists
	@if command -v protoc >/dev/null 2>&1; then \
		echo "system protoc found: $$(command -v protoc) ($$(protoc --version))"; \
	elif [ -x "$(PROTOC_BIN)" ]; then \
		echo "protoc already fetched: $(PROTOC_BIN) ($$($(PROTOC_BIN) --version))"; \
	else \
		echo "fetching protoc $(PROTOC_VERSION) into $(PROTOC_DIR)/ ..."; \
		mkdir -p $(PROTOC_DIR); \
		curl -sSfL -o $(PROTOC_DIR)/protoc.zip \
			"https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-linux-x86_64.zip" \
		|| { echo "error: protoc download failed"; exit 1; }; \
		python3 -c "import zipfile; zipfile.ZipFile('$(PROTOC_DIR)/protoc.zip').extractall('$(PROTOC_DIR)')" \
		|| unzip -qo $(PROTOC_DIR)/protoc.zip -d $(PROTOC_DIR); \
		rm -f $(PROTOC_DIR)/protoc.zip; \
		chmod +x $(PROTOC_BIN); \
		echo "protoc ready: $(PROTOC_BIN) ($$($(PROTOC_BIN) --version))"; \
	fi

# ─── Cleanup ───────────────────────────────────────────────────────────────────
clean: ## Remove build artefacts, local data, and repo-local tool/cache fallbacks
	cargo clean
	rm -rf data .buildx .cargo-home .tools
