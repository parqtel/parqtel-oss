.PHONY: help cluster-up cluster-create cluster-down cluster-status images-build deploy undeploy redeploy load-test logs shell context

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

cluster-up: ## Start k3d cluster and deploy parqtel (reuses existing)
	chmod +x setup.sh && ./setup.sh

cluster-create: ## Create a new k3d cluster only
	@if k3d cluster get secure-cluster &>/dev/null; then echo "Error: secure-cluster already exists"; exit 1; fi
	k3d cluster create --config k3d-config.yaml

cluster-down: ## Tear down Kubernetes resources and optionally the cluster
	chmod +x teardown.sh && ./teardown.sh

cluster-status: ## Show cluster info and pod status
	kubectl cluster-info
	kubectl -n parqtel get pods
	@echo "parqtel: http://parqtel.localhost"
	@echo "grafana: http://grafana.localhost"

images-build: ## Build and push images to local k3d registry
	@ARCH=$$(uname -m); \
	PLATFORM="linux/amd64"; \
	if [ "$$ARCH" = "arm64" ] || [ "$$ARCH" = "aarch64" ]; then PLATFORM="linux/arm64"; fi; \
	docker buildx build --platform $$PLATFORM -t registry.localhost:5050/parqtel:dev --push ../..; \
	docker buildx build --platform $$PLATFORM -t registry.localhost:5050/load-generator:dev --push ../compose/load-generator/

deploy: ## Deploy parqtel using dev overlay
	helm upgrade --install parqtel ../charts/parqtel/ --namespace parqtel --values overlays/dev/values.yaml --wait

undeploy: ## Remove parqtel and parqtel-test from namespace
	helm uninstall parqtel -n parqtel --ignore-not-found
	helm uninstall parqtel-test -n parqtel --ignore-not-found

redeploy: images-build deploy ## Rebuild images and redeploy

load-test: ## Run timed load test
	chmod +x load-test.sh && ./load-test.sh

logs: ## Tail parqtel pod logs
	kubectl -n parqtel logs -f -l app.kubernetes.io/name=parqtel --tail 100

shell: ## Open a shell in the parqtel pod
	kubectl -n parqtel exec -it deploy/parqtel -- /bin/sh

context: ## Switch to k3d-secure-cluster context
	kubectl config use-context k3d-secure-cluster
