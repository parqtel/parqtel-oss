#!/usr/bin/env bash
set -euo pipefail

# parqtel Kubernetes Environment Bootstrap
# This script sets up a local k3d cluster and deploys parqtel via Helm.

echo ">>> Checking prerequisites..."
for tool in docker k3d helm kubectl; do
    if ! command -v "$tool" &> /dev/null; then
        echo "ERROR: $tool is not installed."
        echo "  macOS: brew install $tool"
        echo "  Linux: visit https://github.com/$tool/$tool/releases"
        exit 1
    fi
done

K8S_VERSION=$(kubectl version --client -o json | jq -r '.clientVersion.major + "." + .clientVersion.minor' | sed 's/[^0-9.]*//g')
if (( $(echo "$K8S_VERSION < 1.24" | bc -l) )); then
    echo "WARNING: kubectl version $K8S_VERSION is older than 1.24."
fi

HELM_VERSION=$(helm version --short | cut -d'v' -f2 | cut -d'.' -f1,2)
if (( $(echo "$HELM_VERSION < 3.10" | bc -l) )); then
    echo "WARNING: helm version $HELM_VERSION is older than 3.10."
fi

ARCH=$(uname -m)
PLATFORM="linux/amd64"
if [[ "$ARCH" == "arm64" || "$ARCH" == "aarch64" ]]; then
    PLATFORM="linux/arm64"
fi
echo ">>> Detected platform: $PLATFORM"

CLUSTER_NAME="secure-cluster"
CONTEXT="k3d-$CLUSTER_NAME"

if kubectl config get-contexts "$CONTEXT" &> /dev/null; then
    echo ">>> Reusing existing cluster: $CLUSTER_NAME"
    kubectl config use-context "$CONTEXT"
else
    echo ">>> Creating new k3d cluster: $CLUSTER_NAME"
    k3d cluster create --config deploy/k8s/k3d-config.yaml
fi

if ! kubectl cluster-info &> /dev/null; then
    echo "ERROR: Cluster $CLUSTER_NAME is unreachable."
    exit 1
fi

echo ">>> Ensuring registry is ready..."
until curl -s http://registry.localhost:5050/v2/ &> /dev/null; do
    echo "...waiting for registry.localhost:5050..."
    sleep 2
done

echo ">>> Building and pushing images..."
docker buildx build --platform "$PLATFORM" -t registry.localhost:5050/parqtel:dev --push .
docker buildx build --platform "$PLATFORM" -t registry.localhost:5050/load-generator:dev --push deploy/compose/load-generator/

echo ">>> Configuring /etc/hosts (manual check required)..."
HOSTS_ENTRIES=("registry.localhost" "parqtel.localhost" "grafana.localhost")
MISSING_HOSTS=()
for h in "${HOSTS_ENTRIES[@]}"; do
    if ! grep -q "$h" /etc/hosts; then
        MISSING_HOSTS+=("$h")
    fi
done

if [ ${#MISSING_HOSTS[@]} -gt 0 ]; then
    echo "WARNING: Missing entries in /etc/hosts for: ${MISSING_HOSTS[*]}"
    echo "Please run: sudo sh -c 'echo \"127.0.0.1 ${MISSING_HOSTS[*]}\" >> /etc/hosts'"
fi

echo ">>> Preparing namespace..."
kubectl create namespace parqtel --dry-run=client -o yaml | kubectl apply -f -

echo ">>> Deploying parqtel..."
helm upgrade --install parqtel deploy/charts/parqtel/ \
    --namespace parqtel \
    --values deploy/k8s/overlays/dev/values.yaml \
    --wait --timeout 120s

echo ">>> Deploying parqtel-test support services..."
helm upgrade --install parqtel-test deploy/k8s/charts/parqtel-test/ \
    --namespace parqtel \
    --wait --timeout 120s

echo ">>> Verifying readiness..."
RETRY=0
until [ $RETRY -ge 24 ]; do
    if curl -s -f http://parqtel.localhost/api/v1/label/__name__/values &> /dev/null; then
        echo ">>> parqtel is ready at http://parqtel.localhost"
        break
    fi
    echo "...polling parqtel.localhost ($RETRY/24)..."
    sleep 5
    RETRY=$((RETRY+1))
done

if [ $RETRY -eq 24 ]; then
    echo "ERROR: parqtel failed to become ready within 120s."
    kubectl -n parqtel logs -l app.kubernetes.io/name=parqtel --tail 50
    exit 1
fi

echo "========================================"
echo "Cluster: $CONTEXT"
echo "Status:  READY"
echo "URLs:"
echo "  - parqtel: http://parqtel.localhost"
echo "  - grafana: http://grafana.localhost"
echo "Commands:"
echo "  - Logs: kubectl -n parqtel logs -f -l app.kubernetes.io/name=parqtel"
echo "  - Shell: kubectl -n parqtel exec -it deploy/parqtel -- /bin/sh"
echo "========================================"
