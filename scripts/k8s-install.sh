#!/usr/bin/env bash
set -euo pipefail

# parqtel Helm Deployment Script
# Deploys parqtel to the CURRENT kubernetes context.

NAMESPACE=${NAMESPACE:-parqtel}
RELEASE_NAME=${RELEASE_NAME:-parqtel}
VALUES_FILE=${VALUES_FILE:-deploy/k8s/overlays/dev/values.yaml}

echo ">>> Deploying to namespace: $NAMESPACE"
echo ">>> Current Context: $(kubectl config current-context)"

# Create namespace if it doesn't exist
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

echo ">>> Installing/Upgrading parqtel..."
helm upgrade --install "$RELEASE_NAME" charts/parqtel/ \
    --namespace "$NAMESPACE" \
    --values "$VALUES_FILE" \
    --wait --timeout 300s

echo ">>> Deployment complete."
kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=parqtel
