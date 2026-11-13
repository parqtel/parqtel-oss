#!/usr/bin/env bash
set -euo pipefail

# parqtel Kubernetes Environment Cleanup
# Uninstalls parqtel and optionally deletes the k3d cluster.

echo ">>> Uninstalling parqtel releases..."
helm uninstall parqtel -n parqtel --ignore-not-found
helm uninstall parqtel-test -n parqtel --ignore-not-found

echo ">>> Deleting namespace..."
kubectl delete namespace parqtel --ignore-not-found

echo "Cluster k3d-secure-cluster is still running."
echo "To delete it, run: k3d cluster delete secure-cluster"

read -p "Delete the k3d cluster? [y/N] " response
case "$response" in
    [yY][eE][sS]|[yY]) 
        echo ">>> Deleting k3d cluster secure-cluster..."
        k3d cluster delete secure-cluster
        ;;
    *)
        echo ">>> Cluster preservation confirmed."
        ;;
esac
