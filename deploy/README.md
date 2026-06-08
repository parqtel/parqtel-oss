# parqtel Deployment & Operations

This directory contains everything needed to deploy and operate parqtel in Kubernetes and Docker environments.

## 🚀 Quick Start (Existing Cluster)

If you already have a Kubernetes cluster configured and your `kubectl` context set:

1.  **Deploy parqtel**:
    ```bash
    make k8s-install
    ```
    *This uses the default dev values. To use custom values:*
    `VALUES_FILE=my-values.yaml make k8s-install`

2.  **Validate Deployment**:
    ```bash
    make k8s-validate
    ```
    *This runs the full E2E test suite against the current context.*

3.  **Clean up**:
    ```bash
    make k8s-undeploy
    ```

## 🛠️ Local Development (Docker Compose)

For fast iteration without a Kubernetes cluster:

```bash
make local-up     # Spin up Parqtel, Prometheus, Grafana
make test-api     # Run a quick smoke test
make local-down   # Tear down the environment
```

## 🧪 Advanced: Local K8s (k3d)

If you need a local Kubernetes environment with a real API Aggregator and Ingress:

```bash
make local-k3d-up    # Setup k3d cluster and deploy
make local-k3d-down  # Destroy cluster
```

## 📁 Directory Structure

-   `k8s/`: Environment-specific overlays and installation scripts.
-   `systemd/`: Service units for bare-metal deployments.

> The Helm chart lives at `charts/parqtel/` (project root).
> The Docker Compose setup lives at `compose/` (project root) with `docker-compose.yml` at root.
