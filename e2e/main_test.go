package e2e

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/parqtel/parqtel/e2e/helpers"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	namespace   = flag.String("namespace", "parqtel-e2e", "Kubernetes namespace for e2e tests")
	releaseName = flag.String("release-name", "parqtel-e2e", "Helm release name")
	imageTag    = flag.String("image-tag", "dev", "Docker image tag for parqtel")
	valuesFile  = flag.String("values-file", "../deploy/k8s/overlays/ci/values.yaml", "Path to Helm values file")
	parqtelURL  = flag.String("parqtel-url", "http://localhost:8080", "Base URL for parqtel API (can be service IP or localhost with port-forward)")
)

func TestMain(m *testing.M) {
	flag.Parse()

	ctx := context.Background()
	client, err := helpers.NewK8sClient()
	if err != nil {
		fmt.Printf("Failed to create K8s client: %v\n", err)
		os.Exit(1)
	}

	// Setup: Create namespace and install Helm chart
	fmt.Printf(">>> Setting up e2e environment in namespace %s...\n", *namespace)
	if err := client.CreateNamespace(ctx, *namespace); err != nil {
		fmt.Printf("Failed to create namespace: %v\n", err)
		os.Exit(1)
	}

	helm := helpers.NewHelm(*namespace, *releaseName)
	if err := helm.Install(ctx, "../deploy/charts/parqtel", *valuesFile, *imageTag); err != nil {
		fmt.Printf("Failed to install Helm chart: %v\n", err)
		cleanup(client, helm)
		os.Exit(1)
	}

	// Wait for parqtel to be ready
	fmt.Println(">>> Waiting for parqtel deployment to be ready...")
	if err := helpers.WaitForDeployment(ctx, client.Clientset, *namespace, *releaseName, 2*time.Minute); err != nil {
		fmt.Printf("Deployment not ready: %v\n", err)
		cleanup(client, helm)
		os.Exit(1)
	}

	// Run tests
	exitCode := m.Run()

	// Teardown
	cleanup(client, helm)

	os.Exit(exitCode)
}

func cleanup(client *helpers.K8sClient, helm *helpers.Helm) {
	fmt.Println(">>> Tearing down e2e environment...")
	ctx := context.Background()
	_ = helm.Uninstall(ctx)
	_ = client.Clientset.CoreV1().Namespaces().Delete(ctx, *namespace, metav1.DeleteOptions{})
}
