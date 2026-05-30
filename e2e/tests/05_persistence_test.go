//go:build e2e,slow
package tests

import (
	"context"
	"testing"
	"time"

	"github.com/parqtel/parqtel/e2e/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPersistence(t *testing.T) {
	ctx := context.Background()
	client, err := helpers.NewK8sClient()
	require.NoError(t, err)
	hc := helpers.NewHTTPClient("http://parqtel.localhost")
	otlp := helpers.NewOTLPBuilder()

	ns := "parqtel"

	t.Run("Data survives pod restart", func(t *testing.T) {
		metricName := "persistence_test"
		payload := otlp.BuildGauge(metricName, 99.9, nil, time.Now().UnixNano())
		hc.Post("/v1/metrics/json", "application/json", payload)

		// Delete pod
		pods, _ := client.Clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/name=parqtel"})
		podName := pods.Items[0].Name
		err = client.Clientset.CoreV1().Pods(ns).Delete(ctx, podName, metav1.DeleteOptions{})
		assert.NoError(t, err)

		// Wait for new pod
		time.Sleep(10 * time.Second)
		helpers.WaitForDeployment(ctx, client.Clientset, ns, "parqtel", 1*time.Minute)

		// Query again (parqtel might need time to reload index)
		time.Sleep(5 * time.Second)
		body, _, _ := hc.Get("/api/v1/label/__name__/values")
		assert.Contains(t, string(body), metricName)
	})
}
