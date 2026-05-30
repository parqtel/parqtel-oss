//go:build e2e
package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/parqtel/parqtel/e2e/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestInstallation(t *testing.T) {
	ctx := context.Background()
	client, err := helpers.NewK8sClient()
	require.NoError(t, err)

	ns := "parqtel"
	release := "parqtel"

	t.Run("Deployment exists and is ready", func(t *testing.T) {
		deploy, err := client.Clientset.AppsV1().Deployments(ns).Get(ctx, release, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, int32(1), deploy.Status.ReadyReplicas)
	})

	t.Run("Service exists and is LoadBalancer", func(t *testing.T) {
		svc, err := client.Clientset.CoreV1().Services(ns).Get(ctx, release, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, "LoadBalancer", string(svc.Spec.Type))
	})

	t.Run("ConfigMap exists with toml", func(t *testing.T) {
		cm, err := client.Clientset.CoreV1().ConfigMaps(ns).Get(ctx, release, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Contains(t, cm.Data, "parqtel.toml")
	})

	t.Run("PVC exists and is bound", func(t *testing.T) {
		pvc, err := client.Clientset.CoreV1().PersistentVolumeClaims(ns).Get(ctx, release, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, "Bound", string(pvc.Status.Phase))
	})
}
