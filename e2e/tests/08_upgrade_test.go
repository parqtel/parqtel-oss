//go:build e2e
package tests

import (
	"context"
	"testing"

	"github.com/parqtel/parqtel/e2e/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpgrade(t *testing.T) {
	ctx := context.Background()
	client, err := helpers.NewK8sClient()
	require.NoError(t, err)

	ns := "parqtel-e2e"
	release := "parqtel-e2e"
	helm := helpers.NewHelm(ns, release)

	t.Run("Helm upgrade preserves PVC", func(t *testing.T) {
		// Run upgrade with slightly different values
		err := helm.Install(ctx, "../chart/parqtel", "../k8s/overlays/dev/values.yaml", "dev")
		assert.NoError(t, err)

		pvc, err := client.Clientset.CoreV1().PersistentVolumeClaims(ns).Get(ctx, release, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, "Bound", string(pvc.Status.Phase))
	})
}

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
