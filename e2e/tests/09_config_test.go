//go:build e2e
package tests

import (
	"context"
	"testing"

	"github.com/parqtel/parqtel/e2e/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestConfiguration(t *testing.T) {
	ctx := context.Background()
	client, err := helpers.NewK8sClient()
	require.NoError(t, err)

	ns := "parqtel-e2e"
	release := "parqtel-e2e"

	t.Run("ConfigMap matches Helm values", func(t *testing.T) {
		cm, err := client.Clientset.CoreV1().ConfigMaps(ns).Get(ctx, release, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Contains(t, cm.Data["parqtel.toml"], "compression = \"zstd\"")
	})
}
