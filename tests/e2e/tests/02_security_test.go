//go:build e2e && security
package tests

import (
	"context"
	"testing"

	"github.com/parqtel/parqtel/e2e/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSecurityPosture(t *testing.T) {
	ctx := context.Background()
	client, err := helpers.NewK8sClient()
	require.NoError(t, err)

	ns := "parqtel"
	pods, err := client.Clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=parqtel",
	})
	require.NoError(t, err)
	require.NotEmpty(t, pods.Items)
	pod := pods.Items[0]

	t.Run("Runs as non-root user 65534", func(t *testing.T) {
		sc := pod.Spec.Containers[0].SecurityContext
		assert.NotNil(t, sc)
		assert.True(t, *sc.RunAsNonRoot)
		assert.Equal(t, int64(65534), *sc.RunAsUser)
	})

	t.Run("Read-only root filesystem", func(t *testing.T) {
		sc := pod.Spec.Containers[0].SecurityContext
		assert.True(t, *sc.ReadOnlyRootFilesystem)
	})

	t.Run("Privilege escalation disabled", func(t *testing.T) {
		sc := pod.Spec.Containers[0].SecurityContext
		assert.False(t, *sc.AllowPrivilegeEscalation)
	})

	t.Run("Capabilities dropped", func(t *testing.T) {
		sc := pod.Spec.Containers[0].SecurityContext
		require.NotNil(t, sc.Capabilities)
		assert.Contains(t, sc.Capabilities.Drop, corev1.Capability("ALL"))
	})
}

// Importing corev1 for Capability type
import corev1 "k8s.io/api/core/v1"
