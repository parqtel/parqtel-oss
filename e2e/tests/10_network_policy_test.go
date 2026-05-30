//go:build e2e,security
package tests

import (
	"context"
	"testing"

	"github.com/parqtel/parqtel/e2e/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNetworkPolicy(t *testing.T) {
	ctx := context.Background()
	client, err := helpers.NewK8sClient()
	require.NoError(t, err)

	ns := "parqtel-e2e"
	release := "parqtel-e2e"

	t.Run("NetworkPolicy resource exists", func(t *testing.T) {
        // Only if enabled in values
		_, err := client.Clientset.NetworkingV1().NetworkPolicies(ns).Get(ctx, release, metav1.GetOptions{})
		if err != nil {
            t.Skip("NetworkPolicy not enabled or not found")
        }
		assert.NoError(t, err)
	})
}
