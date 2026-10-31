package helpers

import (
	"context"
	"fmt"
	"os/exec"
)

type Helm struct {
	Namespace   string
	ReleaseName string
}

func NewHelm(namespace, releaseName string) *Helm {
	return &Helm{
		Namespace:   namespace,
		ReleaseName: releaseName,
	}
}

func (h *Helm) Install(ctx context.Context, chartPath, valuesFile, imageTag string) error {
	args := []string{
		"upgrade", "--install", h.ReleaseName, chartPath,
		"--namespace", h.Namespace,
		"--values", valuesFile,
		"--set", fmt.Sprintf("image.tag=%s", imageTag),
		"--wait",
	}

	cmd := exec.CommandContext(ctx, "helm", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("helm install failed: %w, output: %s", err, string(output))
	}
	return nil
}

func (h *Helm) Uninstall(ctx context.Context) error {
	args := []string{
		"uninstall", h.ReleaseName,
		"--namespace", h.Namespace,
		"--ignore-not-found",
	}

	cmd := exec.CommandContext(ctx, "helm", args...)
	return cmd.Run()
}
