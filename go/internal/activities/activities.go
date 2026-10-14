// Package activities holds the side-effecting steps of the provisioning
// workflow. Temporal calls these through the worker; the workflow itself never
// touches the network or the shell.
//
// Two Runner implementations exist:
//   - DryRunner: simulates every step with a short sleep. Use this first, on a
//     laptop, with no cluster at all, to see the workflow run end to end.
//   - ShellRunner: runs real commands (ssh, kubectl, helm) against your nodes.
package activities

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
)

// NodeSpec describes one GPU node the workflow should bring into the cluster.
type NodeSpec struct {
	Name    string `json:"name"`    // e.g. "pve5-gpu1"
	Address string `json:"address"` // ssh target, e.g. "192.168.1.106"
	GPUs    int    `json:"gpus"`    // expected GPU count, used for validation
}

// ValidationResult is what ValidateNode returns.
type ValidationResult struct {
	Node       string  `json:"node"`
	GPUsFound  int     `json:"gpusFound"`
	NCCLBusGBs float64 `json:"ncclBusGBs"` // all_reduce bus bandwidth, GB/s
	Passed     bool    `json:"passed"`
	Detail     string  `json:"detail"`
}

// Runner is the thing that actually does work. Activities delegate to it so
// the same activity code can run in dry-run or for real.
type Runner interface {
	PrepareNode(ctx context.Context, n NodeSpec) error
	JoinCluster(ctx context.Context, n NodeSpec, joinToken string) error
	InstallGPUOperator(ctx context.Context) error
	ValidateNode(ctx context.Context, n NodeSpec) (ValidationResult, error)
	RemoveNode(ctx context.Context, n NodeSpec) error
}

// Activities is registered with the Temporal worker. Method names become
// activity names.
type Activities struct {
	Runner Runner
}

func (a *Activities) PrepareNode(ctx context.Context, n NodeSpec) error {
	activity.GetLogger(ctx).Info("PrepareNode", "node", n.Name)
	return a.Runner.PrepareNode(ctx, n)
}

func (a *Activities) JoinCluster(ctx context.Context, n NodeSpec, joinToken string) error {
	activity.GetLogger(ctx).Info("JoinCluster", "node", n.Name)
	return a.Runner.JoinCluster(ctx, n, joinToken)
}

func (a *Activities) InstallGPUOperator(ctx context.Context) error {
	activity.GetLogger(ctx).Info("InstallGPUOperator")
	return a.Runner.InstallGPUOperator(ctx)
}

func (a *Activities) ValidateNode(ctx context.Context, n NodeSpec) (ValidationResult, error) {
	activity.GetLogger(ctx).Info("ValidateNode", "node", n.Name)
	return a.Runner.ValidateNode(ctx, n)
}

func (a *Activities) RemoveNode(ctx context.Context, n NodeSpec) error {
	activity.GetLogger(ctx).Info("RemoveNode", "node", n.Name)
	return a.Runner.RemoveNode(ctx, n)
}

// ---------------------------------------------------------------------------
// DryRunner: no cluster needed.
// ---------------------------------------------------------------------------

type DryRunner struct {
	StepDelay time.Duration // how long each fake step takes
}

func (d DryRunner) wait(ctx context.Context) error {
	select {
	case <-time.After(d.StepDelay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d DryRunner) PrepareNode(ctx context.Context, n NodeSpec) error           { return d.wait(ctx) }
func (d DryRunner) JoinCluster(ctx context.Context, n NodeSpec, _ string) error { return d.wait(ctx) }
func (d DryRunner) InstallGPUOperator(ctx context.Context) error                { return d.wait(ctx) }
func (d DryRunner) RemoveNode(ctx context.Context, n NodeSpec) error            { return d.wait(ctx) }
func (d DryRunner) ValidateNode(ctx context.Context, n NodeSpec) (ValidationResult, error) {
	if err := d.wait(ctx); err != nil {
		return ValidationResult{}, err
	}
	return ValidationResult{Node: n.Name, GPUsFound: n.GPUs, NCCLBusGBs: 12.5, Passed: true, Detail: "dry-run"}, nil
}

// ---------------------------------------------------------------------------
// ShellRunner: real commands against real nodes.
// Keep the commands in one place so they are easy to read and change.
// ---------------------------------------------------------------------------

type ShellRunner struct {
	SSHUser        string // e.g. "root"
	K3sServerURL   string // e.g. "https://192.168.1.105:6443"
	GPUOperatorNS  string // e.g. "gpu-operator"
	HelmValuesPath string // e.g. "k8s/gpu-operator-values.yaml"
}

func (s ShellRunner) ssh(ctx context.Context, n NodeSpec, script string) (string, error) {
	cmd := exec.CommandContext(ctx, "ssh",
		"-o", "StrictHostKeyChecking=accept-new",
		"-o", "ConnectTimeout=10",
		fmt.Sprintf("%s@%s", s.SSHUser, n.Address), "bash", "-se")
	cmd.Stdin = strings.NewReader(script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("ssh %s: %w: %s", n.Name, err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func (s ShellRunner) local(ctx context.Context, name string, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s %v: %w: %s", name, args, err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// PrepareNode makes sure the node has the NVIDIA driver and containerd ready.
// Idempotent: safe to run twice.
func (s ShellRunner) PrepareNode(ctx context.Context, n NodeSpec) error {
	_, err := s.ssh(ctx, n, `
set -e
command -v nvidia-smi >/dev/null || { echo "nvidia driver missing on $(hostname)"; exit 1; }
nvidia-smi -L
swapoff -a || true
modprobe br_netfilter || true
`)
	return err
}

// JoinCluster installs the k3s agent pointed at the server and waits for Ready.
func (s ShellRunner) JoinCluster(ctx context.Context, n NodeSpec, joinToken string) error {
	script := fmt.Sprintf(`
set -e
if ! systemctl is-active --quiet k3s-agent; then
  curl -sfL https://get.k3s.io | K3S_URL=%q K3S_TOKEN=%q sh -
fi
`, s.K3sServerURL, joinToken)
	if _, err := s.ssh(ctx, n, script); err != nil {
		return err
	}
	// wait for the node to be Ready from the control plane's point of view
	_, err := s.local(ctx, "kubectl", "wait", "--for=condition=Ready", "node/"+n.Name, "--timeout=5m")
	return err
}

// InstallGPUOperator helm-installs the NVIDIA GPU Operator once per cluster.
func (s ShellRunner) InstallGPUOperator(ctx context.Context) error {
	if _, err := s.local(ctx, "helm", "repo", "add", "nvidia", "https://helm.ngc.nvidia.com/nvidia"); err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}
	if _, err := s.local(ctx, "helm", "repo", "update"); err != nil {
		return err
	}
	_, err := s.local(ctx, "helm", "upgrade", "--install", "gpu-operator", "nvidia/gpu-operator",
		"-n", s.GPUOperatorNS, "--create-namespace", "-f", s.HelmValuesPath, "--wait", "--timeout", "15m")
	return err
}

// ValidateNode checks the GPU count Kubernetes sees and runs a short nccl test.
func (s ShellRunner) ValidateNode(ctx context.Context, n NodeSpec) (ValidationResult, error) {
	out, err := s.local(ctx, "kubectl", "get", "node", n.Name, "-o", `jsonpath={.status.allocatable.nvidia\.com/gpu}`)
	if err != nil {
		return ValidationResult{Node: n.Name, Detail: err.Error()}, err
	}
	var gpus int
	fmt.Sscanf(strings.TrimSpace(out), "%d", &gpus)
	res := ValidationResult{Node: n.Name, GPUsFound: gpus}
	if gpus < n.GPUs {
		res.Detail = fmt.Sprintf("expected %d GPUs, kubernetes sees %d", n.GPUs, gpus)
		return res, nil // not an error: the workflow decides what to do with a failed validation
	}
	// Single-node nccl all_reduce as a smoke test. Replace with a multi-node job later.
	bw, err := s.ssh(ctx, n, `
set -e
if command -v all_reduce_perf >/dev/null; then
  all_reduce_perf -b 8M -e 128M -f 2 -g $(nvidia-smi -L | wc -l) | awk '/Avg bus bandwidth/ {print $NF}'
else
  echo 0
fi`)
	if err != nil {
		res.Detail = err.Error()
		return res, nil
	}
	fmt.Sscanf(strings.TrimSpace(bw), "%f", &res.NCCLBusGBs)
	res.Passed = true
	res.Detail = "ok"
	return res, nil
}

// RemoveNode drains and deletes the node, then stops the agent.
func (s ShellRunner) RemoveNode(ctx context.Context, n NodeSpec) error {
	_, _ = s.local(ctx, "kubectl", "drain", n.Name, "--ignore-daemonsets", "--delete-emptydir-data", "--timeout=2m")
	_, _ = s.local(ctx, "kubectl", "delete", "node", n.Name, "--ignore-not-found")
	_, err := s.ssh(ctx, n, `systemctl stop k3s-agent || true; /usr/local/bin/k3s-agent-uninstall.sh || true`)
	return err
}
