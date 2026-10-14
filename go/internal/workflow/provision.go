// Package workflow contains the provisioning state machine. This code is
// deterministic: no network, no clock, no random. Everything that touches the
// world happens in activities, which Temporal retries and records for us.
package workflow

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/georgejpark/gpu-provisioner/internal/activities"
)

// Signal and query names. The Python API and the CLI use these same strings.
const (
	SignalScale    = "scale"    // payload: ScaleRequest
	SignalTeardown = "teardown" // no payload
	SignalHealth   = "health"   // payload: activities.ValidationResult (from the CronJob)
	QueryStatus    = "status"   // returns ClusterStatus
	TaskQueue      = "gpu-provisioner"
)

type ProvisionRequest struct {
	ClusterID string                `json:"clusterId"`
	JoinToken string                `json:"joinToken"`
	Nodes     []activities.NodeSpec `json:"nodes"`
}

type ScaleRequest struct {
	Add    []activities.NodeSpec `json:"add"`
	Remove []string              `json:"remove"` // node names
}

type NodeState struct {
	Spec       activities.NodeSpec          `json:"spec"`
	Phase      string                       `json:"phase"` // pending|preparing|joining|validating|ready|failed|removing|removed
	LastError  string                       `json:"lastError,omitempty"`
	Validation *activities.ValidationResult `json:"validation,omitempty"`
}

type ClusterStatus struct {
	ClusterID        string                `json:"clusterId"`
	Phase            string                `json:"phase"` // provisioning|ready|degraded|tearing-down|destroyed
	GPUOperatorReady bool                  `json:"gpuOperatorReady"`
	Nodes            map[string]*NodeState `json:"nodes"`
	LastHealthCheck  string                `json:"lastHealthCheck,omitempty"`
}

// Retry policy for activities: real infrastructure fails in transient ways
// (ssh timeouts, apt locks, helm waits), so retry with backoff, but give up
// after a bounded number of attempts so the node is marked failed, not stuck.
var activityOpts = workflow.ActivityOptions{
	StartToCloseTimeout: 20 * time.Minute,
	HeartbeatTimeout:    0,
	RetryPolicy: &temporal.RetryPolicy{
		InitialInterval:    10 * time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    2 * time.Minute,
		MaximumAttempts:    4,
	},
}

// ProvisionCluster brings a set of GPU nodes into a Kubernetes cluster, installs
// the NVIDIA GPU Operator, validates each node, then stays alive to handle
// scale and teardown signals and health reports until torn down.
func ProvisionCluster(ctx workflow.Context, req ProvisionRequest) (ClusterStatus, error) {
	logger := workflow.GetLogger(ctx)
	ctx = workflow.WithActivityOptions(ctx, activityOpts)
	var acts *activities.Activities

	status := ClusterStatus{ClusterID: req.ClusterID, Phase: "provisioning", Nodes: map[string]*NodeState{}}
	for _, n := range req.Nodes {
		status.Nodes[n.Name] = &NodeState{Spec: n, Phase: "pending"}
	}

	// Queries are answered from workflow state, so the API can show progress
	// without a database.
	if err := workflow.SetQueryHandler(ctx, QueryStatus, func() (ClusterStatus, error) {
		return status, nil
	}); err != nil {
		return status, err
	}

	// provisionNode runs the per-node steps and records the outcome. It never
	// returns an error to the workflow: a failed node is state, not a crash.
	provisionNode := func(ctx workflow.Context, ns *NodeState) {
		ns.Phase = "preparing"
		if err := workflow.ExecuteActivity(ctx, acts.PrepareNode, ns.Spec).Get(ctx, nil); err != nil {
			ns.Phase, ns.LastError = "failed", err.Error()
			return
		}
		ns.Phase = "joining"
		if err := workflow.ExecuteActivity(ctx, acts.JoinCluster, ns.Spec, req.JoinToken).Get(ctx, nil); err != nil {
			ns.Phase, ns.LastError = "failed", err.Error()
			return
		}
		ns.Phase = "validating"
		var res activities.ValidationResult
		if err := workflow.ExecuteActivity(ctx, acts.ValidateNode, ns.Spec).Get(ctx, &res); err != nil {
			ns.Phase, ns.LastError = "failed", err.Error()
			return
		}
		ns.Validation = &res
		if res.Passed {
			ns.Phase = "ready"
		} else {
			ns.Phase, ns.LastError = "failed", res.Detail
		}
	}

	// Nodes are prepared and joined in parallel; each one is its own coroutine.
	provisionAll := func(ctx workflow.Context, nodes []*NodeState) {
		wg := workflow.NewWaitGroup(ctx)
		for _, ns := range nodes {
			ns := ns
			wg.Add(1)
			workflow.Go(ctx, func(ctx workflow.Context) {
				defer wg.Done()
				provisionNode(ctx, ns)
			})
		}
		wg.Wait(ctx)
	}

	recomputePhase := func() {
		ready, failed := 0, 0
		for _, ns := range status.Nodes {
			switch ns.Phase {
			case "ready":
				ready++
			case "failed":
				failed++
			}
		}
		switch {
		case ready > 0 && failed == 0:
			status.Phase = "ready"
		case ready > 0:
			status.Phase = "degraded"
		default:
			status.Phase = "failed"
		}
	}

	// --- Phase 1: initial provisioning -----------------------------------
	// Join the nodes first (the operator needs at least one GPU node), then
	// install the operator once, then validate. Validation inside provisionNode
	// is deliberately done after the operator is up, so split the steps here.
	initial := make([]*NodeState, 0, len(status.Nodes))
	for _, n := range req.Nodes {
		initial = append(initial, status.Nodes[n.Name])
	}

	// prepare + join only
	wg := workflow.NewWaitGroup(ctx)
	for _, ns := range initial {
		ns := ns
		wg.Add(1)
		workflow.Go(ctx, func(ctx workflow.Context) {
			defer wg.Done()
			ns.Phase = "preparing"
			if err := workflow.ExecuteActivity(ctx, acts.PrepareNode, ns.Spec).Get(ctx, nil); err != nil {
				ns.Phase, ns.LastError = "failed", err.Error()
				return
			}
			ns.Phase = "joining"
			if err := workflow.ExecuteActivity(ctx, acts.JoinCluster, ns.Spec, req.JoinToken).Get(ctx, nil); err != nil {
				ns.Phase, ns.LastError = "failed", err.Error()
				return
			}
			ns.Phase = "joined"
		})
	}
	wg.Wait(ctx)

	if err := workflow.ExecuteActivity(ctx, acts.InstallGPUOperator).Get(ctx, nil); err != nil {
		status.Phase = "failed"
		return status, fmt.Errorf("gpu operator install failed: %w", err)
	}
	status.GPUOperatorReady = true

	// validate the joined nodes
	wg = workflow.NewWaitGroup(ctx)
	for _, ns := range initial {
		ns := ns
		if ns.Phase != "joined" {
			continue
		}
		wg.Add(1)
		workflow.Go(ctx, func(ctx workflow.Context) {
			defer wg.Done()
			ns.Phase = "validating"
			var res activities.ValidationResult
			if err := workflow.ExecuteActivity(ctx, acts.ValidateNode, ns.Spec).Get(ctx, &res); err != nil {
				ns.Phase, ns.LastError = "failed", err.Error()
				return
			}
			ns.Validation = &res
			if res.Passed {
				ns.Phase = "ready"
			} else {
				ns.Phase, ns.LastError = "failed", res.Detail
			}
		})
	}
	wg.Wait(ctx)
	recomputePhase()
	logger.Info("initial provisioning finished", "phase", status.Phase)

	// --- Phase 2: long-lived control loop ---------------------------------
	// The workflow now waits for signals. This is the part Temporal is for:
	// it can sit here for weeks, survive worker restarts, and pick up exactly
	// where it left off.
	scaleCh := workflow.GetSignalChannel(ctx, SignalScale)
	teardownCh := workflow.GetSignalChannel(ctx, SignalTeardown)
	healthCh := workflow.GetSignalChannel(ctx, SignalHealth)

	for {
		var scale ScaleRequest
		var health activities.ValidationResult
		teardown := false

		sel := workflow.NewSelector(ctx)
		sel.AddReceive(scaleCh, func(c workflow.ReceiveChannel, _ bool) { c.Receive(ctx, &scale) })
		sel.AddReceive(healthCh, func(c workflow.ReceiveChannel, _ bool) { c.Receive(ctx, &health) })
		sel.AddReceive(teardownCh, func(c workflow.ReceiveChannel, _ bool) { c.Receive(ctx, nil); teardown = true })
		sel.Select(ctx)

		switch {
		case teardown:
			status.Phase = "tearing-down"
			for _, ns := range status.Nodes {
				if ns.Phase == "removed" {
					continue
				}
				ns.Phase = "removing"
				if err := workflow.ExecuteActivity(ctx, acts.RemoveNode, ns.Spec).Get(ctx, nil); err != nil {
					ns.LastError = err.Error()
				}
				ns.Phase = "removed"
			}
			status.Phase = "destroyed"
			return status, nil

		case health.Node != "":
			// A health report from the nightly CronJob. Update state; if a node
			// went bad, the cluster is degraded until someone scales it out.
			if ns, ok := status.Nodes[health.Node]; ok {
				h := health
				ns.Validation = &h
				if !h.Passed && ns.Phase == "ready" {
					ns.Phase, ns.LastError = "failed", h.Detail
				}
			}
			status.LastHealthCheck = workflow.Now(ctx).UTC().Format(time.RFC3339)
			recomputePhase()

		default:
			// scale request
			for _, name := range scale.Remove {
				if ns, ok := status.Nodes[name]; ok && ns.Phase != "removed" {
					ns.Phase = "removing"
					if err := workflow.ExecuteActivity(ctx, acts.RemoveNode, ns.Spec).Get(ctx, nil); err != nil {
						ns.LastError = err.Error()
					}
					ns.Phase = "removed"
				}
			}
			added := make([]*NodeState, 0, len(scale.Add))
			for _, spec := range scale.Add {
				if _, exists := status.Nodes[spec.Name]; exists && status.Nodes[spec.Name].Phase != "removed" {
					continue
				}
				ns := &NodeState{Spec: spec, Phase: "pending"}
				status.Nodes[spec.Name] = ns
				added = append(added, ns)
			}
			if len(added) > 0 {
				status.Phase = "provisioning"
				provisionAll(ctx, added)
			}
			recomputePhase()
		}
	}
}
