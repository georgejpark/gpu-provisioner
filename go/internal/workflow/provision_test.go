package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/georgejpark/gpu-provisioner/internal/activities"
)

// These tests run the workflow in Temporal's in-memory test environment with
// mocked activities: no server, no cluster, finishes in milliseconds.

func newEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ProvisionCluster)
	env.RegisterActivity(&activities.Activities{Runner: activities.DryRunner{}})
	return env
}

func twoNodes() ProvisionRequest {
	return ProvisionRequest{ClusterID: "t1", JoinToken: "tok", Nodes: []activities.NodeSpec{
		{Name: "n1", Address: "10.0.0.1", GPUs: 1},
		{Name: "n2", Address: "10.0.0.2", GPUs: 1},
	}}
}

func TestProvisionThenTeardown(t *testing.T) {
	env := newEnv(t)
	var a *activities.Activities
	env.OnActivity(a.PrepareNode, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.JoinCluster, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.InstallGPUOperator, mock.Anything).Return(nil)
	env.OnActivity(a.ValidateNode, mock.Anything, mock.Anything).Return(
		func(_ context.Context, n activities.NodeSpec) (activities.ValidationResult, error) {
			return activities.ValidationResult{Node: n.Name, GPUsFound: 1, Passed: true}, nil
		})
	env.OnActivity(a.RemoveNode, mock.Anything, mock.Anything).Return(nil)

	// after provisioning, check status via query, then tear down
	env.RegisterDelayedCallback(func() {
		v, err := env.QueryWorkflow(QueryStatus)
		require.NoError(t, err)
		var st ClusterStatus
		require.NoError(t, v.Get(&st))
		require.Equal(t, "ready", st.Phase)
		require.True(t, st.GPUOperatorReady)
		require.Equal(t, "ready", st.Nodes["n1"].Phase)
		env.SignalWorkflow(SignalTeardown, nil)
	}, time.Minute)

	env.ExecuteWorkflow(ProvisionCluster, twoNodes())
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var final ClusterStatus
	require.NoError(t, env.GetWorkflowResult(&final))
	require.Equal(t, "destroyed", final.Phase)
	require.Equal(t, "removed", final.Nodes["n2"].Phase)
}

func TestFailedNodeMakesClusterDegraded(t *testing.T) {
	env := newEnv(t)
	var a *activities.Activities
	env.OnActivity(a.PrepareNode, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.JoinCluster, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.InstallGPUOperator, mock.Anything).Return(nil)
	env.OnActivity(a.ValidateNode, mock.Anything, mock.Anything).Return(
		func(_ context.Context, n activities.NodeSpec) (activities.ValidationResult, error) {
			if n.Name == "n2" {
				return activities.ValidationResult{Node: n.Name, GPUsFound: 0, Passed: false, Detail: "no gpu"}, nil
			}
			return activities.ValidationResult{Node: n.Name, GPUsFound: 1, Passed: true}, nil
		})
	env.OnActivity(a.RemoveNode, mock.Anything, mock.Anything).Return(nil)

	env.RegisterDelayedCallback(func() {
		v, _ := env.QueryWorkflow(QueryStatus)
		var st ClusterStatus
		require.NoError(t, v.Get(&st))
		require.Equal(t, "degraded", st.Phase)
		require.Equal(t, "failed", st.Nodes["n2"].Phase)
		require.Equal(t, "no gpu", st.Nodes["n2"].LastError)
		env.SignalWorkflow(SignalTeardown, nil)
	}, time.Minute)

	env.ExecuteWorkflow(ProvisionCluster, twoNodes())
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestScaleOutAddsNode(t *testing.T) {
	env := newEnv(t)
	var a *activities.Activities
	env.OnActivity(a.PrepareNode, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.JoinCluster, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.InstallGPUOperator, mock.Anything).Return(nil)
	env.OnActivity(a.ValidateNode, mock.Anything, mock.Anything).Return(
		func(_ context.Context, n activities.NodeSpec) (activities.ValidationResult, error) {
			return activities.ValidationResult{Node: n.Name, GPUsFound: 1, Passed: true}, nil
		})
	env.OnActivity(a.RemoveNode, mock.Anything, mock.Anything).Return(nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalScale, ScaleRequest{Add: []activities.NodeSpec{{Name: "n3", Address: "10.0.0.3", GPUs: 2}}})
	}, time.Minute)
	env.RegisterDelayedCallback(func() {
		v, _ := env.QueryWorkflow(QueryStatus)
		var st ClusterStatus
		require.NoError(t, v.Get(&st))
		require.Equal(t, "ready", st.Nodes["n3"].Phase)
		require.Len(t, st.Nodes, 3)
		env.SignalWorkflow(SignalTeardown, nil)
	}, 2*time.Minute)

	env.ExecuteWorkflow(ProvisionCluster, twoNodes())
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestGPUOperatorFailureFailsWorkflow(t *testing.T) {
	env := newEnv(t)
	var a *activities.Activities
	env.OnActivity(a.PrepareNode, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.JoinCluster, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.InstallGPUOperator, mock.Anything).Return(assertErr("helm timed out"))

	env.ExecuteWorkflow(ProvisionCluster, twoNodes())
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

type assertErr string

func (e assertErr) Error() string { return string(e) }
