// The worker hosts the workflow and activity code. Run one or more of these;
// Temporal hands them work from the task queue.
package main

import (
	"log"
	"os"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/georgejpark/gpu-provisioner/internal/activities"
	wf "github.com/georgejpark/gpu-provisioner/internal/workflow"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	c, err := client.Dial(client.Options{HostPort: env("TEMPORAL_ADDRESS", "localhost:7233")})
	if err != nil {
		log.Fatalf("temporal dial: %v", err)
	}
	defer c.Close()

	// DRY_RUN=1 (default) simulates every step. Set DRY_RUN=0 to run real
	// ssh/kubectl/helm commands against the nodes.
	var runner activities.Runner = activities.DryRunner{StepDelay: 2 * time.Second}
	if env("DRY_RUN", "1") == "0" {
		runner = activities.ShellRunner{
			SSHUser:        env("SSH_USER", "root"),
			K3sServerURL:   env("K3S_URL", "https://192.168.1.105:6443"),
			GPUOperatorNS:  env("GPU_OPERATOR_NS", "gpu-operator"),
			HelmValuesPath: env("GPU_OPERATOR_VALUES", "../k8s/gpu-operator-values.yaml"),
		}
	}

	w := worker.New(c, wf.TaskQueue, worker.Options{})
	w.RegisterWorkflow(wf.ProvisionCluster)
	w.RegisterActivity(&activities.Activities{Runner: runner})

	log.Printf("worker started on task queue %q (dry-run=%v)", wf.TaskQueue, env("DRY_RUN", "1") != "0")
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker: %v", err)
	}
}
