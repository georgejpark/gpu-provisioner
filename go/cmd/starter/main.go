// A small CLI for driving the workflow without the Python API:
//
//	starter create  <clusterId> <node1>=<addr>:<gpus> [<node2>=...]
//	starter status  <clusterId>
//	starter scale   <clusterId> add <node>=<addr>:<gpus> | remove <node>
//	starter health  <clusterId> <node> <passed:true|false> [detail]
//	starter destroy <clusterId>
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"go.temporal.io/sdk/client"

	"github.com/georgejpark/gpu-provisioner/internal/activities"
	wf "github.com/georgejpark/gpu-provisioner/internal/workflow"
)

func parseNode(s string) activities.NodeSpec {
	// name=addr:gpus
	name, rest, _ := strings.Cut(s, "=")
	addr, gpus, _ := strings.Cut(rest, ":")
	n := activities.NodeSpec{Name: name, Address: addr, GPUs: 1}
	fmt.Sscanf(gpus, "%d", &n.GPUs)
	return n
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: starter <create|status|scale|health|destroy> <clusterId> ...")
		os.Exit(2)
	}
	addr := os.Getenv("TEMPORAL_ADDRESS")
	if addr == "" {
		addr = "localhost:7233"
	}
	c, err := client.Dial(client.Options{HostPort: addr})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	cmd, id := os.Args[1], os.Args[2]
	wfID := "cluster-" + id

	switch cmd {
	case "create":
		req := wf.ProvisionRequest{ClusterID: id, JoinToken: os.Getenv("K3S_TOKEN")}
		for _, a := range os.Args[3:] {
			req.Nodes = append(req.Nodes, parseNode(a))
		}
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: wfID, TaskQueue: wf.TaskQueue}, wf.ProvisionCluster, req)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("started %s run %s\n", run.GetID(), run.GetRunID())

	case "status":
		resp, err := c.QueryWorkflow(ctx, wfID, "", wf.QueryStatus)
		if err != nil {
			log.Fatal(err)
		}
		var st wf.ClusterStatus
		if err := resp.Get(&st); err != nil {
			log.Fatal(err)
		}
		out, _ := json.MarshalIndent(st, "", "  ")
		fmt.Println(string(out))

	case "scale":
		if len(os.Args) < 5 {
			log.Fatal("scale <clusterId> add|remove <node...>")
		}
		var req wf.ScaleRequest
		for _, a := range os.Args[4:] {
			if os.Args[3] == "add" {
				req.Add = append(req.Add, parseNode(a))
			} else {
				req.Remove = append(req.Remove, a)
			}
		}
		if err := c.SignalWorkflow(ctx, wfID, "", wf.SignalScale, req); err != nil {
			log.Fatal(err)
		}
		fmt.Println("scale signal sent")

	case "health":
		if len(os.Args) < 5 {
			log.Fatal("health <clusterId> <node> <true|false> [detail]")
		}
		res := activities.ValidationResult{Node: os.Args[3], Passed: os.Args[4] == "true", Detail: strings.Join(os.Args[5:], " ")}
		if err := c.SignalWorkflow(ctx, wfID, "", wf.SignalHealth, res); err != nil {
			log.Fatal(err)
		}
		fmt.Println("health signal sent")

	case "destroy":
		if err := c.SignalWorkflow(ctx, wfID, "", wf.SignalTeardown, nil); err != nil {
			log.Fatal(err)
		}
		fmt.Println("teardown signal sent")

	default:
		log.Fatalf("unknown command %q", cmd)
	}
}
