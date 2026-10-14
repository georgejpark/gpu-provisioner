# GPU Cluster Provisioner

Turn a set of GPU machines into a running Kubernetes cluster with one API call, and keep it that way.

A **Temporal** workflow written in **Go** owns the provisioning state machine: prepare each node, join it to k3s, install the NVIDIA GPU Operator, validate every GPU, then stay alive to handle scale-out, scale-in, nightly health reports and teardown. A **Flask** API in **Python** exposes that as `POST /clusters`. **Celery** workers run the slow one-off jobs (burn-in, benchmarks). A Kubernetes **CronJob** feeds health checks back into the workflow.

Built and run on a home Proxmox cluster with NVIDIA GPUs and a Cisco Nexus 9K fabric.

```
                 ┌────────────┐   start / signal / query   ┌──────────────────────────┐
  curl ─────────►│ Flask API  │ ─────────────────────────► │ Temporal server          │
                 │ (Python)   │                            │ (docker compose / k8s)   │
                 └─────┬──────┘                            └──────────┬───────────────┘
                       │ .delay()                                     │ task queue "gpu-provisioner"
                 ┌─────▼──────┐                            ┌──────────▼───────────────┐
                 │ Celery     │  health signal             │ Go worker                │
                 │ workers    │ ─────────────────────────► │  ProvisionCluster wf     │
                 └────────────┘                            │  activities: ssh/kubectl │
                 ┌────────────┐  health signal             │              /helm       │
                 │ CronJob    │ ─────────────────────────► └──────────┬───────────────┘
                 │ (nightly)  │                                       │
                 └────────────┘                            ┌──────────▼───────────────┐
                                                           │ k3s + NVIDIA GPU Operator│
                                                           │ pve3 / pve5 GPU nodes    │
                                                           └──────────────────────────┘
```

## Why Temporal, and why Celery too

Provisioning a cluster is a long job with many steps that can each fail, and it must not be lost halfway through. Temporal records every step, retries failed activities with backoff, and lets the workflow sleep for days waiting for a signal, surviving worker restarts. That is exactly what a cluster lifecycle needs.

Celery is for the opposite case: short, fire-and-forget jobs where "run it again" is fine. Benchmarks and burn-in belong there. Keeping both shows when each tool fits.

## Run it in 15 minutes with no cluster (dry run)

Requirements: Docker, Go 1.22+, Python 3.11+.

```bash
# 1. Temporal + Redis
make up                          # Temporal UI: http://localhost:8233

# 2. Go worker in dry-run mode (simulates ssh/kubectl/helm with 2s sleeps)
make tidy                        # once: resolves go.sum
make test                        # 4 workflow tests, run in Temporal's in-memory test env
make worker                      # leave running in a terminal

# 3. Start a cluster from the CLI
cd go
go run ./cmd/starter create lab1 pve5-gpu0=192.168.1.106:1 pve3-gpu0=192.168.1.105:1
go run ./cmd/starter status lab1          # watch phases: pending -> preparing -> joining -> validating -> ready
go run ./cmd/starter scale  lab1 add pve3-gpu1=192.168.1.105:1
go run ./cmd/starter health lab1 pve3-gpu0 false "simulated failure"   # cluster goes degraded
go run ./cmd/starter destroy lab1

# 4. Same thing through the Flask API
cd ../python && python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
make api            # terminal A
make celery         # terminal B
curl -X POST localhost:8080/clusters -H 'content-type: application/json' \
  -d '{"clusterId":"lab2","nodes":[{"name":"pve5-gpu0","address":"192.168.1.106","gpus":1}]}'
curl localhost:8080/clusters/lab2
curl -X POST localhost:8080/clusters/lab2/benchmark      # Celery job -> health signal
curl -X DELETE localhost:8080/clusters/lab2
```

Open the Temporal UI and click the workflow: every activity, retry and signal is in the event history. That history is the demo.

## Run it for real on the Proxmox nodes

1. Pick a control-plane host (a small VM is fine). Run `k8s/install-k3s-server.sh` there. Copy the printed `K3S_URL` and `K3S_TOKEN`, and copy `/etc/rancher/k3s/k3s.yaml` to `~/.kube/config` on the machine that runs the Go worker.
2. On each GPU node: NVIDIA driver installed and `nvidia-smi` working, ssh key auth from the worker machine as root (or set `SSH_USER`), and `nvidia-container-toolkit` installed (`apt install nvidia-container-toolkit`).
3. Start the worker for real:
   ```bash
   cd go && DRY_RUN=0 K3S_URL=https://<server-ip>:6443 go run ./cmd/worker
   ```
4. Create the cluster with the real token:
   ```bash
   K3S_TOKEN=<token> go run ./cmd/starter create lab1 pve5=192.168.1.106:2
   ```
   The workflow will ssh to the node, install the k3s agent, wait for `Ready`, helm-install the GPU Operator with `k8s/gpu-operator-values.yaml` (host driver, k3s containerd paths), then read `nvidia.com/gpu` allocatable and run a single-node `all_reduce_perf` if `nccl-tests` is on the node.
5. Optional: `kubectl apply -f k8s/nccl-test-job.yaml` to run nccl-tests inside the cluster on 2 GPUs.
6. Deploy the nightly check: build `python/Dockerfile`, push it, edit the image in `python/cron/healthcheck-cronjob.yaml`, create a ServiceAccount with `get,list` on nodes, and `kubectl apply`.

## Layout

```
go/
  cmd/worker        Temporal worker (DRY_RUN=1 simulates, DRY_RUN=0 runs ssh/kubectl/helm)
  cmd/starter       CLI: create / status / scale / health / destroy
  internal/workflow ProvisionCluster workflow: state machine, signals, query, retries
  internal/activities
                    Activities + Runner interface, DryRunner and ShellRunner
python/
  api.py            Flask API (start/query/signal the Go workflow)
  tasks.py          Celery benchmark job that reports back via a health signal
  temporal_client.py
  cron/             healthcheck.py + CronJob manifest
k8s/                GPU Operator values, k3s server install, nccl test job
docker-compose.yml  Temporal dev server + UI, Redis
```

## Things to be able to explain in an interview

- **Determinism.** The workflow function never calls the network, the clock, or random. All of that is in activities. That's what lets Temporal replay the history after a crash.
- **Retries.** Activities retry 4 times with 10s → 2m backoff. A node that still fails is recorded as `failed`; the cluster becomes `degraded`, not dead. Only a GPU Operator install failure fails the whole workflow, because nothing works without it.
- **Signals vs queries.** Signals (`scale`, `teardown`, `health`) change state. The `status` query reads it without touching the database. The API has no database of its own.
- **Why `InstallGPUOperator` runs once, after the joins.** The operator needs a node to land on; validation needs the operator. Order matters, and the workflow encodes it.
- **Workflow ID = `cluster-<id>`.** Starting the same cluster twice is rejected by Temporal, which is the idempotency guarantee for free.
- **What I'd add next.** Child workflows per node for very large clusters, a Temporal schedule instead of the Kubernetes CronJob, continue-as-new to keep the event history bounded, multi-node NCCL validation, and Proxmox API activities to clone VMs so "create" starts from nothing.
