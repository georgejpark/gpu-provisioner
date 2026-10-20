# Running gpu-provisioner on Proxmox (pve2)

This repo was written for a home lab, not a cloud. This page records what it actually depends on, how to lay it out on the Proxmox host **pve2** with its two **NVIDIA CMP 170HX** cards, what replaces the cloud services a hosted deployment would use, and how to test it in stages.

## 1. Cloud-dependency verdict: NONE

No AWS, Azure or GCP SDK, no Terraform provider, no cloud credential is used anywhere. Evidence, with file and line:

| Where | What it is | Cloud? |
|---|---|---|
| `go/go.mod:5-8` | Direct Go dependencies: `github.com/stretchr/testify v1.10.0`, `go.temporal.io/sdk v1.34.0` | No |
| `go/go.mod:28-29` | `google.golang.org/genproto/googleapis/api` and `.../rpc` (indirect) | No: protobuf/gRPC type definitions pulled in by `go.temporal.io/api`; not Google Cloud client libraries |
| `python/requirements.txt:1-5` | `flask`, `temporalio`, `celery[redis]`, `redis`, `gunicorn` | No |
| whole tree | No `*.tf`, `*.tfvars` or `*.hcl`; the string `terraform` does not occur | No |
| whole tree | `grep -rniE 'aws\|boto\|azure\|gcp\|ec2\|eks\|gke\|aks\|cloudformation\|pulumi'` matches nothing in source (only random letters inside `go/go.sum` hashes, and the word "weeks" in `go/internal/workflow/provision.go:214`) | No |
| `go/internal/activities/activities.go:118-137` | The only side effects: `ssh`, `kubectl`, `helm` via `os/exec` | No |
| `go/internal/activities/activities.go:157`, `k8s/install-k3s-server.sh:5` | `curl https://get.k3s.io` k3s installer | Internet download at install time, no account |
| `go/internal/activities/activities.go:170` | Helm repo `https://helm.ngc.nvidia.com/nvidia` (GPU Operator chart) | Internet download at install time, no account |
| `python/Dockerfile:3` | `kubectl` downloaded from `dl.k8s.io` at image build time | Internet download at build time, no account |
| `python/cron/healthcheck-cronjob.yaml:17` | Image `ghcr.io/georgejpark/gpu-provisioner-python:latest` (GitHub Container Registry) | OPTIONAL: any registry, or import the image into k3s (section 3.6) |
| `k8s/nccl-test-job.yaml:15` | `nvcr.io/nvidia/pytorch:24.08-py3` (NVIDIA NGC public image) for the optional nccl job | OPTIONAL: public pull, no account |
| `python/tasks.py:43-44` | Benchmark runs `ssh root@<address>` for `gpu_burn` or `nvidia-smi` | No |
| `python/cron/healthcheck.py:19-21` | `kubectl get nodes -o json` | No |

Everything the code talks to is on your LAN: a Temporal server (gRPC), Redis, the k3s API server, and the nodes over ssh.

## 2. What it needs to run

**Runtimes**

| Component | Runtime | Source |
|---|---|---|
| Temporal worker + `starter` CLI | Go 1.24+ | `go/go.mod:3` |
| Flask API, Celery worker, healthcheck | Python 3.12 (3.11+ works; CI and the image use 3.12) | `python/Dockerfile:1`, `.github/workflows/ci.yml` |
| Local dev stack | Docker + `docker compose` | `docker-compose.yml`, `Makefile` |

**Services**

| Service | Used by | Provided by |
|---|---|---|
| Temporal server (gRPC `:7233`, web UI `:8233`) | Go worker, `starter`, Flask, Celery, CronJob | `docker-compose.yml:4-9`: `temporalio/temporal:latest`, `server start-dev --ip 0.0.0.0 --ui-port 8233`. **The dev server keeps state in memory**: restarting the container forgets every workflow. Add `--db-filename /data/temporal.db` and a volume to persist, or run full self-hosted Temporal. |
| Redis 7 (`:6379`) | Celery broker and result backend | `docker-compose.yml:10-13`: `redis:7-alpine` |
| PostgreSQL | not used | Only needed if you replace the dev server with full self-hosted Temporal (its persistence store) |
| k3s API server (`:6443`) | Worker (`kubectl`, `helm`), healthcheck | `k8s/install-k3s-server.sh` on the control-plane VM |

**On the machine that runs the Go worker** (`DRY_RUN=0`): `ssh`, `kubectl` and `helm` on `PATH`; `~/.kube/config` pointing at the k3s server; an ssh key accepted by every GPU node as `SSH_USER` (default `root`). The Celery worker needs the same ssh access, as `root` (hard-coded in `python/tasks.py:43`).

**On each GPU node** (what `PrepareNode`, `JoinCluster` and `ValidateNode` expect, `go/internal/activities/activities.go:141-210`): the NVIDIA driver installed and `nvidia-smi` working; `nvidia-container-toolkit` installed; outbound HTTPS to `get.k3s.io`; optionally `all_reduce_perf` (nccl-tests) and `gpu_burn` on `PATH`, otherwise validation reports 0 GB/s and the benchmark falls back to `nvidia-smi`. The GPU Operator is installed with `k8s/gpu-operator-values.yaml`: `driver.enabled=false` (the node's own driver is used), container toolkit pointed at k3s's containerd paths, device plugin, DCGM exporter and GPU feature discovery on, MIG off.

**GPU**: anything the NVIDIA driver and the GPU Operator's device plugin recognise. Here: two CMP 170HX, each passed through to a VM. `k8s/gpu-operator-values.yaml:3` notes that the CMP 170HX needs the patched driver; whatever driver you would use bare-metal goes inside the guest.

**Ports**

| Port | Where | Why |
|---|---|---|
| 7233/tcp | control VM | Temporal gRPC (worker, CLI, Flask, Celery, CronJob pod) |
| 8233/tcp | control VM | Temporal web UI |
| 6379/tcp | control VM | Redis (only Flask and Celery need it) |
| 8080/tcp | control VM | Flask API (`flask run -p 8080`, or gunicorn in the image) |
| 6443/tcp | k3s server VM | Kubernetes API, used by `kubectl`/`helm` from the control VM and by the agents |
| 8472/udp, 10250/tcp | between k3s VMs | k3s defaults: flannel VXLAN and kubelet |
| 22/tcp | every GPU node | ssh from the control VM |

**Environment variables** (all have defaults unless marked)

| Variable | Default | Read in |
|---|---|---|
| `TEMPORAL_ADDRESS` | `localhost:7233` | `go/cmd/worker/main.go:25`, `go/cmd/starter/main.go:38`, `python/settings.py:3` |
| `DRY_RUN` | `1` (simulate); `0` runs ssh/kubectl/helm | `go/cmd/worker/main.go:34` |
| `SSH_USER` | `root` | `go/cmd/worker/main.go:36` |
| `K3S_URL` | `https://192.168.1.105:6443` | `go/cmd/worker/main.go:37` |
| `GPU_OPERATOR_NS` | `gpu-operator` | `go/cmd/worker/main.go:38` |
| `GPU_OPERATOR_VALUES` | `../k8s/gpu-operator-values.yaml` (relative to `go/`, i.e. run via `make worker-real` from the repo root or set an absolute path) | `go/cmd/worker/main.go:39` |
| `K3S_TOKEN` | empty; needed for a real `create` | `go/cmd/starter/main.go:53`, `python/settings.py:7` |
| `REDIS_URL` | `redis://localhost:6379/0` | `python/settings.py:6` |
| `FLASK_APP` | set to `api` by `make api` | `Makefile:22` |
| `CLUSTER_ID` | **required** | `python/cron/healthcheck.py:15` |

## 3. Proxmox plan for pve2

pve2 is an HP Z8 G4 with two Xeon Platinum 8269CY (52 cores / 104 threads), 64 GB RAM, Proxmox on a ZFS root, two NVIDIA CMP 170HX. IOMMU (`intel_iommu=on iommu=pt`), the vfio modules, the nouveau blacklist and PCIe ACS are already in place on the host, so GPU passthrough needs only the per-card steps below.

### 3.1 Guests

| VMID | Name | Role | vCPU | RAM | Disk | Notes |
|---|---|---|---|---|---|---|
| 200 | `gpu-ctl` | Temporal + Redis (`docker-compose.yml`), Go worker, Flask API, Celery, `kubectl`, `helm` | 4 | 8 GB | 40 GB | Debian VM. An LXC with `nesting=1` can run Docker too, but a VM keeps the compose stack boring. |
| 201 | `k3s-server` | k3s control plane (`k8s/install-k3s-server.sh`) | 2 | 4 GB | 32 GB | No GPU. Can be folded into 200 to save RAM; kept separate so the worker's `kubectl`/`helm` talk to a real remote API server. |
| 210 | `gpu-node-1` | k3s agent with **both** CMP 170HX passed through | 8 | 16 GB | 64 GB | q35, OVMF, `cpu: host`, `balloon: 0`, `hostpci0`/`hostpci1` with `pcie=1` |

Total: 14 vCPU and 28 GB of pve2's 64 GB, leaving room for everything else on the host.

**Why both cards in one VM (option A).** `k8s/nccl-test-job.yaml:24` asks for `nvidia.com/gpu: 2` in a single pod, and `ValidateNode` runs `all_reduce_perf -g $(nvidia-smi -L | wc -l)` on the node (`activities.go:198`), so one node with two GPUs gives a real two-GPU NCCL number. Create the cluster as `gpu-node-1=192.168.1.210:2`.

**Option B: one card per VM.** Add VM 211 `gpu-node-2` (8 vCPU, 12 GB, 64 GB) with the second card, and create `gpu-node-1=192.168.1.210:1 gpu-node-2=192.168.1.211:1`. This exercises the parallel join, `scale add/remove` and `degraded` paths with real nodes, at the cost of the two-GPU NCCL test. Both options use the same commands; only the `hostpci` lines and the `starter create` arguments differ.

Addresses used on this page: `gpu-ctl` 192.168.1.200, `k3s-server` 192.168.1.201, `gpu-node-1` 192.168.1.210, `gpu-node-2` 192.168.1.211. They are static picks for the lab; if they collide with your DHCP range, change them in one place (the VM's network config) and in the commands below.

### 3.2 On pve2 (Proxmox web console or `ssh pve2`, as root): find the cards and bind them to vfio

```bash
lspci -nnD -d 10de:
for d in $(lspci -D -d 10de: | awk '{print $1}'); do echo "$d iommu_group $(basename "$(readlink /sys/bus/pci/devices/$d/iommu_group)")"; done
lspci -k -d 10de:
```

Expect exactly two NVIDIA entries, each in its own IOMMU group, and no `Kernel driver in use:` line for either (or `vfio-pci`). If a host driver has claimed them, pin them to vfio-pci by vendor:device id (read from the first command's output) and rebuild the initramfs. pve2 boots with `proxmox-boot-tool`, so refresh it too:

```bash
ID=$(lspci -n -d 10de: | awk 'NR==1{print $3}')
echo "options vfio-pci ids=$ID" > /etc/modprobe.d/vfio-gpu.conf
cat /etc/modprobe.d/vfio-gpu.conf
update-initramfs -u -k all && proxmox-boot-tool refresh
```

Reboot pve2 only if a driver had claimed the cards; otherwise skip the reboot.

### 3.3 On pve2: create the GPU VM and attach the cards

Create VM 210 in the Proxmox web UI with the sizes from the table: machine `q35`, BIOS `OVMF` (adds the EFI disk), CPU type `host`, 8 cores, 16384 MB, 64 GB disk, a Debian netinst ISO, network on `vmbr0`. Install Debian with hostname `gpu-node-1` (the hostname **must** equal the node name you will pass to `starter create`, because the workflow runs `kubectl wait --for=condition=Ready node/gpu-node-1`, `activities.go:164`, and k3s names the node after the hostname). Then, with the VM stopped, attach both cards and disable ballooning (VFIO needs the VM's memory pinned):

```bash
GPU1=$(lspci -D -d 10de: | awk 'NR==1{print $1}')
GPU2=$(lspci -D -d 10de: | awk 'NR==2{print $1}')
echo "card 1: $GPU1   card 2: $GPU2"
qm set 210 --cpu host --balloon 0 --hostpci0 "$GPU1",pcie=1 --hostpci1 "$GPU2",pcie=1
qm config 210 | grep -E '^(hostpci|balloon|cpu|machine|bios)'
qm start 210
```

For option B, run `qm set 210 --hostpci0 "$GPU1",pcie=1` and `qm set 211 --hostpci0 "$GPU2",pcie=1` instead.

### 3.4 Inside `gpu-node-1` (as root)

1. Confirm the cards are visible: `lspci -nn -d 10de:` lists two devices.
2. Install the NVIDIA driver you use for the CMP 170HX (see the note in `k8s/gpu-operator-values.yaml:3`), reboot, and confirm:
   ```bash
   nvidia-smi -L
   ```
   Expect two lines.
3. Install `nvidia-container-toolkit` from NVIDIA's apt repository (the GPU Operator's toolkit component configures k3s's containerd for it; the package itself must be present, as the README's "Run it for real" step says).
4. Optional, for real validation numbers: build [nccl-tests](https://github.com/NVIDIA/nccl-tests) and put `all_reduce_perf` on `PATH`; put `gpu_burn` on `PATH` for the Celery benchmark.
5. Allow the control VM in as root with a key: paste `gpu-ctl`'s public key into `/root/.ssh/authorized_keys` and keep `PermitRootLogin prohibit-password`.

### 3.5 Inside `k3s-server` (VM 201, as root)

```bash
apt-get install -y git curl
git clone https://github.com/georgejpark/gpu-provisioner.git /root/gpu-provisioner
bash /root/gpu-provisioner/k8s/install-k3s-server.sh
```

The script installs k3s without traefik, then prints `K3S_URL=https://192.168.1.201:6443`, `K3S_TOKEN=...` and the kubeconfig path. Note that a k3s server is also a schedulable node, so the nightly CronJob pod may land here; that matters for section 3.6.

### 3.6 Inside `gpu-ctl` (VM 200, as george)

Install Docker (with the compose plugin), Go 1.24+, `python3-venv`, `kubectl` and `helm`, then:

```bash
git clone https://github.com/georgejpark/gpu-provisioner.git ~/gpu-provisioner
cd ~/gpu-provisioner
make up                                                    # Temporal :7233/:8233, Redis :6379
mkdir -p ~/.kube
scp root@192.168.1.201:/etc/rancher/k3s/k3s.yaml ~/.kube/config
sed -i 's#https://127.0.0.1:6443#https://192.168.1.201:6443#' ~/.kube/config
kubectl get nodes
```

Run the worker for real, in its own terminal (this passes `K3S_URL` through `make worker-real`; `GPU_OPERATOR_VALUES` keeps its default because `make` runs the worker from `go/`):

```bash
cd ~/gpu-provisioner && make worker-real K3S_URL=https://192.168.1.201:6443
```

Create the cluster with the token read live from the k3s server (nothing is written to disk):

```bash
cd ~/gpu-provisioner/go && K3S_TOKEN=$(ssh root@192.168.1.201 cat /var/lib/rancher/k3s/server/node-token) go run ./cmd/starter create lab1 gpu-node-1=192.168.1.210:2
```

API and Celery, each in its own terminal:

```bash
cd ~/gpu-provisioner && python3 -m venv python/.venv && . python/.venv/bin/activate && pip install -r python/requirements.txt && make api
```

```bash
cd ~/gpu-provisioner && . python/.venv/bin/activate && make celery
```

Nightly CronJob. The manifest's `TEMPORAL_ADDRESS` (`python/cron/healthcheck-cronjob.yaml:23`) assumes Temporal runs inside the cluster; here it runs on `gpu-ctl`, so point it at `192.168.1.200:7233`. The image tag is `:latest`, which Kubernetes always pulls, so the simplest path is to push the image to GHCR as a public package; if you prefer to import it into every k3s node instead, add `imagePullPolicy: IfNotPresent` to the manifest.

```bash
cd ~/gpu-provisioner
docker build -t ghcr.io/georgejpark/gpu-provisioner-python:latest python
docker push ghcr.io/georgejpark/gpu-provisioner-python:latest
sed -i 's#value: "temporal-frontend.temporal.svc:7233"#value: "192.168.1.200:7233"#' python/cron/healthcheck-cronjob.yaml
kubectl create namespace gpu-provisioner
kubectl -n gpu-provisioner create serviceaccount gpu-healthcheck
kubectl create clusterrole gpu-healthcheck --verb=get,list --resource=nodes
kubectl create clusterrolebinding gpu-healthcheck --clusterrole=gpu-healthcheck --serviceaccount=gpu-provisioner:gpu-healthcheck
kubectl apply -f python/cron/healthcheck-cronjob.yaml
```

### 3.7 Things the code assumes that are easy to miss

- Node name == VM hostname (`activities.go:164`).
- `GPU_OPERATOR_VALUES` defaults to `../k8s/gpu-operator-values.yaml`, relative to `go/` (`go/cmd/worker/main.go:39`). If you build a binary and run it from elsewhere, set it to `/home/george/gpu-provisioner/k8s/gpu-operator-values.yaml`.
- The Temporal dev server forgets everything on restart unless started with `--db-filename`.
- The Celery benchmark sshes as `root` to the node's address (`python/tasks.py:43`); it is not dry-run aware.
- A `health` signal with `passed: true` does not move a `failed` node back to `ready` (`provision.go:253-255`); recover by `scale remove` then `scale add`. This is deliberate: a human decides.
- `JoinCluster` is idempotent (`systemctl is-active k3s-agent` short-circuits the install, `activities.go:156`), so re-running a `create` after a `destroy` works.

## 4. Substitutes for cloud services

| A hosted deployment would use | This plan uses | Where |
|---|---|---|
| Temporal Cloud | Self-hosted Temporal dev server in Docker on `gpu-ctl`; add `--db-filename` + a volume, or full self-hosted Temporal with PostgreSQL, for durability across restarts | `docker-compose.yml:4-9` |
| Managed Redis (ElastiCache, Memorystore) | `redis:7-alpine` in the same compose stack | `docker-compose.yml:10-13` |
| Managed Kubernetes (EKS, GKE, AKS) | k3s: one server VM, agent VMs joined by the workflow | `k8s/install-k3s-server.sh`, `activities.go:153-166` |
| Cloud GPU instances (p4d, NC-series, A2) | Proxmox VMs with VFIO passthrough of the CMP 170HX | section 3.3 |
| Cloud-managed NVIDIA drivers / DaemonSets | Driver installed in the guest, GPU Operator with `driver.enabled=false` | `k8s/gpu-operator-values.yaml:4-5` |
| IAM / SSM Session Manager for node access | ssh keys, `root@node` | `activities.go:118-129`, `tasks.py:43` |
| Container registry (ECR, GCR, ACR) | GHCR public package, or `docker save` + `k3s ctr images import` on each node with `imagePullPolicy: IfNotPresent` | `python/cron/healthcheck-cronjob.yaml:17` |
| Scheduler (EventBridge, Cloud Scheduler) | Kubernetes CronJob at 03:00; a Temporal Schedule is listed as future work in the README | `python/cron/healthcheck-cronjob.yaml:7` |
| Load balancer / API gateway | None; Flask on `:8080` on the LAN | `Makefile:22` |
| Terraform / CloudFormation | None; VMs are created with the Proxmox UI and `qm`. "Proxmox API activities to clone VMs" is future work in the README | section 3.3 |

## 5. Test plan

Each tier is independent; the earlier ones need no hardware.

### Tier 0: unit (laptop or CI, seconds)

| # | Command (repo root) | Expected |
|---|---|---|
| 0.1 | `make test` | `ok github.com/georgejpark/gpu-provisioner/internal/workflow`; 4 tests: `TestProvisionThenTeardown`, `TestFailedNodeMakesClusterDegraded`, `TestScaleOutAddsNode`, `TestGPUOperatorFailureFailsWorkflow` |
| 0.2 | `cd go && go vet ./...` | no output |
| 0.3 | `python3 -m compileall -q python` | no output |
| 0.4 | push to `main` | the three CI jobs in `.github/workflows/ci.yml` are green |

### Tier 1: dry run (Mac or `gpu-ctl`, no cluster, 15 minutes)

| # | Command | Expected |
|---|---|---|
| 1.1 | `make up` | `docker compose ps` shows `temporal` and `redis` up; http://localhost:8233 loads |
| 1.2 | `make worker` | log line `worker started on task queue "gpu-provisioner" (dry-run=true)` |
| 1.3 | `cd go && go run ./cmd/starter create lab1 gpu-node-1=192.168.1.210:1 gpu-node-2=192.168.1.211:1` | `started cluster-lab1 run ...` |
| 1.4 | `go run ./cmd/starter status lab1` repeatedly | phase `provisioning` with node phases `preparing` → `joining` → `joined` → `validating` → `ready`; then cluster `ready`, `gpuOperatorReady: true` |
| 1.5 | `go run ./cmd/starter create lab1 gpu-node-1=192.168.1.210:1` again | rejected: workflow ID `cluster-lab1` already running (idempotency) |
| 1.6 | `go run ./cmd/starter scale lab1 add gpu-node-3=192.168.1.212:1` then `status` | `gpu-node-3` goes `pending` → `ready`; 3 nodes |
| 1.7 | `go run ./cmd/starter health lab1 gpu-node-2 false "simulated failure"` then `status` | `gpu-node-2` `failed`, cluster `degraded`, `lastHealthCheck` set |
| 1.8 | `go run ./cmd/starter scale lab1 remove gpu-node-2` then `status` | `gpu-node-2` `removed`, cluster back to `ready` |
| 1.9 | Stop the worker with Ctrl-C during 1.6's `joining`, restart `make worker` | the workflow resumes from the event history and finishes; nothing is re-run twice in the UI |
| 1.10 | `go run ./cmd/starter destroy lab1` | status becomes `tearing-down` then the workflow completes with phase `destroyed`; all nodes `removed` |
| 1.11 | `make api` + `make celery`, then the four `curl` calls from the README | `POST /clusters` returns 202 with `workflowId: cluster-lab2`; `GET` returns the status JSON; `POST .../benchmark` returns a `taskId` and the Celery log shows an ssh attempt to 192.168.1.210 that fails, followed by a `health` signal with `passed: false`, so `GET` shows `degraded`; `DELETE` returns 202 and the workflow ends `destroyed` |
| 1.12 | `curl -X POST localhost:8080/clusters` with the same `clusterId` twice | second call should be 409. `api.py:43-45` only catches `RPCError`; if you see a 500 instead, the Python SDK raised `WorkflowAlreadyStartedError` and the handler needs that exception too |
| 1.13 | `curl localhost:8080/jobs/<taskId from 1.11>` | `state: SUCCESS` and the results dict |

### Tier 2: real cluster on pve2

| # | Where | Command / action | Expected |
|---|---|---|---|
| 2.1 | pve2 | section 3.2 commands | two NVIDIA devices, separate IOMMU groups, no host driver |
| 2.2 | `gpu-node-1` | `nvidia-smi -L` | two CMP 170HX listed |
| 2.3 | `gpu-ctl` | `kubectl get nodes` | `k3s-server Ready` |
| 2.4 | `gpu-ctl` | `ssh root@192.168.1.210 nvidia-smi -L` | works non-interactively (key auth) |
| 2.5 | `gpu-ctl` | `make worker-real K3S_URL=https://192.168.1.201:6443` | `worker started ... (dry-run=false)` |
| 2.6 | `gpu-ctl` | the `starter create lab1 gpu-node-1=192.168.1.210:2` command from 3.6 | Temporal UI shows `PrepareNode`, `JoinCluster` (several minutes: k3s install + `kubectl wait`), `InstallGPUOperator` (helm `--wait`, up to 15 min), `ValidateNode` |
| 2.7 | `gpu-ctl` | `kubectl get nodes -o wide` and `kubectl get pods -n gpu-operator` | `gpu-node-1 Ready`; operator, device-plugin, toolkit, dcgm-exporter and gfd pods `Running` |
| 2.8 | `gpu-ctl` | `kubectl get node gpu-node-1 -o jsonpath='{.status.allocatable.nvidia\.com/gpu}'` | `2` |
| 2.9 | `gpu-ctl` | `go run ./cmd/starter status lab1` | cluster `ready`, node `validation.gpusFound: 2`, `ncclBusGBs` > 0 if `all_reduce_perf` is installed on the node, else 0 |
| 2.10 | `gpu-ctl` | `kubectl apply -f k8s/nccl-test-job.yaml && kubectl logs -f job/nccl-test` | nccl-tests builds and prints `Avg bus bandwidth` for 2 GPUs |
| 2.11 | `gpu-ctl` | `curl -X POST localhost:8080/clusters/lab1/benchmark` | Celery runs `gpu_burn 60` (or `nvidia-smi`) on the node; `status` shows the node's `validation.detail` updated and `passed: true` |
| 2.12 | `gpu-ctl` | `kubectl -n gpu-provisioner create job --from=cronjob/gpu-healthcheck manual-1 && kubectl -n gpu-provisioner logs job/manual-1` | `gpu-node-1: expected 2 found 2 -> ok`; `lastHealthCheck` updates in `status` |
| 2.13 | pve2 | `qm set 210 --delete hostpci1 && qm stop 210 && qm start 210`, then rerun 2.12 | `expected 2 found 1 -> FAIL`; node `failed`, cluster `degraded`. Restore with `qm set 210 --hostpci1 "$GPU2",pcie=1` and `scale remove` + `scale add` |
| 2.14 | `gpu-ctl` | Ctrl-C the worker during 2.6's `JoinCluster`, start it again | the activity is retried (up to 4 attempts, 10 s → 2 m backoff) and the workflow continues from where it was |
| 2.15 | `gpu-ctl` | `go run ./cmd/starter destroy lab1` | node drained and deleted (`kubectl get nodes` no longer lists it), `k3s-agent` uninstalled on the VM, workflow result `destroyed` |
| 2.16 | `gpu-ctl` | rerun 2.6 | a fresh `cluster-lab1` run succeeds (the join script is idempotent) |

### Tier 3: option B only (two one-GPU VMs)

Repeat 2.6 with `gpu-node-1=192.168.1.210:1 gpu-node-2=192.168.1.211:1`, then: `scale remove gpu-node-2` (node drained, VM's agent uninstalled), `scale add gpu-node-2=192.168.1.211:1` (rejoins), and power off VM 211 before a manual healthcheck run (node `failed`, cluster `degraded`, the other node still serves).
