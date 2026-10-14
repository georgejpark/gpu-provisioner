"""Celery workers: the slow, side-effecting jobs the API should not block on.

Temporal owns the cluster's lifecycle state machine. Celery owns one-off jobs
that happen *to* a cluster (benchmarks, burn-in, report generation). The split
is deliberate: Temporal for long-lived, must-not-lose state; Celery for short
async tasks where "just retry it" is fine.
"""
import subprocess
import time

from celery import Celery

import settings
import temporal_client as tc

celery = Celery("gpu_jobs", broker=settings.REDIS_URL, backend=settings.REDIS_URL)
celery.conf.task_track_started = True


@celery.task(bind=True, max_retries=2, default_retry_delay=30)
def run_benchmark(self, cluster_id: str) -> dict:
    """Run a benchmark against every ready node and report back to Temporal."""
    st = tc.run(tc.status(cluster_id))
    results = {}
    for name, node in st.get("nodes", {}).items():
        if node.get("phase") != "ready":
            continue
        addr = node["spec"]["address"]
        results[name] = _gpu_burn(addr)
        # feed the result back into the workflow as a health signal
        tc.run(tc.signal(cluster_id, "health", {
            "node": name,
            "gpusFound": node["spec"]["gpus"],
            "ncclBusGBs": 0.0,
            "passed": results[name]["ok"],
            "detail": results[name]["detail"],
        }))
    return {"clusterId": cluster_id, "results": results, "finishedAt": time.time()}


def _gpu_burn(address: str, seconds: int = 60) -> dict:
    """Run gpu-burn over ssh if present; otherwise just check nvidia-smi."""
    cmd = ["ssh", "-o", "ConnectTimeout=10", f"root@{address}",
           f"command -v gpu_burn >/dev/null && gpu_burn {seconds} || nvidia-smi --query-gpu=name,temperature.gpu --format=csv"]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=seconds + 60)
        ok = out.returncode == 0 and "FAULTY" not in out.stdout
        return {"ok": ok, "detail": (out.stdout or out.stderr)[-500:]}
    except subprocess.TimeoutExpired:
        return {"ok": False, "detail": "benchmark timed out"}
