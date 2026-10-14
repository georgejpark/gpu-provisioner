"""Nightly health check, run as a Kubernetes CronJob.

For every node Kubernetes reports, check the allocatable GPU count matches the
spec, then send a `health` signal to the cluster's Temporal workflow. The
workflow marks bad nodes failed and the cluster degraded; a human or an
autoscaler then decides what to do.
"""
import json
import os
import subprocess
import sys

import temporal_client as tc

CLUSTER_ID = os.environ["CLUSTER_ID"]


def allocatable_gpus() -> dict[str, int]:
    out = subprocess.run(
        ["kubectl", "get", "nodes", "-o", "json"], capture_output=True, text=True, check=True
    ).stdout
    nodes = json.loads(out)["items"]
    return {n["metadata"]["name"]: int(n["status"].get("allocatable", {}).get("nvidia.com/gpu", 0)) for n in nodes}


def main() -> int:
    st = tc.run(tc.status(CLUSTER_ID))
    live = allocatable_gpus()
    bad = 0
    for name, node in st["nodes"].items():
        if node["phase"] in ("removed", "removing"):
            continue
        expected = node["spec"]["gpus"]
        found = live.get(name, 0)
        passed = found >= expected
        bad += 0 if passed else 1
        tc.run(tc.signal(CLUSTER_ID, "health", {
            "node": name, "gpusFound": found, "ncclBusGBs": 0.0,
            "passed": passed,
            "detail": "ok" if passed else f"expected {expected} GPUs, kubernetes sees {found}",
        }))
        print(f"{name}: expected {expected} found {found} -> {'ok' if passed else 'FAIL'}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
