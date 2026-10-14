"""Thin wrapper around the Temporal Python client so Flask and Celery share it.

The workflow itself is written in Go. Python only starts it, signals it and
queries it. Temporal serializes arguments as JSON, so the dict shapes here must
match the Go structs (json tags) in internal/workflow/provision.go.
"""
import asyncio
from temporalio.client import Client

import settings

_client: Client | None = None


async def _get() -> Client:
    global _client
    if _client is None:
        _client = await Client.connect(settings.TEMPORAL_ADDRESS)
    return _client


def run(coro):
    """Run an async Temporal call from sync Flask/Celery code."""
    return asyncio.run(coro)


def wf_id(cluster_id: str) -> str:
    return f"cluster-{cluster_id}"


async def start_cluster(cluster_id: str, nodes: list[dict]) -> str:
    client = await _get()
    req = {"clusterId": cluster_id, "joinToken": settings.K3S_TOKEN, "nodes": nodes}
    handle = await client.start_workflow(
        settings.WORKFLOW_NAME, req, id=wf_id(cluster_id), task_queue=settings.TASK_QUEUE
    )
    return handle.result_run_id or ""


async def status(cluster_id: str) -> dict:
    client = await _get()
    handle = client.get_workflow_handle(wf_id(cluster_id))
    return await handle.query("status")


async def signal(cluster_id: str, name: str, payload=None) -> None:
    client = await _get()
    handle = client.get_workflow_handle(wf_id(cluster_id))
    if payload is None:
        await handle.signal(name)
    else:
        await handle.signal(name, payload)
