"""Flask API: one call to create, inspect, scale or destroy a GPU cluster.

    POST   /clusters              {"clusterId": "lab1", "nodes": [{"name","address","gpus"}]}
    GET    /clusters/<id>         -> live status from the Temporal workflow (query)
    POST   /clusters/<id>/scale   {"add": [...], "remove": ["node"]}
    DELETE /clusters/<id>         -> teardown signal
    POST   /clusters/<id>/benchmark  -> queue a Celery benchmark job
    GET    /jobs/<task_id>        -> Celery job state/result
"""
from flask import Flask, jsonify, request
from temporalio.service import RPCError

import temporal_client as tc
from tasks import celery, run_benchmark

app = Flask(__name__)


def _bad(msg, code=400):
    return jsonify({"error": msg}), code


def _valid_nodes(nodes):
    if not isinstance(nodes, list) or not nodes:
        return "nodes must be a non-empty list"
    for n in nodes:
        if not isinstance(n, dict) or not n.get("name") or not n.get("address"):
            return "each node needs name and address"
        n["gpus"] = int(n.get("gpus", 1))
    return None


@app.post("/clusters")
def create_cluster():
    body = request.get_json(silent=True) or {}
    cid, nodes = body.get("clusterId"), body.get("nodes")
    if not cid:
        return _bad("clusterId is required")
    if err := _valid_nodes(nodes):
        return _bad(err)
    try:
        run_id = tc.run(tc.start_cluster(cid, nodes))
    except RPCError as e:
        if "already" in str(e).lower():
            return _bad(f"cluster {cid} already exists", 409)
        return _bad(f"temporal error: {e}", 502)
    return jsonify({"clusterId": cid, "workflowId": tc.wf_id(cid), "runId": run_id}), 202


@app.get("/clusters/<cid>")
def get_cluster(cid):
    try:
        return jsonify(tc.run(tc.status(cid)))
    except RPCError as e:
        return _bad(f"cluster {cid} not found or not running: {e}", 404)


@app.post("/clusters/<cid>/scale")
def scale_cluster(cid):
    body = request.get_json(silent=True) or {}
    add, remove = body.get("add", []), body.get("remove", [])
    if add and (err := _valid_nodes(add)):
        return _bad(err)
    if not add and not remove:
        return _bad("provide add and/or remove")
    tc.run(tc.signal(cid, "scale", {"add": add, "remove": remove}))
    return jsonify({"ok": True}), 202


@app.delete("/clusters/<cid>")
def destroy_cluster(cid):
    tc.run(tc.signal(cid, "teardown"))
    return jsonify({"ok": True}), 202


@app.post("/clusters/<cid>/benchmark")
def benchmark_cluster(cid):
    task = run_benchmark.delay(cid)
    return jsonify({"taskId": task.id}), 202


@app.get("/jobs/<task_id>")
def job_status(task_id):
    res = celery.AsyncResult(task_id)
    return jsonify({"taskId": task_id, "state": res.state, "result": res.result if res.ready() else None})


@app.get("/health")
def health():
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
