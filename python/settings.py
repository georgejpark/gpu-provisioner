import os

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
TASK_QUEUE = "gpu-provisioner"              # must match workflow.TaskQueue in Go
WORKFLOW_NAME = "ProvisionCluster"          # Go function name = workflow type
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
K3S_TOKEN = os.getenv("K3S_TOKEN", "")
