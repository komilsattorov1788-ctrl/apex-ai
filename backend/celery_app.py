import os
from celery import Celery

# Redis serves as both the Message Broker and the Results Backend for horizontal Celery workers
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/1")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/2")

celery_app = Celery(
    "neural_sync_tasks",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=["tasks"]
)

# Optional configurations for 100k RPS Enterprise environments
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    worker_concurrency=int(os.getenv("CELERY_CONCURRENCY", "4")),  # 4 threads/processes per pod
    worker_prefetch_multiplier=int(os.getenv("CELERY_PREFETCH", "10")), # How many tasks to reserve per worker node
    task_routes={
        "tasks.heavy_ai_generation": {"queue": "ai_compute"},
        "tasks.settle_ledger_async": {"queue": "financial_settlement"},
    }
)

if __name__ == '__main__':
    celery_app.start()
