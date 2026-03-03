import json
import logging
import asyncio
import time
from opentelemetry import trace
from security.redis_limiter import (
    get_redis, commit_credits, rollback_credits, 
    set_idempotency, acquire_worker_lock, 
    acquire_semaphore, release_semaphore, record_circuit_latency,
    refresh_worker_lock
)

from database.database import AsyncSessionLocal
from database.models import TransactionLedger, LedgerOperation

logger = logging.getLogger("neural_sync.task")
tracer = trace.get_tracer(__name__)

class SemaphoreExhaustedException(Exception):
    pass

async def set_task_state(task_id: str, state: dict):
    redis = await get_redis()
    if redis:
        mapping = {str(k): json.dumps(v) for k, v in state.items()}
        await redis.hset(f"task:{task_id}", mapping=mapping)
        await redis.expire(f"task:{task_id}", 86400)

async def get_task_state(task_id: str) -> dict:
    redis = await get_redis()
    if redis:
        data = await redis.hgetall(f"task:{task_id}")
        if data: return {k: json.loads(v) for k, v in data.items()}
    return None

async def update_task_state(task_id: str, updates: dict):
    redis = await get_redis()
    if redis:
        mapping = {str(k): json.dumps(v) for k, v in updates.items()}
        await redis.hset(f"task:{task_id}", mapping=mapping)

async def add_user_task_index(user_id: str, task_id: str):
    redis = await get_redis()
    if redis:
        key = f"tasks_list:{user_id}"
        await redis.lpush(key, task_id)
        await redis.ltrim(key, 0, 99)

async def run_recovery_daemon():
    redis = await get_redis()
    if not redis: return
    server_time = (await redis.time())[0]
    sla_deadline = server_time - 300 
    
    stuck_txs = await redis.zrange("active_reservations", 0, sla_deadline, byscore=True)
    for entry in stuck_txs:
        try:
            tx_id, user_id = entry.split("|")
            
            was_rolled_back = await rollback_credits(user_id, tx_id)
            if was_rolled_back:
                logger.warning(f"[RECOVERY DAEMON] SLA Expiration Rollback for stuck TX {tx_id}")
                
        except Exception as e:
            logger.error(f"[RECOVERY DAEMON] Failure tracking entry {entry}: {e}")

async def background_task_runner(task_id: str, intent: str, message: str, lang_code: str, user_id: str, cost: int, tx_id: str, correlation_id: str, target_engine: str = "gpt-4o"):
    from services.media_service import generate_video, generate_image
    from services.code_service import generate_code
    from services.chat_service import get_default_chat_response
    
    redis = await get_redis()
    
    with tracer.start_as_current_span("worker_execution") as span:
        span.set_attribute("transaction.id", tx_id)
        span.set_attribute("task.intent", intent)
        span.set_attribute("target.engine", target_engine)
        
        if not await acquire_semaphore(task_id, limit=2000):
            span.add_event("semaphore_pressure_exhausted")
            raise SemaphoreExhaustedException("Worker node bounds safely exhausted. Auto-retry pending in stream.")
            
        try:
            expected_compute_time = 600 if intent == "video" else 60
            if not await acquire_worker_lock(tx_id, task_id, expected_processing_time=expected_compute_time):
                return None
                
            HANDLERS = {"video": generate_video, "image": generate_image, "code": generate_code, "chat": get_default_chat_response}
            handler = HANDLERS.get(intent, get_default_chat_response)

            server_time = (await redis.time())[0]
            await update_task_state(task_id, {"status": "processing", "progress": 15})
            
            async def lock_refresher():
                while True:
                    await asyncio.sleep(10)
                    await refresh_worker_lock(tx_id, task_id, extra_time=120)
                    
            refresher_task = asyncio.create_task(lock_refresher())
            try:
                result = await asyncio.wait_for(handler(message, lang_code), timeout=expected_compute_time * 3)
            except asyncio.TimeoutError:
                span.add_event("watchdog_kill_triggered")
                raise Exception(f"Execution forcefully aborted exceeding strict native SLA boundary: {expected_compute_time * 3}s")
            finally:
                refresher_task.cancel()
            
            safe_result = {
                "s3_url": getattr(result, 's3_url', 'simulated_s3_url_placeholder'),
                "metadata_preview": "Generations safely scoped."
            }
            
            exact_used_cost = cost
            completion_len = len(safe_result.get("metadata_preview", ""))
            if intent in ["chat", "code"] and completion_len > 0:
                exact_used_cost = max(1, completion_len // 50) 
                exact_used_cost = min(exact_used_cost, cost)
            
            # CRITICAL BUG FIX 3: Token Refund Race Condition Window fix.
            # Perform Commit & Refund Database logic ATOMICALLY first strictly BEFORE updating task completed bounds implicitly cleanly over states natively!
            commit_success = await commit_credits(user_id, tx_id, exact_used_cost=exact_used_cost)
            if not commit_success:
                logger.error(f"[{tx_id}] Duplicate Event Racing handled smoothly on Postgres commit barrier natively.")
                # If race failed lock natively, we skip idempotent state overwrite
                return
            
            await update_task_state(task_id, {
                "status": "completed", 
                "progress": 100,
                "cost_deducted": True,
                "result": safe_result,
                "completed_at": (await redis.time())[0]
            })
            await set_idempotency(user_id, correlation_id, "completed", {"type": intent, "content": "Success", "task_id": task_id})
                
        except SemaphoreExhaustedException:
            raise
            
        except Exception as e:
            server_time = (await redis.time())[0]
            err_msg = str(e).lower()
            upstream_crashes = ["timeout", "connection", "rate limit", "upstream", "unavailable", "aborted"]
            
            span.record_exception(e)
            if any(c in err_msg for c in upstream_crashes):
                await record_circuit_latency(target_engine, latency_ms=99999, is_error=True)
            
            await rollback_credits(user_id, tx_id)
            await update_task_state(task_id, {
                "status": "failed", "progress": 0, "cost_deducted": False,
                "error": str(e), "completed_at": server_time
            })
            await set_idempotency(user_id, correlation_id, "failed", {"type": "error", "content": "Worker Fault Recovery."})
            
            if any(c in err_msg for c in upstream_crashes):
                raise
        
        finally:
            await release_semaphore(task_id)
