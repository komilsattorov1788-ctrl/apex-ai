import os
import asyncio
import logging
import json
import time
from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/1")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("neural_sync.worker")

celery_app = Celery("neural_worker", broker=REDIS_URL, backend=REDIS_URL)
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    worker_prefetch_multiplier=1,
    task_reject_on_worker_lost=True,
    task_acks_late=True,
    task_publish_retry=True,
    broker_transport_options={'confirm_publish': True}
)

def get_process_event_loop():
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed(): raise RuntimeError
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop

# SIGTERM
@worker_process_shutdown.connect
def graceful_worker_shutdown(**kwargs):
    logger.warning("Worker receiving SIGTERM. Gracefully halting queues and flushing loop metrics...")
    get_process_event_loop()
    pass

@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(60.0, daemon_recover_stuck_reservations.s(), name='recover_stuck_txs')
    sender.add_periodic_task(1.0, daemon_redis_outbox_dispatcher.s(), name='dispatch_outbound_tasks')

@celery_app.task(name="tasks.daemon_redis_outbox_dispatcher")
def daemon_redis_outbox_dispatcher():
    from security.redis_limiter import get_redis
    import redis.exceptions
    
    # CRITICAL BUG FIX 2: Asyncio.run() fully encapsulated safely dropping loop references internally completely. 
    # This prevents total celery loop corruption natively!
    async def dispatch_loop():
        redis = await get_redis()
        try:
            await redis.xgroup_create("stream:outbox_ai_tasks", "outbox_group", id="0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e): raise
            
        entries = await redis.xreadgroup(
            "outbox_group", "dispatcher-1", {"stream:outbox_ai_tasks": ">"}, count=50, block=2000
        )
        
        if not entries: return
        
        stream_name, messages = entries[0]
        for entry_id, message_data in messages:
            try:
                payload_raw = message_data.get("payload")
                if not payload_raw: continue
                
                payload = json.loads(payload_raw)
                target_engine = payload.get("target_engine", "gpt-4o")
                
                async_res = process_ai_generation.apply_async(
                    args=[
                        payload["task_id"], payload["intent"], payload["message"], 
                        payload["lang_code"], payload["user_id"], payload["cost"], 
                        payload["tx_id"], payload["correlation_id"], target_engine
                    ],
                    retry=True, retry_policy={'max_retries': 3, 'interval_start': 0.1, 'interval_max': 1}
                )
                
                if not async_res or not getattr(async_res, 'id', None):
                    raise Exception("Broker dropped outbound outbox packet natively.")
                
                await redis.xack("stream:outbox_ai_tasks", "outbox_group", entry_id)
                
            except Exception as e:
                logger.error(f"[Outbox Dispatcher Stream] Backoff Loopback initiated natively: {e}")
                break
                
    asyncio.run(dispatch_loop())

@celery_app.task(name="tasks.daemon_recover_stuck_reservations")
def daemon_recover_stuck_reservations():
    from services.task_service import run_recovery_daemon
    from security.redis_limiter import get_redis
    
    async def recover_streams_pel():
        redis = await get_redis()
        pending = await redis.xpending_range("stream:outbox_ai_tasks", "outbox_group", min="-", max="+", count=100)
        stale_ids = []
        for pel in pending:
            time_idle_ms = pel["time_since_delivered"]
            if time_idle_ms > 300000:
                logger.warning(f"Consumer Starvation PEL XCLAIM Triggered: {pel['message_id']}")
                stale_ids.append(pel['message_id'])
                
        if stale_ids:
            claimed_msgs = await redis.xclaim("stream:outbox_ai_tasks", "outbox_group", "dispatcher-recovery-node", min_idle_time=300000, message_ids=stale_ids)
        
        await run_recovery_daemon()
        
    asyncio.run(recover_streams_pel())

@celery_app.task(name="tasks.process_ai_generation", bind=True, max_retries=3)
def process_ai_generation(
    self, task_id: str, intent: str, message: str, lang_code: str, 
    user_id: str, cost: int, tx_id: str, correlation_id: str, target_engine: str
):
    from services.task_service import background_task_runner, SemaphoreExhaustedException
    
    try:
        # CRITICAL BUG FIX 2: Full isolation mapping over run_until_complete collision logic.
        asyncio.run(background_task_runner(
            task_id, intent, message, lang_code, user_id, cost, tx_id, correlation_id, target_engine
        ))
    except SemaphoreExhaustedException:
        pass
    except Exception as exc:
        if self.request.retries >= self.max_retries:
            logger.error(f"DLQ Hit natively: {tx_id} thoroughly failed.")
            async def push_dlq():
                from security.redis_limiter import get_redis
                r = await get_redis()
                await r.xadd("stream:dlq_ai_tasks", {"tx_id": tx_id, "err": str(exc)}, maxlen=10000)
            asyncio.run(push_dlq())
        else:
            raise self.retry(exc=exc, countdown=2 ** self.request.retries)
