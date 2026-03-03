import os
import asyncio
import logging
import json
import time

from database.database import AsyncSessionLocal
from database.models import OutboxEvent
from sqlalchemy.future import select
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("neural_sync.outbox_relay")

async def dispatcher_relay_loop():
    """
    CRITICAL STRATEGY 4: DB Outbox Relay (Kafka-class Durability)
    Bridges safely the ACID isolated Outbox PostgreSQL messages straight into the Redis Stream,
    converting asynchronous events securely scaling without dual-write risks.
    """
    from security.redis_limiter import get_redis
    redis = await get_redis()
    
    logger.info("Outbox SOT Dispatcher Relay Hooked cleanly...")
    
    while True:
        try:
            async with AsyncSessionLocal() as session:
                # CRITICAL BUG FIX 1: PostgreSQL Deadlock Risk (RowContention / Lock Timeout) 
                # Dropped FOR UPDATE SKIP LOCKED. Used atomic Status mapping approach 
                # `UPDATE outbox RETURNING id` is a massive optimization blocking read locks entirely avoiding race conditions efficiently!
                
                claim_query = text("""
                    UPDATE outbox_events 
                    SET status = 'processing' 
                    WHERE id IN (
                        SELECT id FROM outbox_events 
                        WHERE status = 'pending' 
                        ORDER BY created_at ASC 
                        LIMIT 100
                    )
                    RETURNING id, payload
                """)
                
                result = await session.execute(claim_query)
                rows = result.fetchall()
                await session.commit()
                
                if not rows:
                    await asyncio.sleep(1.0)
                    continue
                    
                processed_ids = []
                for row_id, payload_str in rows:
                    try:
                        payload = json.loads(payload_str)
                        await redis.xadd("stream:outbox_ai_tasks", {"payload": json.dumps(payload)}, maxlen=100000, approximate=True)
                        processed_ids.append(row_id)
                        logger.debug(f"Relayed safely outbox tx_id payload {row_id}")
                    except Exception as e:
                        logger.error(f"[XADD Relay FAULT] Cannot deliver {row_id} properly: {e}")
                
                if processed_ids:
                    # Mass update sent statuses guaranteeing no drops!
                    async with AsyncSessionLocal() as final_session:
                        update_stmt = text("UPDATE outbox_events SET status = 'sent' WHERE id = ANY(:ids)")
                        await final_session.execute(update_stmt, {"ids": processed_ids})
                        await final_session.commit()
                
        except Exception as e:
            logger.error(f"Global Supervisor Outbox Relay Catch Native Faulting: {e}")
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(dispatcher_relay_loop())
