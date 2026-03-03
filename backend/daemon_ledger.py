import asyncio
import json
import logging
from database.database import AsyncSessionLocal
from database.models import TransactionLedger, LedgerOperation
from security.redis_limiter import get_redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("neural_sync.ledger_daemon")

async def process_ledger_events():
    """
    10/10 Enterprise Immutable Persistence Synchronizer:
    Drains 'pg_ledger_events' Redis stream generated natively during credit operations 
    and writes strictly synchronously to PostgreSQL database ensuring high-durability Source of Truth SOT.
    """
    logger.info("Ledger Daemon Supervisor Online. Syncing pg_ledger_events to DB...")
    redis = await get_redis()
    
    try:
        await redis.xgroup_create("stream:pg_ledger_events", "pg_writer_group", id="0", mkstream=True)
    except Exception:
        pass # Group securely present
        
    while True:
        try:
            entries = await redis.xreadgroup(
                "pg_writer_group", "writer-1", {"stream:pg_ledger_events": ">"}, count=250, block=2000
            )
            
            if not entries:
                continue
                
            stream_name, messages = entries[0]
            
            # Using transaction commit chunk block
            async with AsyncSessionLocal() as session:
                for entry_id, message_data in messages:
                    payload_raw = message_data.get("data")
                    if not payload_raw: continue
                    
                    try:
                        data = json.loads(payload_raw)
                        event_type = data["type"]
                        tx_id = data["t"]
                        user_id = data["u"]
                        cost = data.get("c", 0)
                        
                        if event_type == "RESERVE":
                            ledger = TransactionLedger(
                                user_id=user_id, tx_id=tx_id, intent="generated", 
                                operation=LedgerOperation.RESERVE, debit_amount=cost
                            )
                            session.add(ledger)
                        elif event_type == "COMMIT":
                            ledger = TransactionLedger(
                                user_id=user_id, tx_id=tx_id, intent="generated", 
                                operation=LedgerOperation.COMMIT
                            )
                            session.add(ledger)
                        elif event_type == "ROLLBACK":
                            ledger = TransactionLedger(
                                user_id=user_id, tx_id=tx_id, intent="generated", 
                                operation=LedgerOperation.REFUND, credit_amount=cost
                            )
                            session.add(ledger)
                                
                        await session.commit() # Strictly commit atomic row modification limits
                        await redis.xack("stream:pg_ledger_events", "pg_writer_group", entry_id)
                        await redis.xdel("stream:pg_ledger_events", entry_id)
                        logger.debug(f"[LEDGER] Persistent state: {tx_id} -> {event_type}")
                        
                    except Exception as e:
                        await session.rollback()
                        logger.error(f"[LEDGER CATCH] Error syncing ledger tx_id {tx_id}: {e}")
                        
        except Exception as e:
            logger.error(f"Global Supervisor loop faulting: {e}")
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(process_ledger_events())
