import asyncio
import time
import sys
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import TimeoutError

# Simulate a VERY small Postgres-like pool limits on SQLite using memory (so it accepts pool configs with QueuePool depending on setup, wait, SQLite memory uses SingletonThreadPool. Let's just use file with QueuePool)
# Actually, SQLite doesn't behave exactly like Postgres for pooling, so we can mock a QueuePool explicitly.
from sqlalchemy.util.concurrency import greenlet_spawn
from sqlalchemy.pool import AsyncAdaptedQueuePool

engine = create_async_engine(
    "sqlite+aiosqlite:///file:exhaust_test?mode=memory&cache=shared&uri=true",
    poolclass=AsyncAdaptedQueuePool,
    pool_size=3,
    max_overflow=2,
    pool_timeout=1 # Timeout after 1 second if queue is full
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine, 
    class_=AsyncSession, 
    expire_on_commit=False,
)

Base = declarative_base()

async def simulate_slow_query(task_id: int):
    try:
        start_time = time.time()
        print(f"[Task {task_id}] Waiting for DB connection...")
        async with AsyncSessionLocal() as session:
            print(f"[Task {task_id}] Connection acquired! Running query...")
            # Hold the connection for 2 seconds
            await asyncio.sleep(2)
            print(f"[Task {task_id}] Done. Released after {(time.time() - start_time):.2f}s")
            return "SUCCESS"
    except TimeoutError as e:
        print(f"[Task {task_id}] POOL EXHAUSTION DETECTED (TimeoutError): {e}")
        return "EXHAUSTED"
    except Exception as e:
        print(f"[Task {task_id}] ERROR: {e}")
        return "ERROR"

async def run_simulation():
    print("--- POSTGRES CONNECTION POOL EXHAUSTION SIMULATION ---")
    print("Pool Size: 3 | Max Overflow: 2 | Total possible concurrent connections: 5")
    print("Spawning 20 concurrent requests that each hold connections for 2 seconds...")
    
    tasks = [simulate_slow_query(i) for i in range(20)]
    results = await asyncio.gather(*tasks)
    
    success = results.count("SUCCESS")
    exhausted = results.count("EXHAUSTED")
    print(f"\n--- RESULTS ---")
    print(f"Successful Queries (Queue Processed): {success}")
    print(f"Failed via Pool Exhaustion Queue Timeout: {exhausted}")

    if exhausted > 0:
        print("\n[CONCLUSION] Connection pool exhaustion SUCCESSFULLY SIMULATED and caught!")
        print("In production, requests would get HTTP 500/503 (or handled gracefully by circuit breaker),")
        print("which prevents the actual database server from crashing from too many connections!")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run_simulation())
