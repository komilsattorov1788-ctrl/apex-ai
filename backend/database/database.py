import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

# PostgreSQL connection strings
# For 100k RPS, write operations go to MASTER, read operations to REPLICA(S).
# For PgBouncer in transaction pooling mode, prepared statements MUST be disabled.
# This means appending `?prepared_statement_cache_size=0` to the asyncpg connection string if interacting with PgBouncer.
DATABASE_URL_MASTER = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./ledger.db")
DATABASE_URL_REPLICA = os.getenv("REPLICA_DATABASE_URL", DATABASE_URL_MASTER)

def create_engine_for_url(url: str):
    engine_kwargs = {"echo": False}
    
    if "sqlite" not in url:
        # PgBouncer High-Throughput / K8s HPA Configuration
        # In a distributed environment with PgBouncer, the *app* connection pool size should be smaller,
        # but PgBouncer's server pool handles the mass scale.
        engine_kwargs.update({
            "pool_size": int(os.getenv("DB_POOL_SIZE", "15")), # Lower per pod, K8s HPA handles total scale
            "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", "5")),
            "pool_pre_ping": True, 
            "pool_timeout": 30,
            "pool_recycle": 1800 # Recycle connections every 30 mins to avoid dropped connections by firewall/load balancer
        })
    return create_async_engine(url, **engine_kwargs)

master_engine = create_engine_for_url(DATABASE_URL_MASTER)
replica_engine = create_engine_for_url(DATABASE_URL_REPLICA)

# Async Session Makers for Master (Write) and Replica (Read)
AsyncSessionLocalWrite = async_sessionmaker(
    bind=master_engine, class_=AsyncSession, expire_on_commit=False, autocommit=False, autoflush=False
)

AsyncSessionLocalRead = async_sessionmaker(
    bind=replica_engine, class_=AsyncSession, expire_on_commit=False, autocommit=False, autoflush=False
)

Base = declarative_base()

# Dependency Injectors for FastAPI
async def get_db_write():
    """Dependency for DB Write Operations (Master Node)"""
    async with AsyncSessionLocalWrite() as session:
        yield session

async def get_db_read():
    """Dependency for DB Read Operations (Replica Nodes). Follows CQRS mental model."""
    async with AsyncSessionLocalRead() as session:
        yield session

# Fallback compatible for existing code
get_db = get_db_write
AsyncSessionLocal = AsyncSessionLocalWrite
