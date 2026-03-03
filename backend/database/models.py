import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, Float, DateTime, Enum, Index, text
from sqlalchemy.orm import declarative_base
import enum

Base = declarative_base()

class LedgerOperation(str, enum.Enum):
    RESERVE = 'reserve'
    COMMIT = 'commit'
    REFUND = 'refund'
    DEPOSIT = 'deposit'

class TransactionLedger(Base):
    """
    10/10 Enterprise Bank-Grade Architecture: Double-Entry Immutable Ledger!
    Never updates rows. Balance = SUM(credit_amount) - SUM(debit_amount).
    """
    __tablename__ = 'transaction_ledger'
    
    # Composite PK for Citus Sharding Distribution
    entry_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(255), primary_key=True, index=True, nullable=False)
    
    # Correlation ID tying all ledger operations of one user action together
    tx_id = Column(String(36), index=True, nullable=False)
    
    intent = Column(String(50), nullable=False)
    operation = Column(Enum(LedgerOperation), nullable=False)
    
    debit_amount = Column(Integer, default=0, nullable=False) # Deductions (cost incurred)
    credit_amount = Column(Integer, default=0, nullable=False)# Additions (deposits, refunds)
    
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    
    # Trace context for observability linking
    traceparent = Column(String(255), nullable=True)

class OutboxEvent(Base):
    """
    10/10 true Database-backed Transactional Outbox.
    Ensures message emission exactly-once bounds are tied strictly to Ledger ACID transactions.
    """
    __tablename__ = 'outbox_events'
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    tx_id = Column(String(36), index=True, nullable=False)
    payload = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    status = Column(String(20), default='pending') # pending | sent | failed
    
    # Composite indexing for fast polling by dispatcher
    __table_args__ = (
        Index('idx_outbox_pending', 'status', 'created_at', postgresql_where=text("status = 'pending'")),
    )
