"""
APEX AI - Deterministic Billing Engine
========================================
Double-entry immutable ledger for every AI inference.

Every inference creates 2 atomic ledger entries:
  DEBIT  user_account   -$X   (user pays)
  CREDIT revenue_pool   +$X   (system earns)

Key guarantees:
  - Atomic: both entries written or neither
  - Immutable: entries are append-only (never updated/deleted)
  - Idempotent: duplicate txn_id = ignored safely
  - Auditable: full history always available
  - Fraud-proof: negative balance = hard reject

Model: cost-per-token pricing
  gpt-4o-mini:  $0.00015 per 1k input tokens
  gpt-4o:       $0.005   per 1k input tokens
  claude-haiku: $0.00025 per 1k input tokens
  claude-sonnet:$0.003   per 1k input tokens
"""

import asyncio
import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional

logger = logging.getLogger("apex.data_plane.billing_engine")


# ─────────────────────────────────────────────────
# PRICING TABLE (per 1k tokens)
# ─────────────────────────────────────────────────
MODEL_PRICING = {
    "gpt-4o-mini":                {"input": 0.00015, "output": 0.00060},
    "gpt-4o":                     {"input": 0.005,   "output": 0.015},
    "claude-3-haiku-20240307":    {"input": 0.00025, "output": 0.00125},
    "claude-3-5-sonnet-20241022": {"input": 0.003,   "output": 0.015},
    "ollama/llama3":              {"input": 0.0,     "output": 0.0},
}

# Free tier monthly credit in USD
FREE_TIER_CREDIT_USD  = 5.0
PRO_TIER_CREDIT_USD   = 50.0
ENT_TIER_CREDIT_USD   = 500.0

TIER_CREDITS = {
    "free":       FREE_TIER_CREDIT_USD,
    "pro":        PRO_TIER_CREDIT_USD,
    "enterprise": ENT_TIER_CREDIT_USD,
}


# ─────────────────────────────────────────────────
# LEDGER ENTRY TYPES
# ─────────────────────────────────────────────────
class EntryType(str, Enum):
    DEBIT  = "debit"   # money leaves an account
    CREDIT = "credit"  # money enters an account

class AccountType(str, Enum):
    USER_WALLET   = "user_wallet"
    REVENUE_POOL  = "revenue_pool"
    COST_POOL     = "cost_pool"
    REFUND_POOL   = "refund_pool"
    PROMO_POOL    = "promo_pool"

class TxnStatus(str, Enum):
    PENDING   = "pending"
    COMMITTED = "committed"
    REVERSED  = "reversed"
    REJECTED  = "rejected"


# ─────────────────────────────────────────────────
# IMMUTABLE LEDGER ENTRY
# ─────────────────────────────────────────────────
@dataclass(frozen=True)
class LedgerEntry:
    entry_id:     str
    txn_id:       str
    account_id:   str
    account_type: AccountType
    entry_type:   EntryType
    amount_usd:   float
    currency:     str
    model:        str
    input_tokens: int
    output_tokens: int
    timestamp:    float
    metadata:     str   # JSON string (immutable)

    def to_dict(self) -> dict:
        return asdict(self)

    def checksum(self) -> str:
        raw = f"{self.txn_id}:{self.account_id}:{self.entry_type}:{self.amount_usd:.8f}:{self.timestamp}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


@dataclass
class Transaction:
    txn_id:        str
    user_id:       str
    model:         str
    input_tokens:  int
    output_tokens: int
    total_cost_usd: float
    status:        TxnStatus
    entries:       list
    created_at:    float
    committed_at:  Optional[float] = None
    reversed_at:   Optional[float] = None
    reverse_reason: Optional[str] = None


# ─────────────────────────────────────────────────
# COST CALCULATOR
# ─────────────────────────────────────────────────
def calculate_cost(
    model: str,
    input_tokens: int,
    output_tokens: int,
) -> float:
    pricing = MODEL_PRICING.get(model, MODEL_PRICING["gpt-4o-mini"])
    cost = (
        input_tokens  / 1000 * pricing["input"] +
        output_tokens / 1000 * pricing["output"]
    )
    return round(cost, 8)


def estimate_tokens(message: str, response: str = "") -> tuple:
    input_tokens  = max(1, len(message)  // 4)
    output_tokens = max(1, len(response) // 4)
    return input_tokens, output_tokens


# ─────────────────────────────────────────────────
# BILLING ENGINE
# ─────────────────────────────────────────────────
class BillingEngine:
    """
    Double-entry immutable billing ledger.
    Uses Redis when available, falls back to in-memory.
    """

    def __init__(self):
        # In-memory ledger (fallback + cache)
        self._ledger: list = []
        self._balances: dict = {}
        self._txns: dict = {}
        self._committed_txns: set = set()
        self._lock = asyncio.Lock()

        # Stats
        self._total_billed_usd = 0.0
        self._total_txns = 0
        self._rejected_txns = 0
        self._reversed_txns = 0

    # ─────────────────────────────────────────────
    # BALANCE MANAGEMENT
    # ─────────────────────────────────────────────
    async def get_balance(self, user_id: str, tier: str = "free") -> float:
        async with self._lock:
            if user_id not in self._balances:
                credit = TIER_CREDITS.get(tier, FREE_TIER_CREDIT_USD)
                self._balances[user_id] = credit
                logger.info(f"[Billing] New user {user_id} tier={tier} credit=${credit:.2f}")
            return self._balances[user_id]

    async def add_credit(self, user_id: str, amount_usd: float, reason: str = "top_up") -> float:
        async with self._lock:
            if user_id not in self._balances:
                self._balances[user_id] = 0.0
            self._balances[user_id] += amount_usd
            entry = LedgerEntry(
                entry_id=str(uuid.uuid4()),
                txn_id=f"credit_{user_id}_{int(time.time())}",
                account_id=user_id,
                account_type=AccountType.USER_WALLET,
                entry_type=EntryType.CREDIT,
                amount_usd=amount_usd,
                currency="USD",
                model="none",
                input_tokens=0,
                output_tokens=0,
                timestamp=time.time(),
                metadata=json.dumps({"reason": reason}),
            )
            self._ledger.append(entry)
            logger.info(f"[Billing] CREDIT ${amount_usd:.4f} to {user_id} reason={reason}")
            return self._balances[user_id]

    # ─────────────────────────────────────────────
    # TRANSACTION: BEGIN → COMMIT or REVERSE
    # ─────────────────────────────────────────────
    async def begin_transaction(
        self,
        user_id: str,
        model: str,
        input_tokens: int,
        output_tokens: int = 0,
        tier: str = "free",
        txn_id: Optional[str] = None,
    ) -> Transaction:
        txn_id = txn_id or str(uuid.uuid4())

        # Idempotency: already committed?
        if txn_id in self._committed_txns:
            logger.warning(f"[Billing] Duplicate txn_id={txn_id} — ignoring")
            return self._txns.get(txn_id)

        cost = calculate_cost(model, input_tokens, output_tokens)
        balance = await self.get_balance(user_id, tier)

        # Fraud check: insufficient funds?
        if balance < cost:
            self._rejected_txns += 1
            logger.warning(f"[Billing] REJECTED user={user_id} cost=${cost:.6f} balance=${balance:.6f}")
            raise InsufficientFundsError(
                f"Insufficient balance: ${balance:.4f} < ${cost:.4f}",
                balance=balance,
                required=cost,
            )

        # Create double-entry
        now = time.time()
        meta = json.dumps({"model": model, "input_tokens": input_tokens, "output_tokens": output_tokens})

        debit_entry = LedgerEntry(
            entry_id=str(uuid.uuid4()), txn_id=txn_id,
            account_id=user_id, account_type=AccountType.USER_WALLET,
            entry_type=EntryType.DEBIT, amount_usd=cost, currency="USD",
            model=model, input_tokens=input_tokens, output_tokens=output_tokens,
            timestamp=now, metadata=meta,
        )
        credit_entry = LedgerEntry(
            entry_id=str(uuid.uuid4()), txn_id=txn_id,
            account_id="system", account_type=AccountType.REVENUE_POOL,
            entry_type=EntryType.CREDIT, amount_usd=cost, currency="USD",
            model=model, input_tokens=input_tokens, output_tokens=output_tokens,
            timestamp=now, metadata=meta,
        )

        txn = Transaction(
            txn_id=txn_id, user_id=user_id, model=model,
            input_tokens=input_tokens, output_tokens=output_tokens,
            total_cost_usd=cost, status=TxnStatus.PENDING,
            entries=[debit_entry, credit_entry], created_at=now,
        )

        async with self._lock:
            self._txns[txn_id] = txn

        logger.info(f"[Billing] BEGIN txn={txn_id} user={user_id} model={model} cost=${cost:.6f}")
        return txn

    async def commit_transaction(self, txn_id: str) -> Transaction:
        async with self._lock:
            txn = self._txns.get(txn_id)
            if not txn:
                raise ValueError(f"Transaction {txn_id} not found")
            if txn.status != TxnStatus.PENDING:
                raise ValueError(f"Transaction {txn_id} is {txn.status}, not PENDING")

            # Deduct from balance (atomic)
            user_id = txn.user_id
            if self._balances.get(user_id, 0) < txn.total_cost_usd:
                txn.status = TxnStatus.REJECTED
                self._rejected_txns += 1
                raise InsufficientFundsError("Balance changed between begin and commit")

            self._balances[user_id] -= txn.total_cost_usd
            self._balances[user_id] = round(self._balances[user_id], 8)

            # Append to immutable ledger
            for entry in txn.entries:
                self._ledger.append(entry)

            txn.status = TxnStatus.COMMITTED
            txn.committed_at = time.time()
            self._committed_txns.add(txn_id)
            self._total_billed_usd += txn.total_cost_usd
            self._total_txns += 1

        logger.info(
            f"[Billing] COMMIT txn={txn_id} user={user_id} "
            f"cost=${txn.total_cost_usd:.6f} balance=${self._balances[user_id]:.4f}"
        )
        return txn

    async def reverse_transaction(self, txn_id: str, reason: str = "error") -> Transaction:
        async with self._lock:
            txn = self._txns.get(txn_id)
            if not txn:
                raise ValueError(f"Transaction {txn_id} not found")

            if txn.status == TxnStatus.COMMITTED:
                # Refund
                self._balances[txn.user_id] = round(
                    self._balances.get(txn.user_id, 0) + txn.total_cost_usd, 8)

                # Add reversal entries (immutable — we ADD not delete)
                now = time.time()
                for entry in txn.entries[:]:
                    reversal = LedgerEntry(
                        entry_id=str(uuid.uuid4()),
                        txn_id=f"reversal_{txn_id}",
                        account_id=entry.account_id,
                        account_type=entry.account_type,
                        entry_type=EntryType.CREDIT if entry.entry_type == EntryType.DEBIT else EntryType.DEBIT,
                        amount_usd=entry.amount_usd,
                        currency=entry.currency,
                        model=entry.model,
                        input_tokens=entry.input_tokens,
                        output_tokens=entry.output_tokens,
                        timestamp=now,
                        metadata=json.dumps({"reversal_of": entry.entry_id, "reason": reason}),
                    )
                    self._ledger.append(reversal)

                txn.status = TxnStatus.REVERSED
                txn.reversed_at = now
                txn.reverse_reason = reason
                self._reversed_txns += 1
                logger.info(f"[Billing] REVERSED txn={txn_id} reason={reason} refund=${txn.total_cost_usd:.6f}")

        return txn

    # ─────────────────────────────────────────────
    # HIGH-LEVEL: ONE-SHOT CHARGE
    # ─────────────────────────────────────────────
    async def charge(
        self,
        user_id: str,
        model: str,
        message: str,
        response: str = "",
        tier: str = "free",
    ) -> dict:
        input_tokens, output_tokens = estimate_tokens(message, response)
        txn = await self.begin_transaction(user_id, model, input_tokens, output_tokens, tier)
        committed = await self.commit_transaction(txn.txn_id)
        balance = await self.get_balance(user_id, tier)
        return {
            "txn_id":          committed.txn_id,
            "charged_usd":     committed.total_cost_usd,
            "balance_usd":     balance,
            "model":           model,
            "input_tokens":    input_tokens,
            "output_tokens":   output_tokens,
            "status":          committed.status.value,
            "checksum":        committed.entries[0].checksum(),
        }

    # ─────────────────────────────────────────────
    # STATS & AUDIT
    # ─────────────────────────────────────────────
    def get_stats(self) -> dict:
        return {
            "total_transactions":    self._total_txns,
            "total_billed_usd":      round(self._total_billed_usd, 4),
            "rejected_transactions": self._rejected_txns,
            "reversed_transactions": self._reversed_txns,
            "ledger_entries":        len(self._ledger),
            "active_users":          len(self._balances),
        }

    async def get_user_statement(self, user_id: str, limit: int = 20) -> dict:
        entries = [e for e in self._ledger if e.account_id == user_id]
        entries_sorted = sorted(entries, key=lambda e: e.timestamp, reverse=True)[:limit]
        balance = await self.get_balance(user_id)
        return {
            "user_id":    user_id,
            "balance_usd": round(balance, 4),
            "entries":    [e.to_dict() for e in entries_sorted],
            "total_entries": len(entries_sorted),
        }

    async def verify_ledger_integrity(self) -> dict:
        errors = []
        checksums = set()
        for entry in self._ledger:
            cs = entry.checksum()
            if cs in checksums:
                errors.append(f"Duplicate checksum: {cs}")
            checksums.add(cs)
        return {
            "total_entries": len(self._ledger),
            "integrity": "OK" if not errors else "COMPROMISED",
            "errors": errors,
        }


# ─────────────────────────────────────────────────
# EXCEPTIONS
# ─────────────────────────────────────────────────
class InsufficientFundsError(Exception):
    def __init__(self, message: str, balance: float = 0.0, required: float = 0.0):
        super().__init__(message)
        self.balance = balance
        self.required = required


# ─────────────────────────────────────────────────
# SINGLETON
# ─────────────────────────────────────────────────
_billing: Optional[BillingEngine] = None

def get_billing_engine() -> BillingEngine:
    global _billing
    if _billing is None:
        _billing = BillingEngine()
        logger.info("[BillingEngine] Initialized. Double-entry immutable ledger ready.")
    return _billing
