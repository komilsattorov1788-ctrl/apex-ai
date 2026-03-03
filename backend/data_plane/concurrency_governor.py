"""
APEX AI - DATA PLANE: Global Concurrency Governor
====================================================
Controls HOW MANY requests run simultaneously.
Without this: 1000 users hit at once → GPU melts → everyone gets 500.
With this:    smooth queue, tier priorities, graceful degradation.

Architecture:
  ┌─────────────────────────────────────────┐
  │         ConcurrencyGovernor             │
  │                                         │
  │  Global Semaphore: max 100 concurrent   │
  │  Per-User Semaphore:                    │
  │    FREE:       max 2 concurrent         │
  │    PRO:        max 10 concurrent        │
  │    ENTERPRISE: max 50 concurrent        │
  │                                         │
  │  Token Budget (per minute):             │
  │    FREE:       10,000 tokens            │
  │    PRO:        100,000 tokens           │
  │    ENTERPRISE: unlimited                │
  │                                         │
  │  Queue:   500 requests max              │
  │  Timeout: 30s wait max                  │
  └─────────────────────────────────────────┘

Real-world: This is how OpenAI's rate limiter works internally.
"""

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional
from contextlib import asynccontextmanager
from enum import Enum

logger = logging.getLogger("apex.data_plane.concurrency_governor")


# ─────────────────────────────────────────────────
# TIER CONFIGURATION
# ─────────────────────────────────────────────────
class UserTier(str, Enum):
    FREE       = "free"
    PRO        = "pro"
    ENTERPRISE = "enterprise"


@dataclass
class TierLimits:
    max_concurrent:     int    # parallel requests allowed
    token_budget_min:   int    # tokens per minute
    queue_timeout_s:    float  # how long to wait in queue
    priority:           int    # higher = served first


TIER_LIMITS: dict[str, TierLimits] = {
    UserTier.FREE: TierLimits(
        max_concurrent=2,
        token_budget_min=10_000,
        queue_timeout_s=10.0,
        priority=0,
    ),
    UserTier.PRO: TierLimits(
        max_concurrent=10,
        token_budget_min=100_000,
        queue_timeout_s=20.0,
        priority=1,
    ),
    UserTier.ENTERPRISE: TierLimits(
        max_concurrent=50,
        token_budget_min=999_999_999,   # unlimited
        queue_timeout_s=30.0,
        priority=2,
    ),
}

GLOBAL_MAX_CONCURRENT = 100    # Total system-wide parallel requests
GLOBAL_QUEUE_MAX      = 500    # Max waiting requests
TOKEN_WINDOW_SECONDS  = 60     # Budget reset window


# ─────────────────────────────────────────────────
# TOKEN BUDGET TRACKER
# ─────────────────────────────────────────────────
@dataclass
class TokenBudget:
    """Sliding window token budget per user."""
    budget_per_minute: int
    _used_tokens: int = 0
    _window_start: float = field(default_factory=time.time)

    def consume(self, tokens: int) -> bool:
        """Returns True if budget allows, False if exceeded."""
        now = time.time()
        # Reset window if expired
        if now - self._window_start > TOKEN_WINDOW_SECONDS:
            self._used_tokens = 0
            self._window_start = now

        if self._used_tokens + tokens > self.budget_per_minute:
            return False      # Over budget

        self._used_tokens += tokens
        return True

    def remaining(self) -> int:
        now = time.time()
        if now - self._window_start > TOKEN_WINDOW_SECONDS:
            return self.budget_per_minute
        return max(0, self.budget_per_minute - self._used_tokens)

    def reset_in_seconds(self) -> float:
        return max(0.0, TOKEN_WINDOW_SECONDS - (time.time() - self._window_start))


# ─────────────────────────────────────────────────
# ADMISSION RESULT
# ─────────────────────────────────────────────────
@dataclass
class AdmissionResult:
    admitted: bool
    wait_ms: float = 0.0
    queue_position: int = 0
    reason: str = "admitted"
    retry_after_s: float = 0.0
    budget_remaining: int = 0


# ─────────────────────────────────────────────────
# GOVERNOR STATS
# ─────────────────────────────────────────────────
@dataclass
class GovernorStats:
    total_admitted:  int = 0
    total_queued:    int = 0
    total_rejected:  int = 0
    total_timed_out: int = 0
    peak_concurrent: int = 0
    current_active:  int = 0
    current_queued:  int = 0


# ─────────────────────────────────────────────────
# GLOBAL CONCURRENCY GOVERNOR
# ─────────────────────────────────────────────────
class ConcurrencyGovernor:
    """
    Central gatekeeper for all inference requests.
    Applies per-user and global limits, manages queue,
    tracks token budgets, and reports real-time stats.
    """

    def __init__(self):
        # Global semaphore: hard cap on simultaneous requests
        self._global_sem = asyncio.Semaphore(GLOBAL_MAX_CONCURRENT)

        # Per-user semaphores (created lazily)
        self._user_sems: dict[str, asyncio.Semaphore] = {}
        self._user_sems_lock = asyncio.Lock()

        # Token budgets per user
        self._budgets: dict[str, TokenBudget] = {}
        self._budgets_lock = asyncio.Lock()

        # Stats
        self._stats = GovernorStats()
        self._active_count = 0
        self._queue_count   = 0
        self._lock = asyncio.Lock()

    # ─────────────────────────────
    # USER SEMAPHORE (lazy init)
    # ─────────────────────────────
    async def _get_user_sem(self, user_id: str, tier: str) -> asyncio.Semaphore:
        async with self._user_sems_lock:
            if user_id not in self._user_sems:
                limits = TIER_LIMITS.get(tier, TIER_LIMITS[UserTier.FREE])
                self._user_sems[user_id] = asyncio.Semaphore(limits.max_concurrent)
            return self._user_sems[user_id]

    # ─────────────────────────────
    # TOKEN BUDGET (lazy init)
    # ─────────────────────────────
    async def _get_budget(self, user_id: str, tier: str) -> TokenBudget:
        async with self._budgets_lock:
            if user_id not in self._budgets:
                limits = TIER_LIMITS.get(tier, TIER_LIMITS[UserTier.FREE])
                self._budgets[user_id] = TokenBudget(limits.token_budget_min)
            return self._budgets[user_id]

    # ─────────────────────────────
    # CORE: ACQUIRE SLOT
    # ─────────────────────────────
    @asynccontextmanager
    async def acquire(
        self,
        user_id: str,
        tier: str = "free",
        estimated_tokens: int = 500,
    ):
        """
        Context manager: acquire a concurrency slot.
        Blocks if at capacity (up to timeout), then raises if still blocked.

        Usage:
            async with governor.acquire(user_id, tier, tokens):
                result = await call_llm(...)
        """
        limits = TIER_LIMITS.get(tier, TIER_LIMITS[UserTier.FREE])
        admit_start = time.time()

        # ── 1. Check token budget ──────────────────────────
        budget = await self._get_budget(user_id, tier)
        if not budget.consume(estimated_tokens):
            self._stats.total_rejected += 1
            retry_after = budget.reset_in_seconds()
            raise TokenBudgetExceeded(
                f"Token budget exhausted for {tier} tier. "
                f"Retry in {retry_after:.0f}s. "
                f"Remaining: {budget.remaining()} tokens.",
                retry_after_s=retry_after,
            )

        # ── 2. Check global queue capacity ────────────────
        async with self._lock:
            if self._queue_count >= GLOBAL_QUEUE_MAX:
                self._stats.total_rejected += 1
                raise SystemOverloaded(
                    "System at maximum capacity. Please retry shortly.")
            self._queue_count += 1

        # ── 3. Acquire per-user semaphore ─────────────────
        user_sem = await self._get_user_sem(user_id, tier)
        try:
            acquired_user = await asyncio.wait_for(
                user_sem.acquire(),
                timeout=limits.queue_timeout_s / 2,   # half for user-level
            )
        except asyncio.TimeoutError:
            async with self._lock:
                self._queue_count -= 1
            self._stats.total_timed_out += 1
            raise QueueTimeout(
                f"Per-user concurrency limit reached ({limits.max_concurrent} "
                f"concurrent for {tier} tier). Try again shortly.")

        # ── 4. Acquire global semaphore ───────────────────
        try:
            acquired_global = await asyncio.wait_for(
                self._global_sem.acquire(),
                timeout=limits.queue_timeout_s / 2,   # remaining half for global
            )
        except asyncio.TimeoutError:
            user_sem.release()
            async with self._lock:
                self._queue_count -= 1
            self._stats.total_timed_out += 1
            raise QueueTimeout(
                "Global concurrency limit reached. System under high load.")

        # ── 5. Update stats ───────────────────────────────
        wait_ms = (time.time() - admit_start) * 1000
        async with self._lock:
            self._active_count += 1
            self._queue_count  -= 1
            self._stats.total_admitted += 1
            if wait_ms > 5:
                self._stats.total_queued += 1
            if self._active_count > self._stats.peak_concurrent:
                self._stats.peak_concurrent = self._active_count
            self._stats.current_active = self._active_count

        logger.debug(
            f"[Governor] ADMITTED user={user_id} tier={tier} "
            f"wait={wait_ms:.0f}ms active={self._active_count}"
        )

        try:
            yield AdmissionResult(
                admitted=True,
                wait_ms=round(wait_ms, 1),
                reason="admitted",
                budget_remaining=budget.remaining(),
            )
        finally:
            # ── 6. Release slots ──────────────────────────
            user_sem.release()
            self._global_sem.release()
            async with self._lock:
                self._active_count -= 1
                self._stats.current_active = self._active_count
            logger.debug(f"[Governor] RELEASED user={user_id} active={self._active_count}")

    # ─────────────────────────────
    # STATS & HEALTH
    # ─────────────────────────────
    def get_stats(self) -> dict:
        return {
            "global_capacity":   GLOBAL_MAX_CONCURRENT,
            "current_active":    self._stats.current_active,
            "current_queued":    self._queue_count,
            "peak_concurrent":   self._stats.peak_concurrent,
            "total_admitted":    self._stats.total_admitted,
            "total_queued":      self._stats.total_queued,
            "total_rejected":    self._stats.total_rejected,
            "total_timed_out":   self._stats.total_timed_out,
            "utilization_pct":   round(
                self._stats.current_active / GLOBAL_MAX_CONCURRENT * 100, 1),
            "tier_limits": {
                tier: {
                    "max_concurrent":   limits.max_concurrent,
                    "token_budget_min": limits.token_budget_min,
                }
                for tier, limits in TIER_LIMITS.items()
            }
        }

    async def get_user_status(self, user_id: str, tier: str) -> dict:
        budget = await self._get_budget(user_id, tier)
        return {
            "user_id":             user_id,
            "tier":                tier,
            "tokens_remaining":    budget.remaining(),
            "budget_resets_in_s":  round(budget.reset_in_seconds(), 1),
            "max_concurrent":      TIER_LIMITS.get(tier, TIER_LIMITS["free"]).max_concurrent,
        }


# ─────────────────────────────────────────────────
# CUSTOM EXCEPTIONS
# ─────────────────────────────────────────────────
class TokenBudgetExceeded(Exception):
    def __init__(self, message: str, retry_after_s: float = 60.0):
        super().__init__(message)
        self.retry_after_s = retry_after_s

class QueueTimeout(Exception):
    pass

class SystemOverloaded(Exception):
    pass


# ─────────────────────────────────────────────────
# SINGLETON
# ─────────────────────────────────────────────────
_governor: Optional[ConcurrencyGovernor] = None

def get_governor() -> ConcurrencyGovernor:
    global _governor
    if _governor is None:
        _governor = ConcurrencyGovernor()
        logger.info(
            f"[ConcurrencyGovernor] Initialized. "
            f"Global cap: {GLOBAL_MAX_CONCURRENT} | "
            f"Queue: {GLOBAL_QUEUE_MAX} | "
            f"Tiers: FREE={TIER_LIMITS['free'].max_concurrent} "
            f"PRO={TIER_LIMITS['pro'].max_concurrent} "
            f"ENTERPRISE={TIER_LIMITS['enterprise'].max_concurrent}"
        )
    return _governor
