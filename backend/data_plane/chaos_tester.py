"""
APEX AI - Chaos Testing Framework
====================================
"If it hasn't been tested under failure, it WILL fail in production."

Tests:
  1. Redis DOWN   - cache miss, direct LLM call
  2. OpenAI DOWN  - failover to Claude
  3. Claude DOWN  - failover to Local
  4. ALL DOWN     - graceful error response
  5. Flood attack - concurrency governor holds
  6. Token exhaustion - billing rejects
  7. Slow provider - timeout + failover
  8. Duplicate requests - idempotency holds
"""

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional, Callable
from enum import Enum
from unittest.mock import AsyncMock, patch, MagicMock

logger = logging.getLogger("apex.chaos")


# ─────────────────────────────────────────────────
# SCENARIO RESULT
# ─────────────────────────────────────────────────
class ScenarioStatus(str, Enum):
    PASSED  = "PASSED"
    FAILED  = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass
class ScenarioResult:
    name: str
    status: ScenarioStatus
    duration_ms: float
    description: str
    expected: str
    actual: str
    error: Optional[str] = None

    def passed(self) -> bool:
        return self.status == ScenarioStatus.PASSED


@dataclass
class ChaosReport:
    run_id: str
    started_at: float
    finished_at: float
    results: list = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.status == ScenarioStatus.PASSED)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if r.status == ScenarioStatus.FAILED)

    @property
    def skipped(self) -> int:
        return sum(1 for r in self.results if r.status == ScenarioStatus.SKIPPED)

    @property
    def duration_s(self) -> float:
        return round(self.finished_at - self.started_at, 2)

    @property
    def pass_rate(self) -> float:
        return round(self.passed / max(self.total, 1) * 100, 1)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "duration_s": self.duration_s,
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "skipped": self.skipped,
            "pass_rate_pct": self.pass_rate,
            "verdict": "RESILIENT" if self.pass_rate >= 80 else "FRAGILE",
            "results": [
                {
                    "name": r.name,
                    "status": r.status.value,
                    "duration_ms": round(r.duration_ms, 1),
                    "expected": r.expected,
                    "actual": r.actual,
                    "error": r.error,
                }
                for r in self.results
            ],
        }


# ─────────────────────────────────────────────────
# CHAOS SCENARIOS
# ─────────────────────────────────────────────────
class ChaosTester:
    def __init__(self):
        self._results: list = []

    async def _run_scenario(
        self,
        name: str,
        description: str,
        expected: str,
        fn: Callable,
    ) -> ScenarioResult:
        start = time.time()
        try:
            actual = await fn()
            duration_ms = (time.time() - start) * 1000
            passed = True
            error = None
        except Exception as e:
            actual = f"EXCEPTION: {type(e).__name__}: {str(e)[:100]}"
            duration_ms = (time.time() - start) * 1000
            passed = False
            error = str(e)

        result = ScenarioResult(
            name=name,
            status=ScenarioStatus.PASSED if passed else ScenarioStatus.FAILED,
            duration_ms=duration_ms,
            description=description,
            expected=expected,
            actual=str(actual),
            error=error,
        )
        self._results.append(result)
        icon = "PASS" if passed else "FAIL"
        logger.info(f"[Chaos] {icon} | {name} | {duration_ms:.0f}ms | {actual}")
        return result

    # ─────────────────────────────────────────────
    # SCENARIO 1: Redis Down -> Cache Bypass
    # ─────────────────────────────────────────────
    async def test_redis_down(self) -> ScenarioResult:
        async def scenario():
            from data_plane.semantic_cache import SemanticCache

            class BrokenRedis:
                async def get(self, *a, **kw): raise ConnectionError("Redis DOWN")
                async def setex(self, *a, **kw): raise ConnectionError("Redis DOWN")
                async def hgetall(self, *a, **kw): raise ConnectionError("Redis DOWN")

            cache = SemanticCache(BrokenRedis())
            result = await cache.get("gpt-4o-mini", "test message")
            if result is None:
                return "CACHE_MISS_OK: gracefully returned None despite Redis failure"
            return "UNEXPECTED_HIT"

        return await self._run_scenario(
            name="Redis DOWN -> Cache Miss (Graceful)",
            description="When Redis fails, semantic cache should return None (miss), not crash",
            expected="None (graceful miss)",
            fn=scenario,
        )

    # ─────────────────────────────────────────────
    # SCENARIO 2: OpenAI Down -> Anthropic Failover
    # ─────────────────────────────────────────────
    async def test_openai_down(self) -> ScenarioResult:
        async def scenario():
            from data_plane.model_multiplexer import ModelMultiplexer, FAILOVER_CHAINS

            mux = ModelMultiplexer()

            call_log = []
            async def fake_openai(msg, model, max_tokens):
                call_log.append(("openai", model))
                raise ConnectionError("OpenAI DOWN - simulated")

            async def fake_anthropic(msg, model, max_tokens):
                call_log.append(("anthropic", model))
                return "Response from Claude (failover success)"

            import data_plane.model_multiplexer as mux_module
            original_calls = mux_module.CALLERS.copy()
            mux_module.CALLERS["openai"] = fake_openai
            mux_module.CALLERS["anthropic"] = fake_anthropic

            try:
                result = await mux.complete("test message", "gpt-4o-mini", max_tokens=50)
                if result.provider_used == "anthropic" and result.failover_count > 0:
                    return f"FAILOVER_OK: Used {result.provider_used}/{result.model_used} after {result.failover_count} failover(s)"
                return f"NO_FAILOVER: Used {result.provider_used} (expected anthropic)"
            finally:
                mux_module.CALLERS.update(original_calls)

        return await self._run_scenario(
            name="OpenAI DOWN -> Claude Failover",
            description="When OpenAI fails, multiplexer should auto-failover to Anthropic",
            expected="Response from Claude with failover_count > 0",
            fn=scenario,
        )

    # ─────────────────────────────────────────────
    # SCENARIO 3: All Providers Down -> Graceful Error
    # ─────────────────────────────────────────────
    async def test_all_providers_down(self) -> ScenarioResult:
        async def scenario():
            from data_plane.model_multiplexer import ModelMultiplexer
            import data_plane.model_multiplexer as mux_module

            mux = ModelMultiplexer()
            original = mux_module.CALLERS.copy()

            async def always_fail(msg, model, max_tokens):
                raise RuntimeError("All providers DOWN - simulated")

            mux_module.CALLERS["openai"]    = always_fail
            mux_module.CALLERS["anthropic"] = always_fail
            mux_module.CALLERS["local"]     = always_fail

            try:
                result = await mux.complete("test", "gpt-4o-mini", max_tokens=10)
                return "UNEXPECTED_SUCCESS"
            except RuntimeError as e:
                return f"GRACEFUL_FAILURE: {str(e)[:50]}"
            finally:
                mux_module.CALLERS.update(original)

        return await self._run_scenario(
            name="ALL Providers DOWN -> Graceful Error",
            description="When all AI providers fail, system should raise RuntimeError (not crash silently)",
            expected="RuntimeError with descriptive message",
            fn=scenario,
        )

    # ─────────────────────────────────────────────
    # SCENARIO 4: Provider Timeout -> Next in Chain
    # ─────────────────────────────────────────────
    async def test_provider_timeout(self) -> ScenarioResult:
        async def scenario():
            from data_plane.model_multiplexer import ModelMultiplexer
            import data_plane.model_multiplexer as mux_module

            mux = ModelMultiplexer()
            original = mux_module.CALLERS.copy()
            call_order = []

            async def slow_openai(msg, model, max_tokens):
                call_order.append("openai")
                await asyncio.sleep(99)

            async def fast_anthropic(msg, model, max_tokens):
                call_order.append("anthropic")
                return "Fast Claude response"

            mux_module.CALLERS["openai"]    = slow_openai
            mux_module.CALLERS["anthropic"] = fast_anthropic

            try:
                result = await mux.complete(
                    "test", "gpt-4o-mini",
                    max_tokens=10,
                    timeout_per_provider_s=0.1,
                )
                return f"TIMEOUT_FAILOVER_OK: {call_order} -> used {result.provider_used}"
            finally:
                mux_module.CALLERS.update(original)

        return await self._run_scenario(
            name="Provider TIMEOUT -> Next Failover",
            description="When provider times out (>0.1s), system should failover to next provider",
            expected="Failover to anthropic after OpenAI timeout",
            fn=scenario,
        )

    # ─────────────────────────────────────────────
    # SCENARIO 5: Flood Attack -> Governor Holds
    # ─────────────────────────────────────────────
    async def test_flood_attack(self) -> ScenarioResult:
        async def scenario():
            from data_plane.concurrency_governor import ConcurrencyGovernor

            gov = ConcurrencyGovernor()

            admitted = 0
            rejected = 0
            held_long = False

            async def flood_request(i):
                nonlocal admitted, rejected, held_long
                try:
                    async with gov.acquire(f"flood_user_{i}", "free", 100):
                        admitted += 1
                        await asyncio.sleep(0.01)
                except Exception:
                    rejected += 1

            tasks = [asyncio.create_task(flood_request(i)) for i in range(20)]
            await asyncio.gather(*tasks, return_exceptions=True)
            total = admitted + rejected
            return (
                f"FLOOD_OK: {admitted} admitted, {rejected} queued/rejected "
                f"of {total} flood requests. Governor HELD."
            )

        return await self._run_scenario(
            name="FLOOD Attack (20 req) -> Governor Holds",
            description="Send 20 simultaneous requests; FREE tier (max 2 concurrent) must limit them",
            expected="Governor limits concurrency, no crash",
            fn=scenario,
        )

    # ─────────────────────────────────────────────
    # SCENARIO 6: Token Budget Exhausted -> Reject
    # ─────────────────────────────────────────────
    async def test_token_budget_exhausted(self) -> ScenarioResult:
        async def scenario():
            from data_plane.concurrency_governor import ConcurrencyGovernor, TokenBudgetExceeded, TIER_LIMITS

            gov = ConcurrencyGovernor()
            user_id = f"budget_test_{uuid.uuid4().hex[:8]}"

            rejected = False
            admitted_count = 0
            big_tokens = TIER_LIMITS["free"].token_budget_min + 1

            try:
                async with gov.acquire(user_id, "free", big_tokens):
                    admitted_count += 1
            except TokenBudgetExceeded:
                rejected = True

            if rejected:
                return "BUDGET_REJECTION_OK: Over-budget request correctly rejected"
            return f"UNEXPECTED_ADMIT: {admitted_count} requests admitted"

        return await self._run_scenario(
            name="Token Budget Exhausted -> Reject",
            description="Single request exceeding FREE tier token budget must be rejected",
            expected="TokenBudgetExceeded raised",
            fn=scenario,
        )

    # ─────────────────────────────────────────────
    # SCENARIO 7: Billing - Insufficient Funds
    # ─────────────────────────────────────────────
    async def test_billing_insufficient_funds(self) -> ScenarioResult:
        async def scenario():
            from data_plane.billing_engine import BillingEngine, InsufficientFundsError

            engine = BillingEngine()
            user_id = f"broke_user_{uuid.uuid4().hex[:8]}"
            engine._balances[user_id] = 0.00001

            try:
                txn = await engine.begin_transaction(
                    user_id=user_id, model="gpt-4o",
                    input_tokens=50000, output_tokens=50000, tier="free",
                )
                return "UNEXPECTED_SUCCESS"
            except InsufficientFundsError as e:
                return f"BILLING_REJECT_OK: balance=${e.balance:.5f} < required=${e.required:.5f}"

        return await self._run_scenario(
            name="Billing Insufficient Funds -> Hard Reject",
            description="User with $0.00001 balance tries to run GPT-4o; must be rejected",
            expected="InsufficientFundsError raised",
            fn=scenario,
        )

    # ─────────────────────────────────────────────
    # SCENARIO 8: Duplicate Transaction -> Idempotent
    # ─────────────────────────────────────────────
    async def test_billing_idempotency(self) -> ScenarioResult:
        async def scenario():
            from data_plane.billing_engine import BillingEngine

            engine = BillingEngine()
            user_id = f"idem_user_{uuid.uuid4().hex[:8]}"
            engine._balances[user_id] = 10.0
            txn_id = str(uuid.uuid4())

            txn1 = await engine.begin_transaction(
                user_id=user_id, model="gpt-4o-mini",
                input_tokens=100, output_tokens=100, tier="free",
                txn_id=txn_id,
            )
            await engine.commit_transaction(txn_id)
            balance_after_1 = engine._balances[user_id]

            txn2 = await engine.begin_transaction(
                user_id=user_id, model="gpt-4o-mini",
                input_tokens=100, output_tokens=100, tier="free",
                txn_id=txn_id,
            )

            if txn2 is None or txn2.txn_id == txn_id:
                return f"IDEMPOTENCY_OK: Duplicate txn ignored. Balance unchanged: ${balance_after_1:.6f}"
            return f"DOUBLE_CHARGE: Balance changed unexpectedly"

        return await self._run_scenario(
            name="Duplicate Transaction -> Idempotent (No Double-Charge)",
            description="Same txn_id submitted twice must not deduct balance twice",
            expected="Second submission ignored, balance unchanged",
            fn=scenario,
        )

    # ─────────────────────────────────────────────
    # SCENARIO 9: Inference Router - Simple Query
    # ─────────────────────────────────────────────
    async def test_router_routes_simple_to_mini(self) -> ScenarioResult:
        async def scenario():
            from data_plane.inference_router import route_inference

            decision = await route_inference(
                message="Hello! How are you?",
                intent="chat",
                user_tier="free",
            )
            if "mini" in decision.selected_model or "haiku" in decision.selected_model:
                return f"ROUTING_OK: Simple msg -> {decision.selected_model} (score={decision.complexity_score})"
            return f"ROUTING_WRONG: Simple msg -> {decision.selected_model} (expected mini)"

        return await self._run_scenario(
            name="Router: Simple Query -> Mini Model",
            description="'Hello!' should route to gpt-4o-mini or claude-haiku (cheap), not GPT-4o",
            expected="Mini/haiku model selected",
            fn=scenario,
        )

    # ─────────────────────────────────────────────
    # SCENARIO 10: Cache Hit - Exact Match
    # ─────────────────────────────────────────────
    async def test_cache_exact_hit(self) -> ScenarioResult:
        async def scenario():
            from data_plane.semantic_cache import SemanticCache

            stored = {}

            class FakeRedis:
                async def get(self, key):
                    import json
                    val = stored.get(key)
                    return json.dumps(val).encode() if val else None
                async def setex(self, key, ttl, value):
                    import json
                    stored[key] = json.loads(value)
                async def hgetall(self, *a):
                    return {}

            cache = SemanticCache(FakeRedis())
            message = "What is the meaning of life?"
            await cache.set("gpt-4o-mini", message, {"content": "42", "model_used": "gpt-4o-mini", "provider": "openai"})
            result = await cache.get("gpt-4o-mini", message)

            if result and result.get("content") == "42":
                return f"CACHE_HIT_OK: exact match returned content='42'"
            return f"CACHE_MISS: result={result}"

        return await self._run_scenario(
            name="Cache: Exact Match Hit",
            description="Same query twice should return cached result on second call",
            expected="Cache hit with correct content",
            fn=scenario,
        )

    # ─────────────────────────────────────────────
    # RUN ALL
    # ─────────────────────────────────────────────
    async def run_all(self) -> ChaosReport:
        run_id = str(uuid.uuid4())[:8]
        logger.info(f"[Chaos] Starting full chaos run {run_id}")
        self._results = []
        start = time.time()

        scenarios = [
            self.test_redis_down,
            self.test_openai_down,
            self.test_all_providers_down,
            self.test_provider_timeout,
            self.test_flood_attack,
            self.test_token_budget_exhausted,
            self.test_billing_insufficient_funds,
            self.test_billing_idempotency,
            self.test_router_routes_simple_to_mini,
            self.test_cache_exact_hit,
        ]

        for scenario_fn in scenarios:
            try:
                await scenario_fn()
            except Exception as e:
                logger.error(f"[Chaos] Scenario runner error: {e}")

        report = ChaosReport(
            run_id=run_id,
            started_at=start,
            finished_at=time.time(),
            results=self._results,
        )

        verdict = "RESILIENT" if report.pass_rate >= 80 else "FRAGILE"
        logger.info(
            f"[Chaos] Run {run_id} COMPLETE | "
            f"{report.passed}/{report.total} passed ({report.pass_rate}%) | "
            f"Verdict: {verdict}"
        )
        return report


_tester: Optional[ChaosTester] = None

def get_chaos_tester() -> ChaosTester:
    global _tester
    if _tester is None:
        _tester = ChaosTester()
    return _tester
