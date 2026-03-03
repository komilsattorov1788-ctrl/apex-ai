"""
APEX AI - Adaptive Model Router
==================================
Real-time latency + cost + error-rate based routing.

Unlike the static InferenceRouter (rules-based),
this router LEARNS from actual runtime performance:

  Score = (cost_weight * cost_score) +
          (latency_weight * latency_score) +
          (reliability_weight * reliability_score)

Every inference updates the model's performance profile.
Over time, the router self-optimizes.

Example:
  gpt-4o-mini:  cost=LOW  latency=380ms  error=0.1%  -> Score=92
  gpt-4o:       cost=HIGH latency=820ms  error=0.2%  -> Score=61
  claude-haiku: cost=LOW  latency=290ms  error=0.0%  -> Score=97 <- WINNER
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional
from collections import deque

logger = logging.getLogger("apex.data_plane.adaptive_router")


# ─────────────────────────────────────────────────
# MODEL PERFORMANCE PROFILE
# ─────────────────────────────────────────────────
@dataclass
class ModelProfile:
    model_id: str
    provider: str
    cost_per_1k_input: float
    cost_per_1k_output: float
    context_window: int

    # Exponential weighted moving average latency (ms)
    ewma_latency_ms: float = 500.0
    ewma_alpha: float = 0.2

    # Rolling window error rate
    _recent_calls: deque = field(default_factory=lambda: deque(maxlen=100))
    _total_calls: int = 0
    _total_errors: int = 0
    _total_latency_ms: float = 0.0

    # Circuit breaker
    consecutive_errors: int = 0
    is_circuit_open: bool = False
    circuit_opened_at: float = 0.0
    circuit_cooldown_s: float = 30.0

    def record_success(self, latency_ms: float):
        self._recent_calls.append(1)
        self._total_calls += 1
        self._total_latency_ms += latency_ms
        self.consecutive_errors = 0

        if self.is_circuit_open:
            self.is_circuit_open = False
            logger.info(f"[AdaptiveRouter] {self.model_id} circuit CLOSED (recovered)")

        self.ewma_latency_ms = (
            self.ewma_alpha * latency_ms +
            (1 - self.ewma_alpha) * self.ewma_latency_ms
        )

    def record_error(self):
        self._recent_calls.append(0)
        self._total_calls += 1
        self._total_errors += 1
        self.consecutive_errors += 1

        if self.consecutive_errors >= 3:
            self.is_circuit_open = True
            self.circuit_opened_at = time.time()
            logger.warning(f"[AdaptiveRouter] {self.model_id} circuit OPEN ({self.consecutive_errors} consecutive errors)")

    def is_available(self) -> bool:
        if not self.is_circuit_open:
            return True
        # Auto-reset after cooldown
        if time.time() - self.circuit_opened_at > self.circuit_cooldown_s:
            self.is_circuit_open = False
            self.consecutive_errors = 0
            logger.info(f"[AdaptiveRouter] {self.model_id} circuit RESET after cooldown")
            return True
        return False

    def error_rate(self) -> float:
        if not self._recent_calls:
            return 0.0
        return 1.0 - (sum(self._recent_calls) / len(self._recent_calls))

    def avg_latency_ms(self) -> float:
        if self._total_calls == 0:
            return self.ewma_latency_ms
        return self._total_latency_ms / self._total_calls

    def cost_per_request(self, input_tokens: int = 500, output_tokens: int = 500) -> float:
        return (
            input_tokens  / 1000 * self.cost_per_1k_input +
            output_tokens / 1000 * self.cost_per_1k_output
        )

    def to_dict(self) -> dict:
        return {
            "model_id":         self.model_id,
            "provider":         self.provider,
            "ewma_latency_ms":  round(self.ewma_latency_ms, 1),
            "avg_latency_ms":   round(self.avg_latency_ms(), 1),
            "error_rate_pct":   round(self.error_rate() * 100, 2),
            "total_calls":      self._total_calls,
            "circuit_open":     self.is_circuit_open,
            "consecutive_errors": self.consecutive_errors,
        }


# ─────────────────────────────────────────────────
# MODEL REGISTRY
# ─────────────────────────────────────────────────
ADAPTIVE_MODEL_REGISTRY: dict[str, ModelProfile] = {
    "gpt-4o-mini": ModelProfile(
        "gpt-4o-mini", "openai",
        cost_per_1k_input=0.00015, cost_per_1k_output=0.00060,
        context_window=128000, ewma_latency_ms=400,
    ),
    "gpt-4o": ModelProfile(
        "gpt-4o", "openai",
        cost_per_1k_input=0.005, cost_per_1k_output=0.015,
        context_window=128000, ewma_latency_ms=800,
    ),
    "claude-3-haiku-20240307": ModelProfile(
        "claude-3-haiku-20240307", "anthropic",
        cost_per_1k_input=0.00025, cost_per_1k_output=0.00125,
        context_window=200000, ewma_latency_ms=300,
    ),
    "claude-3-5-sonnet-20241022": ModelProfile(
        "claude-3-5-sonnet-20241022", "anthropic",
        cost_per_1k_input=0.003, cost_per_1k_output=0.015,
        context_window=200000, ewma_latency_ms=900,
    ),
    "ollama/llama3": ModelProfile(
        "ollama/llama3", "local",
        cost_per_1k_input=0.0, cost_per_1k_output=0.0,
        context_window=8000, ewma_latency_ms=2000,
    ),
}


# ─────────────────────────────────────────────────
# ROUTING WEIGHTS
# ─────────────────────────────────────────────────
@dataclass
class RoutingWeights:
    cost_weight:        float = 0.35
    latency_weight:     float = 0.35
    reliability_weight: float = 0.30

    def validate(self):
        total = self.cost_weight + self.latency_weight + self.reliability_weight
        assert abs(total - 1.0) < 0.001, f"Weights must sum to 1.0, got {total}"


@dataclass
class AdaptiveRoutingDecision:
    selected_model: str
    provider: str
    reason: str
    score: float
    cost_per_req_usd: float
    ewma_latency_ms: float
    error_rate_pct: float
    fallback_chain: list
    all_scores: dict


# ─────────────────────────────────────────────────
# ADAPTIVE ROUTER
# ─────────────────────────────────────────────────
class AdaptiveModelRouter:
    """
    Scores all available models in real-time and picks the best.
    Updates profiles after each inference.
    """

    def __init__(self, weights: Optional[RoutingWeights] = None):
        self.weights = weights or RoutingWeights()
        self.weights.validate()
        self._profiles = ADAPTIVE_MODEL_REGISTRY
        self._lock = asyncio.Lock()
        self._total_routes = 0
        self._route_distribution: dict = {}

    def _score_model(
        self,
        profile: ModelProfile,
        intent: str,
        user_tier: str,
        input_tokens: int,
    ) -> float:
        if not profile.is_available():
            return -1.0

        # 1. Cost score (lower cost = higher score)
        max_cost = 0.015
        cost = profile.cost_per_request(input_tokens, input_tokens)
        cost_score = max(0.0, 1.0 - (cost / max_cost))

        # 2. Latency score (lower latency = higher score)
        max_latency = 3000.0
        latency_score = max(0.0, 1.0 - (profile.ewma_latency_ms / max_latency))

        # 3. Reliability score (lower error rate = higher score)
        reliability_score = max(0.0, 1.0 - profile.error_rate() * 5)

        # Modify weights based on context
        w = self.weights

        # For code/math: prefer reliability over cost
        if intent in ("code", "math"):
            score = (
                w.cost_weight * 0.5 * cost_score +
                w.latency_weight * latency_score +
                w.reliability_weight * 1.5 * reliability_score
            )
        # For simple chat: heavily weight cost
        elif intent == "chat" and input_tokens < 100:
            score = (
                w.cost_weight * 1.5 * cost_score +
                w.latency_weight * latency_score +
                w.reliability_weight * 0.8 * reliability_score
            )
        else:
            score = (
                w.cost_weight * cost_score +
                w.latency_weight * latency_score +
                w.reliability_weight * reliability_score
            )

        # FREE tier: penalize expensive models
        if user_tier == "free" and profile.cost_per_1k_input > 0.001:
            score *= 0.5

        return round(min(1.0, score), 4)

    async def route(
        self,
        message: str,
        intent: str = "chat",
        user_tier: str = "free",
        force_model: Optional[str] = None,
        input_tokens: Optional[int] = None,
    ) -> AdaptiveRoutingDecision:
        if input_tokens is None:
            input_tokens = max(1, len(message) // 4)

        if force_model and force_model in self._profiles:
            profile = self._profiles[force_model]
            if profile.is_available():
                return self._make_decision(force_model, profile, intent, user_tier, input_tokens, reason="forced")

        # Score all models
        all_scores = {}
        for model_id, profile in self._profiles.items():
            all_scores[model_id] = self._score_model(profile, intent, user_tier, input_tokens)

        # Sort by score descending
        ranked = sorted(all_scores.items(), key=lambda x: x[1], reverse=True)

        # Pick best available
        selected = ranked[0][0]
        profile  = self._profiles[selected]
        reason   = (
            f"Best adaptive score={all_scores[selected]:.3f} "
            f"(latency={profile.ewma_latency_ms:.0f}ms "
            f"err={profile.error_rate()*100:.1f}%)"
        )

        fallback = [m for m, s in ranked[1:] if s > 0][:3]

        async with self._lock:
            self._total_routes += 1
            self._route_distribution[selected] = self._route_distribution.get(selected, 0) + 1

        return self._make_decision(selected, profile, intent, user_tier, input_tokens, reason, all_scores, fallback)

    def _make_decision(
        self,
        model_id: str,
        profile: ModelProfile,
        intent: str,
        user_tier: str,
        input_tokens: int,
        reason: str,
        all_scores: Optional[dict] = None,
        fallback: Optional[list] = None,
    ) -> AdaptiveRoutingDecision:
        return AdaptiveRoutingDecision(
            selected_model=model_id,
            provider=profile.provider,
            reason=reason,
            score=all_scores.get(model_id, 1.0) if all_scores else 1.0,
            cost_per_req_usd=profile.cost_per_request(input_tokens, input_tokens),
            ewma_latency_ms=profile.ewma_latency_ms,
            error_rate_pct=round(profile.error_rate() * 100, 2),
            fallback_chain=fallback or [],
            all_scores=all_scores or {},
        )

    # ─────────────────────────────────────────────
    # FEEDBACK: called after each inference
    # ─────────────────────────────────────────────
    async def record_outcome(
        self,
        model_id: str,
        latency_ms: float,
        success: bool,
    ):
        profile = self._profiles.get(model_id)
        if not profile:
            return
        async with self._lock:
            if success:
                profile.record_success(latency_ms)
            else:
                profile.record_error()
        logger.debug(
            f"[AdaptiveRouter] {model_id} feedback: "
            f"success={success} latency={latency_ms:.0f}ms "
            f"new_ewma={profile.ewma_latency_ms:.0f}ms"
        )

    # ─────────────────────────────────────────────
    # STATS
    # ─────────────────────────────────────────────
    def get_stats(self) -> dict:
        return {
            "total_routes":       self._total_routes,
            "route_distribution": self._route_distribution,
            "model_profiles":     {mid: p.to_dict() for mid, p in self._profiles.items()},
            "routing_weights": {
                "cost":        self.weights.cost_weight,
                "latency":     self.weights.latency_weight,
                "reliability": self.weights.reliability_weight,
            }
        }

    def get_leaderboard(self) -> list:
        dummy_scores = {
            mid: self._score_model(p, "chat", "free", 200)
            for mid, p in self._profiles.items()
        }
        ranked = sorted(dummy_scores.items(), key=lambda x: x[1], reverse=True)
        return [
            {
                "rank":         i + 1,
                "model":        mid,
                "score":        round(score * 100, 1),
                "latency_ms":   round(self._profiles[mid].ewma_latency_ms, 1),
                "error_pct":    round(self._profiles[mid].error_rate() * 100, 2),
                "circuit_open": self._profiles[mid].is_circuit_open,
            }
            for i, (mid, score) in enumerate(ranked)
        ]


# ─────────────────────────────────────────────────
# SINGLETON
# ─────────────────────────────────────────────────
_adaptive_router: Optional[AdaptiveModelRouter] = None

def get_adaptive_router() -> AdaptiveModelRouter:
    global _adaptive_router
    if _adaptive_router is None:
        _adaptive_router = AdaptiveModelRouter()
        logger.info("[AdaptiveRouter] Initialized. Real-time latency+cost+reliability routing ready.")
    return _adaptive_router
