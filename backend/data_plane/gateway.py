"""
APEX AI - DATA PLANE: Gateway
================================
Pipeline: Governor → Cache → Route → Multiplex → Cache

  Incoming Request
       │
       ▼
  [0] ConcurrencyGovernor  ──REJECT──► 429 Too Many Requests
       │ADMIT
       ▼
  [1] SemanticCache.get()  ──HIT──► return cached (1-15ms)
       │MISS
       ▼
  [2] InferenceRouter.route() ───► select cheapest healthy model
       │
       ▼
  [3] ModelMultiplexer.complete() ─► call AI, auto-failover if needed
       │
       ▼
  [4] SemanticCache.set()  ───────► store for future requests
       │
       ▼
  Return Response
"""

import logging
import time
from typing import Optional, AsyncGenerator

from data_plane.semantic_cache import get_semantic_cache
from data_plane.inference_router import route_inference
from data_plane.model_multiplexer import get_multiplexer
from data_plane.stream_manager import dispatch_stream, StreamEvent
from data_plane.concurrency_governor import (
    get_governor, TokenBudgetExceeded, QueueTimeout, SystemOverloaded
)

logger = logging.getLogger("apex.data_plane.gateway")


class DataPlaneGateway:
    """
    Unified gateway — one class rules all inference.
    Handles both blocking (complete) and streaming requests.
    """

    async def complete(
        self,
        message: str,
        model: str = "gpt-4o-mini",
        intent: str = "chat",
        user_tier: str = "free",
        max_tokens: int = 1024,
        bypass_cache: bool = False,
        user_id: str = "anonymous",
    ) -> dict:
        """
        Full pipeline: Governor → Cache → Route → Multiplex → Cache → Return.
        """
        total_start = time.time()
        estimated_tokens = max(len(message) // 4, 50)

        # ── STAGE 0: Concurrency Governor ────────────
        governor = get_governor()
        try:
            async with governor.acquire(user_id, user_tier, estimated_tokens):
                # ── STAGE 1: Semantic Cache ───────────
                cache = await get_semantic_cache()
                if not bypass_cache:
                    cached = await cache.get(model, message)
                    if cached:
                        logger.info(f"[Gateway] Cache HIT tier={cached.get('_cache_tier')} model={model}")
                        return {
                            "content":        cached.get("content", ""),
                            "model_used":     cached.get("model_used", model),
                            "provider":       cached.get("provider", "cache"),
                            "latency_ms":     round((time.time() - total_start) * 1000, 2),
                            "cache_hit":      True,
                            "cache_tier":     cached.get("_cache_tier", "unknown"),
                            "routing_reason": "served from cache",
                            "cost_usd":       0.0,
                            "failover_count": 0,
                        }

                # ── STAGE 2: Inference Routing ────────
                routing = await route_inference(
                    message=message,
                    intent=intent,
                    user_tier=user_tier,
                    requested_model=model,
                )
                logger.info(
                    f"[Gateway] Route: {routing.selected_model} "
                    f"(complexity={routing.complexity_score}, reason='{routing.reason}')"
                )

                # ── STAGE 3: Model Multiplexer ────────
                mux = get_multiplexer()
                response = await mux.complete(
                    message=message,
                    primary_model=routing.selected_model,
                    max_tokens=max_tokens,
                )

                # ── STAGE 4: Cache Store ──────────────
                await cache.set(routing.selected_model, message, {
                    "content":    response.content,
                    "model_used": response.model_used,
                    "provider":   response.provider_used,
                })

                return {
                    "content":          response.content,
                    "model_used":       response.model_used,
                    "provider":         response.provider_used,
                    "latency_ms":       round((time.time() - total_start) * 1000, 2),
                    "cache_hit":        False,
                    "cache_tier":       None,
                    "routing_reason":   routing.reason,
                    "complexity_score": routing.complexity_score,
                    "cost_usd":         routing.estimated_cost_usd,
                    "failover_count":   response.failover_count,
                    "fallback_chain":   routing.fallback_chain,
                }

        except TokenBudgetExceeded as e:
            logger.warning(f"[Gateway] Token budget exceeded: user={user_id} tier={user_tier}")
            return {
                "content": f"Rate limit reached. Retry in {e.retry_after_s:.0f}s.",
                "model_used": "none", "provider": "governor",
                "latency_ms": 0.0, "cache_hit": False, "cache_tier": None,
                "routing_reason": "token_budget_exceeded",
                "cost_usd": 0.0, "failover_count": 0,
                "error": "token_budget_exceeded",
                "retry_after_s": e.retry_after_s,
            }
        except (QueueTimeout, SystemOverloaded) as e:
            logger.warning(f"[Gateway] Overloaded: user={user_id} err={e}")
            return {
                "content": "System busy. Please try again shortly.",
                "model_used": "none", "provider": "governor",
                "latency_ms": 0.0, "cache_hit": False, "cache_tier": None,
                "routing_reason": "rate_limited",
                "cost_usd": 0.0, "failover_count": 0,
                "error": "rate_limited",
            }

    async def stream(
        self,
        message: str,
        model: str = "gpt-4o-mini",
        intent: str = "chat",
        user_tier: str = "free",
        stream_id: Optional[str] = None,
        user_id: str = "anonymous",
    ) -> AsyncGenerator[StreamEvent, None]:
        """
        Streaming pipeline: Governor → Route → Stream
        """
        routing = await route_inference(
            message=message,
            intent=intent,
            user_tier=user_tier,
            requested_model=model,
        )
        logger.info(f"[Gateway] Stream route: {routing.selected_model}")

        async for event in dispatch_stream(
            message=message,
            model=routing.selected_model,
            stream_id=stream_id,
        ):
            yield event

    async def get_system_status(self) -> dict:
        """Full health dashboard: Governor + Cache + Multiplexer."""
        mux      = get_multiplexer()
        cache    = await get_semantic_cache()
        governor = get_governor()

        return {
            "data_plane": "operational",
            "components": {
                "concurrency_governor": governor.get_stats(),
                "semantic_cache":       await cache.get_stats(),
                "model_multiplexer":    mux.get_health_report(),
                "inference_router":     "active",
                "stream_manager":       "active",
            }
        }


# ─────────────────────────────────────────────
# SINGLETON
# ─────────────────────────────────────────────
_gateway_instance: Optional[DataPlaneGateway] = None

def get_data_plane() -> DataPlaneGateway:
    global _gateway_instance
    if _gateway_instance is None:
        _gateway_instance = DataPlaneGateway()
        logger.info("[DataPlaneGateway] Initialized. Pipeline: Governor→Cache→Route→Multiplex→Cache.")
    return _gateway_instance
