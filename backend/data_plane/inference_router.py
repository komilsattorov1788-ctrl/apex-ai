"""
APEX AI - DATA PLANE: Inference Router
========================================
Smart model selection engine — the "brain of the brain".

Philosophy: Use the CHEAPEST model that can solve the problem.
  - Simple questions    → gpt-4o-mini   ($0.15/1M tokens)
  - Complex reasoning   → gpt-4o         ($5.00/1M tokens)  
  - Creative/long tasks → claude-3-5-sonnet ($3.00/1M tokens)
  - Fallback/offline    → local Ollama (free)

This is exactly what OpenAI does internally: route cheap traffic
to smaller models, reserve big models for hard problems.

Routing signals:
  1. Token count estimate (long prompt → capable model)
  2. Intent classification (code/math → reasoning model)
  3. Keyword complexity signals (explain, analyze, compare → pro)
  4. User tier (free → mini, pro → full models)
  5. Current model health (circuit breaker status)
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

logger = logging.getLogger("apex.data_plane.inference_router")


# ─────────────────────────────────────────────
# MODEL REGISTRY
# Each model has: id, cost_per_1k_tokens, max_context, speed_tier
# ─────────────────────────────────────────────
class ModelTier(str, Enum):
    NANO   = "nano"    # Fastest, cheapest — simple Q&A
    MINI   = "mini"    # Light reasoning — most traffic
    PRO    = "pro"     # Full reasoning — complex tasks
    ULTRA  = "ultra"   # Most capable — critical tasks
    LOCAL  = "local"   # Free, offline — fallback


@dataclass
class ModelSpec:
    model_id: str
    provider: str            # openai | anthropic | google | local
    tier: ModelTier
    cost_per_1k_input: float  # USD
    cost_per_1k_output: float # USD
    max_context_tokens: int
    avg_latency_ms: int
    supports_streaming: bool  = True
    supports_vision: bool     = False
    supports_function_calls: bool = True


MODEL_REGISTRY: dict[str, ModelSpec] = {
    # ── OpenAI ──
    "gpt-4o-mini": ModelSpec(
        model_id="gpt-4o-mini", provider="openai",
        tier=ModelTier.MINI,
        cost_per_1k_input=0.00015, cost_per_1k_output=0.00060,
        max_context_tokens=128_000, avg_latency_ms=400,
        supports_vision=True
    ),
    "gpt-4o": ModelSpec(
        model_id="gpt-4o", provider="openai",
        tier=ModelTier.PRO,
        cost_per_1k_input=0.005, cost_per_1k_output=0.015,
        max_context_tokens=128_000, avg_latency_ms=800,
        supports_vision=True
    ),
    "o1-mini": ModelSpec(
        model_id="o1-mini", provider="openai",
        tier=ModelTier.PRO,
        cost_per_1k_input=0.003, cost_per_1k_output=0.012,
        max_context_tokens=65_536, avg_latency_ms=3000,
        supports_streaming=False  # o1 doesn't stream
    ),

    # ── Anthropic ──
    "claude-3-haiku-20240307": ModelSpec(
        model_id="claude-3-haiku-20240307", provider="anthropic",
        tier=ModelTier.MINI,
        cost_per_1k_input=0.00025, cost_per_1k_output=0.00125,
        max_context_tokens=200_000, avg_latency_ms=500,
    ),
    "claude-3-5-sonnet-20241022": ModelSpec(
        model_id="claude-3-5-sonnet-20241022", provider="anthropic",
        tier=ModelTier.PRO,
        cost_per_1k_input=0.003, cost_per_1k_output=0.015,
        max_context_tokens=200_000, avg_latency_ms=900,
        supports_vision=True
    ),

    # ── Local (fallback) ──
    "ollama/llama3": ModelSpec(
        model_id="ollama/llama3", provider="local",
        tier=ModelTier.LOCAL,
        cost_per_1k_input=0.0, cost_per_1k_output=0.0,
        max_context_tokens=8_000, avg_latency_ms=2000,
        supports_function_calls=False
    ),
}

# ─────────────────────────────────────────────
# COMPLEXITY SCORING
# ─────────────────────────────────────────────

# Signals that suggest HIGH complexity → use PRO model
HIGH_COMPLEXITY_PATTERNS = [
    r'\banalyze\b', r'\bexplain\b.*\bin detail\b', r'\bcompare\b',
    r'\bdifference between\b', r'\bwrite a\b.*\bprogram\b',
    r'\barchitecture\b', r'\boptimize\b', r'\bdebug\b',
    r'\bsummarize\b.*\bdocument\b', r'\btranslate\b.*\bcode\b',
    r'\brefactor\b', r'\bsecurity\b.*\baudit\b',
    r'\bmathematically\b', r'\bprove\b', r'\bderive\b',
    r'\bcomplex\b', r'\bin-depth\b', r'\bcomprehensive\b',
]

# Signals that suggest LOW complexity → use MINI model
LOW_COMPLEXITY_PATTERNS = [
    r'^hi\b', r'^hello\b', r'^hey\b',
    r'\bwhat is\b.{1,30}\?$',
    r'\bwho is\b.{1,30}\?$',
    r'\bwhen is\b.{1,30}\?$',
    r'\btranslate\b.{1,50}$',
    r'^\w+\s+\w+\s*\?$',   # Very short questions
]

# Intent → recommended tier
INTENT_TIER_MAP: dict[str, ModelTier] = {
    "chat":       ModelTier.MINI,
    "knowledge":  ModelTier.MINI,
    "code":       ModelTier.PRO,
    "math":       ModelTier.PRO,
    "image":      ModelTier.MINI,
    "video":      ModelTier.PRO,
    "analysis":   ModelTier.PRO,
    "creative":   ModelTier.PRO,
    "translation":ModelTier.MINI,
}


@dataclass
class RoutingDecision:
    selected_model: str
    provider: str
    tier: ModelTier
    reason: str
    estimated_cost_usd: float
    fallback_chain: list[str] = field(default_factory=list)
    complexity_score: int = 0  # 0-100


def _estimate_tokens(text: str) -> int:
    """Fast token estimation: ~1 token per 4 characters (GPT-style)."""
    return max(1, len(text) // 4)


def _score_complexity(message: str) -> int:
    """
    Score 0-100:
      0-30:  Simple → MINI
      31-60: Medium → MINI (but high confidence needed)
      61-100: Complex → PRO
    """
    score = 0
    msg_lower = message.lower()

    # Token-based signals
    tokens = _estimate_tokens(message)
    if tokens > 500: score += 25
    elif tokens > 200: score += 15
    elif tokens > 100: score += 8

    # Code blocks → complex
    if "```" in message or "def " in message or "class " in message:
        score += 30

    # Multi-paragraph → complex
    if message.count('\n') > 3:
        score += 15

    # High complexity keywords
    for pattern in HIGH_COMPLEXITY_PATTERNS:
        if re.search(pattern, msg_lower):
            score += 12
            break  # one hit is enough

    # Low complexity patterns (subtract score)
    for pattern in LOW_COMPLEXITY_PATTERNS:
        if re.search(pattern, msg_lower):
            score -= 20
            break

    # Numbers, equations → complex
    if re.search(r'\d+[\+\-\*\/\^]\d+', message):
        score += 10

    # Question with list → complex
    if re.search(r'\b(list|enumerate|steps|pros.*cons)\b', msg_lower):
        score += 10

    return max(0, min(100, score))


# ─────────────────────────────────────────────
# MAIN ROUTING FUNCTION
# ─────────────────────────────────────────────

async def route_inference(
    message: str,
    intent: str = "chat",
    user_tier: str = "free",       # "free" | "pro" | "enterprise"
    requested_model: Optional[str] = None,
    unhealthy_models: Optional[set] = None,
) -> RoutingDecision:
    """
    Core routing logic:
      1. If user explicitly requested a model AND it's healthy → honor it
      2. Score complexity
      3. Apply user tier constraints
      4. Select cheapest model that meets requirements
      5. Build fallback chain
    
    Args:
        message: The user's query text
        intent: Classified intent ("code", "chat", "image", etc.)
        user_tier: User subscription level
        requested_model: Model explicitly requested by user
        unhealthy_models: Set of model IDs currently circuit-broken

    Returns:
        RoutingDecision with selected model and fallback chain
    """
    unhealthy = unhealthy_models or set()
    complexity = _score_complexity(message)
    tokens     = _estimate_tokens(message)

    logger.debug(f"[InferenceRouter] complexity={complexity} tokens={tokens} intent={intent} tier={user_tier}")

    # ── Step 1: Honor explicit request if healthy ──
    if requested_model and requested_model in MODEL_REGISTRY:
        spec = MODEL_REGISTRY[requested_model]
        if requested_model not in unhealthy:
            fallback = _build_fallback_chain(requested_model, intent, user_tier, unhealthy)
            cost = _estimate_cost(spec, tokens)
            return RoutingDecision(
                selected_model=requested_model,
                provider=spec.provider,
                tier=spec.tier,
                reason=f"User requested model honored",
                estimated_cost_usd=cost,
                fallback_chain=fallback,
                complexity_score=complexity,
            )

    # ── Step 2: Determine required tier ──
    intent_tier = INTENT_TIER_MAP.get(intent, ModelTier.MINI)

    if complexity >= 60 or intent_tier == ModelTier.PRO:
        required_tier = ModelTier.PRO
        reason_base = f"Complex query (score={complexity}) requires PRO model"
    elif complexity >= 30:
        required_tier = ModelTier.MINI
        reason_base = f"Medium complexity (score={complexity}) → MINI sufficient"
    else:
        required_tier = ModelTier.NANO if complexity < 15 else ModelTier.MINI
        reason_base = f"Simple query (score={complexity}) → lightweight model"

    # ── Step 3: Apply user tier constraints ──
    if user_tier == "free" and required_tier == ModelTier.PRO:
        # Free users get MINI even for complex queries, with a warning
        required_tier = ModelTier.MINI
        reason_base += " [downgraded: free tier]"
    elif user_tier == "enterprise":
        # Enterprise gets PRO for anything non-trivial
        if complexity > 20:
            required_tier = ModelTier.PRO

    # ── Step 4: Select model ──
    selected = _select_model(required_tier, intent, unhealthy)

    if selected is None:
        # All models unhealthy → emergency local fallback
        selected = "ollama/llama3"
        reason_base += " [EMERGENCY: all cloud models degraded]"

    spec = MODEL_REGISTRY[selected]
    cost = _estimate_cost(spec, tokens)
    fallback = _build_fallback_chain(selected, intent, user_tier, unhealthy)

    return RoutingDecision(
        selected_model=selected,
        provider=spec.provider,
        tier=spec.tier,
        reason=reason_base,
        estimated_cost_usd=cost,
        fallback_chain=fallback,
        complexity_score=complexity,
    )


def _select_model(tier: ModelTier, intent: str, unhealthy: set) -> Optional[str]:
    """Select cheapest healthy model for the required tier."""
    # Priority order per tier
    tier_preferences: dict[ModelTier, list[str]] = {
        ModelTier.NANO:  ["gpt-4o-mini", "claude-3-haiku-20240307"],
        ModelTier.MINI:  ["gpt-4o-mini", "claude-3-haiku-20240307"],
        ModelTier.PRO:   ["gpt-4o", "claude-3-5-sonnet-20241022", "o1-mini"],
        ModelTier.ULTRA: ["gpt-4o", "claude-3-5-sonnet-20241022"],
        ModelTier.LOCAL: ["ollama/llama3"],
    }

    # Code intent → prefer Claude (excellent at coding)
    if intent == "code" and tier == ModelTier.PRO:
        tier_preferences[ModelTier.PRO] = [
            "claude-3-5-sonnet-20241022", "gpt-4o", "o1-mini"
        ]

    candidates = tier_preferences.get(tier, ["gpt-4o-mini"])
    for candidate in candidates:
        if candidate not in unhealthy and candidate in MODEL_REGISTRY:
            return candidate
    return None


def _build_fallback_chain(primary: str, intent: str, user_tier: str, unhealthy: set) -> list[str]:
    """Build ordered fallback chain: if primary fails, try these in order."""
    all_options = ["gpt-4o-mini", "claude-3-haiku-20240307", "gpt-4o",
                   "claude-3-5-sonnet-20241022", "ollama/llama3"]
    chain = []
    for model in all_options:
        if model != primary and model not in unhealthy:
            chain.append(model)
        if len(chain) >= 3:
            break
    chain.append("ollama/llama3")  # Always last resort
    return list(dict.fromkeys(chain))  # Remove duplicates preserving order


def _estimate_cost(spec: ModelSpec, input_tokens: int) -> float:
    """Estimate cost for a typical request."""
    output_tokens = min(input_tokens * 2, 2000)  # Rough output estimate
    cost = (input_tokens / 1000 * spec.cost_per_1k_input +
            output_tokens / 1000 * spec.cost_per_1k_output)
    return round(cost, 6)


# ─────────────────────────────────────────────
# COST ANALYTICS
# ─────────────────────────────────────────────

def get_model_cost_table() -> list[dict]:
    """Return sortable cost comparison table for dashboard."""
    return sorted([
        {
            "model": spec.model_id,
            "provider": spec.provider,
            "tier": spec.tier.value,
            "cost_per_1k_input_usd": spec.cost_per_1k_input,
            "cost_per_1k_output_usd": spec.cost_per_1k_output,
            "max_context_k": spec.max_context_tokens // 1000,
            "avg_latency_ms": spec.avg_latency_ms,
            "streams": spec.supports_streaming,
        }
        for spec in MODEL_REGISTRY.values()
        if spec.provider != "local"
    ], key=lambda x: x["cost_per_1k_input_usd"])
