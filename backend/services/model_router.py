import logging

logger = logging.getLogger("neural_sync.router")

async def route_model(intent: str, query: str, confidence: float = 1.0) -> str:
    """
    HIGH-IMPACT: Model Routing & Fallback
    Decides whether to use an expensive model (e.g., GPT-4o) or a cheaper local/fallback model 
    (like Llama-2-7b, Mistral, or gpt-4o-mini).
    """
    if intent in ["video", "image", "code"]:
        # Complex multi-modal or heavy reasoning tasks require powerful models natively
        return "gpt-4o"
        
    if confidence < 0.7 or len(query) > 2000:
        return "gpt-4o-mini" # Cheaper fast fallback for long contexts
        
    return "local-mistral-7b" # Self-hosted quantized model for short basic chat
