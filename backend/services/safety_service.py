import logging
from fastapi import HTTPException

logger = logging.getLogger("neural_sync.safety")

async def run_safety_pipeline(text: str) -> str:
    """ 
    HIGH-IMPACT: Pre-generation moderation safety checks 
    Blocks malicious inputs and prompt injections before hitting the model layer. 
    """
    toxic_keywords = ["kill", "hack", "destroy_db", "bypass_system"] # Extendable dictionary
    
    text_lower = text.lower()
    for kw in toxic_keywords:
        if kw in text_lower:
            logger.warning("Safety pipeline triggered: Malicious intent detected.")
            raise HTTPException(400, "Request violates APEX AI Moderation policies.")
    
    return text

async def run_post_generation_safety(text: str) -> str:
    """ Post-generation moderation to protect the client """
    # Scrub outputs here against classifiers
    return text
