import logging

import asyncio

def get_language_sync(text: str, default_lang: str = "en") -> str:
    # Just in case sync code needs it
    pass

async def get_language(text: str, default_lang: str = "en") -> str:
    """
    Language detection service (Async).
    Yields control explicitly to prevent any micro-blocking, ready for httpx later.
    """
    await asyncio.sleep(0) # Yield explicitly to event loop
    msg_lower = text.lower()
    
    # Fast manual override for short greetings that UI might not detect
    language_map = {
        "merhaba": "tr", "selam": "tr", "merhabalar": "tr",
        "hola": "es", "buenos dias": "es",
        "salom": "uz", "assalomu alaykum": "uz",
        "privet": "ru", "привет": "ru", "здравствуйте": "ru",
        "nihao": "zh-cn", "bonjour": "fr", "hallo": "de", "konnichiwa": "ja"
    }
    
    for greeting, code in language_map.items():
        if greeting in msg_lower:
            return code
            
    # Fallback to language passed from explicit payload by UI (Browser config)
    return default_lang
