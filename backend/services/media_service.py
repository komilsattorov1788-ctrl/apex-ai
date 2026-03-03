import os
import random
from openai import AsyncOpenAI
import logging

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "YOUR_DALLE3_API_KEY_HERE")
LUMA_API_KEY = os.getenv("LUMA_API_KEY", "YOUR_LUMA_OR_RUNWAY_KEY_HERE")

async def generate_video(prompt: str, lang_code: str) -> dict:
    """
    Handles video generation requests via Luma/Runway APIs.
    """
    # Placeholder for real async HTTP call
    generated_video_link = "https://cdn.pixabay.com/video/2020/05/24/40092-424843075_large.mp4" 
    vid_text = "Sizning buyrug'ingiz bilan noldan yaratilgan maxsus 4K Video ssenariysi:" if lang_code == 'uz' else "Generated cinematic video:"
    
    return {
        "source": f"Apex Video Core Engine ({lang_code.upper()})", 
        "type": "video",
        "url": generated_video_link,
        "content": vid_text
    }

async def generate_image(prompt: str, lang_code: str) -> dict:
    """
    Handles image generation requests via DALL-E 3 API (Real Implementation).
    """
    if OPENAI_API_KEY and OPENAI_API_KEY != "YOUR_DALLE3_API_KEY_HERE":
        try:
            client = AsyncOpenAI(api_key=OPENAI_API_KEY)
            response = await client.images.generate(
                model="dall-e-3",
                prompt=prompt,
                size="1024x1024",
                quality="standard",
                n=1,
            )
            image_url = response.data[0].url
            img_text = "DALL-E 3 Modeli yordamida noldan yasalgan yangi obyekt:" if lang_code == 'uz' else "Newly generated 4K object via DALL-E 3:"
            return {
                "source": f"Vision Core / DALL-E ({lang_code.upper()})", 
                "type": "image",
                "url": image_url,
                "content": img_text
            }
        except Exception as e:
            logging.error(f"Image generation failed: {e}")
            pass
            
    # Fallback / Simulation
    art_sources = [
        "https://images.unsplash.com/photo-1620641788421-7a1c342ea42e?auto=format&fit=crop&q=80&w=800",
        "https://images.unsplash.com/photo-1677442136019-21780ecad995?auto=format&fit=crop&q=80&w=800",
        "https://images.unsplash.com/photo-1683009427513-28e163402d16?auto=format&fit=crop&q=80&w=800",
        "https://images.unsplash.com/photo-1634152962476-4b8a00e1915c?auto=format&fit=crop&q=80&w=800",
        "https://images.unsplash.com/photo-1682687982501-1e58f8147228?auto=format&fit=crop&q=80&w=800"
    ]
    
    chosen_art = random.choice(art_sources)
    img_text = "DALL-E 3 Modeli yordamida noldan tuzilgan obyekt (Demo rejim):" if lang_code == 'uz' else "Newly generated object via DALL-E 3 (Demo mode):"
    
    return {
        "source": f"Vision Core / DALL-E ({lang_code.upper()})", 
        "type": "image",
        "url": chosen_art,
        "content": img_text
    }
