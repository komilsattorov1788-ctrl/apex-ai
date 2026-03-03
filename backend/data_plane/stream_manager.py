import os
import json
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, AsyncGenerator, Any

logger = logging.getLogger("apex.data_plane.stream_manager")

@dataclass
class StreamEvent:
    text: str
    is_finished: bool = False
    model_used: Optional[str] = None
    error: Optional[str] = None

async def sse_response_generator(source: AsyncGenerator[Any, None]):
    """
    Transforms a stream of StreamEvent or dicts into Server-Sent Events (SSE) format for ai_router.py
    """
    try:
        async for event in source:
            if hasattr(event, "__dict__"):
                data = json.dumps(event.__dict__)
            elif isinstance(event, dict):
                data = json.dumps(event)
            else:
                data = json.dumps({"text": str(event)})
            
            yield f"data: {data}\n\n"
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"SSE Generator Error: {e}")
        yield f"data: {json.dumps({'error': str(e)})}\n\n"

async def dispatch_stream(
    message: str,
    model: str,
    stream_id: Optional[str] = None,
) -> AsyncGenerator[StreamEvent, None]:
    """
    Routes streaming requests to the appropriate provider based on the model ID.
    """
    # Detect provider from model ID prefix or registry
    # In our project, gpt-* is openai, claude-* is anthropic, ollama/* is local
    
    if model.startswith("gpt"):
        async for event in _stream_openai(message, model):
            yield event
    elif model.startswith("claude"):
        async for event in _stream_anthropic(message, model):
            yield event
    elif model.startswith("ollama"):
        async for event in _stream_ollama(message, model):
            yield event
    else:
        # Fallback to OpenAI mini
        async for event in _stream_openai(message, "gpt-4o-mini"):
            yield event

async def _stream_openai(message: str, model: str) -> AsyncGenerator[StreamEvent, None]:
    import openai
    try:
        client = openai.AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY", "sk-fake-key"))
        stream = await client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": message}],
            stream=True,
            max_tokens=1024,
        )
        async for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                yield StreamEvent(text=chunk.choices[0].delta.content, model_used=model)
        yield StreamEvent(text="", is_finished=True, model_used=model)
    except Exception as e:
        logger.error(f"OpenAI Stream Error: {e}")
        yield StreamEvent(text="", error=str(e), is_finished=True)

async def _stream_anthropic(message: str, model: str) -> AsyncGenerator[StreamEvent, None]:
    import anthropic
    try:
        client = anthropic.AsyncAnthropic(api_key=os.getenv("CLAUDE_API_KEY", "sk-fake-key"))
        async with client.messages.stream(
            model=model,
            messages=[{"role": "user", "content": message}],
            max_tokens=1024,
        ) as stream:
            async for text in stream.text_stream:
                yield StreamEvent(text=text, model_used=model)
        yield StreamEvent(text="", is_finished=True, model_used=model)
    except Exception as e:
        logger.error(f"Anthropic Stream Error: {e}")
        yield StreamEvent(text="", error=str(e), is_finished=True)

async def _stream_ollama(message: str, model: str) -> AsyncGenerator[StreamEvent, None]:
    import httpx
    try:
        ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
        model_name = model.replace("ollama/", "")
        async with httpx.AsyncClient(timeout=60.0) as client:
            async with client.stream(
                "POST",
                f"{ollama_url}/api/chat",
                json={
                    "model": model_name,
                    "messages": [{"role": "user", "content": message}],
                    "stream": True,
                }
            ) as response:
                async for line in response.aiter_lines():
                    if not line: continue
                    data = json.loads(line)
                    if "message" in data and "content" in data["message"]:
                        yield StreamEvent(text=data["message"]["content"], model_used=model)
                    if data.get("done"): break
        yield StreamEvent(text="", is_finished=True, model_used=model)
    except Exception as e:
        logger.error(f"Ollama Stream Error: {e}")
        yield StreamEvent(text="", error=str(e), is_finished=True)
