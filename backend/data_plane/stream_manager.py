import json
import asyncio
from typing import AsyncGenerator

async def sse_response_generator(source: AsyncGenerator):
    """
    Transforms a stream of dicts/tokens into Server-Sent Events (SSE) format.
    """
    try:
        async for chunk in source:
            # chunk is expected to be a dict or a string token
            if isinstance(chunk, dict):
                data = json.dumps(chunk)
            else:
                data = json.dumps({"token": chunk})
            
            yield f"data: {data}\n\n"
            
    except asyncio.CancelledError:
        # Client disconnected
        pass
    except Exception as e:
        yield f"data: {json.dumps({'error': str(e)})}\n\n"
