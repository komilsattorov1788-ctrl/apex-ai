import logging
import uuid
import time
import hashlib
import hmac
import os
import asyncio
from fastapi import APIRouter, Depends, HTTPException, status, Header, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, constr
from opentelemetry import trace

# ── DATA PLANE ──────────────────────────────────────────────────
from data_plane.gateway import get_data_plane
from data_plane.stream_manager import sse_response_generator
# ────────────────────────────────────────────────────────────────

from services.language_service import get_language
from services.intent_service import classify_intent
from security.redis_limiter import (
    check_rate_limit, reserve_credits, commit_credits, rollback_credits,
    acquire_idempotency_lock, set_idempotency, clear_idempotency,
    check_circuit_breaker, record_circuit_latency, send_to_outbox, get_redis
)
from security.jwt import get_current_user, TokenData
from services.task_service import get_task_state, set_task_state, add_user_task_index

from services.safety_service import run_safety_pipeline, run_post_generation_safety
from services.model_router import route_model
from services.retriever_service import retriever

# Direct Asyncio Database Outbox for 10/10 Scaling
import json
from database.database import AsyncSessionLocal
from database.models import OutboxEvent

logger = logging.getLogger("neural_sync.api")
tracer = trace.get_tracer(__name__)

IDEMP_SECRET = os.getenv("IDEMP_SECRET", "apex-ai-super-secret-key-10-10")

router = APIRouter()

class ChatRequest(BaseModel):
    message: constr(strip_whitespace=True, min_length=1, max_length=10000) = Field(...)
    model: str = Field(default="gpt-4o", max_length=50)
    language: str = Field(default="en", max_length=10)

class ChatResponse(BaseModel):
    model_config = {"populate_by_name": True}
    response_type: str = Field(..., alias="type")
    content: str
    source: str = "System"
    task_id: str | None = None
    target_engine: str | None = None 

from fastapi import APIRouter, Depends, HTTPException, status, Header, Body

# ... (omitting irrelevant lines but matching exactly the logic)
@router.post("/chat", response_model=ChatResponse)
async def ai_chat_completion(
    request: ChatRequest,
    user: TokenData = Depends(get_current_user),
    idempotency_key: str = Header(..., alias="Idempotency-Key", max_length=100)
):
    with tracer.start_as_current_span("ai_chat_completion") as span:
        span.set_attribute("user.id", user.user_id)

        
        estimated_tokens = len(request.message) // 4
        if estimated_tokens > 3000:
            logger.warning(f"Rejecting payload size burst from {user.user_id}: ~{estimated_tokens} tokens")
            raise HTTPException(413, "Token limit dimension exceeded for safe execution.")

        clean_msg = request.message.replace("\x00", "") 
        
        try:
            await run_safety_pipeline(clean_msg)
        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"Safety API component degraded, executing via raw baseline: {e}")
        
        intent_data = await classify_intent(clean_msg)
        intent = intent_data.get("intent", "chat")
        span.set_attribute("ai.intent", intent)
        
        confidence = intent_data.get("confidence", 1.0)
        target_engine = await route_model(intent, clean_msg, confidence)
        span.set_attribute("ai.target_engine", target_engine)
        
        await check_circuit_breaker(target_engine)
        
        redis = await get_redis()
        outbox_len = await redis.xlen("stream:outbox_ai_tasks")
        if outbox_len > 80000:
            raise HTTPException(503, "Cluster Queue Under Spike Attack - Backpressure Isolation Active.")
        
        if intent in ["knowledge", "chat"] and "local" not in target_engine:
            try:
                context = await asyncio.wait_for(
                    asyncio.to_thread(retriever.get_context, clean_msg), 
                    timeout=2.0
                )
                if context:
                    clean_msg = f"Domain Context:\n{context}\n\nUser Query:\n{clean_msg}"
                    span.add_event("rag_context_injected_successfully")
            except asyncio.TimeoutError:
                logger.warning("RAG IO Layer blocking Timeout -> Ignoring grounding softly.")
            except Exception as e:
                pass
        
        cost = estimated_tokens if estimated_tokens > 0 else 1
        cost = max(cost, 10 if intent == "video" else (3 if intent == "image" else 1))
        
        raw_payload_bind = f"{request.model}:{request.language}:{cost}:{intent}:{clean_msg}"
        payload_hash = hashlib.sha256(raw_payload_bind.encode()).hexdigest()
        raw_sig_bind = f"{user.user_id}:{payload_hash}"
        
        sig = hmac.new(IDEMP_SECRET.encode('utf-8'), raw_sig_bind.encode('utf-8'), hashlib.sha256).hexdigest()
        correlation_id = f"{idempotency_key}:{sig}"
        
        # CRITICAL 1 FIX: Identity ↔ Ledger Separation Logical Bug Fixed 100% Natively
        tx_id = hashlib.sha256(correlation_id.encode('utf-8')).hexdigest()[:36]
        span.set_attribute("transaction.id", tx_id)
        
        await check_rate_limit(user.user_id, limit=60 if user.role == "pro" else 10, window=60)
        
        state = await acquire_idempotency_lock(user.user_id, correlation_id)
        if state["status"] in ["completed", "failed"]:
            span.add_event("idempotency_hit")
            return ChatResponse(**state["response"])
        elif state["status"] == "processing":
            if state["response"]: return ChatResponse(**state["response"])
            return ChatResponse(response_type="processing", content="Task queued safely.", source="System")

        start_exec_time = time.time()
        is_reserved = False
        try:
            # SOT Explicit DB insertion via SqlAlchemy properly completely safely blocking duplicates implicitly
            is_reserved = await reserve_credits(
                user.user_id, cost, tx_id, traceparent=str(span.get_span_context().trace_id), intent=intent
            )
            
            if not is_reserved:
                raise HTTPException(402, "Insufficient Credits or Duplicate Transaction Lock.")
                
            lang_code = await get_language(clean_msg, default_lang=request.language)
            
            if intent in ["video", "image", "knowledge"]:
                task_id = str(uuid.uuid4())
                await set_task_state(task_id, {
                    "status": "pending", "progress": 0, "user_id": user.user_id,
                    "intent": intent, "target_engine": target_engine,
                    "started_at": int(time.time()), 
                    "cost_deducted": False, "error": None
                })
                await add_user_task_index(user.user_id, task_id)
                
                resp = ChatResponse(response_type="task", content="Task queued.", task_id=task_id, target_engine=target_engine)
                await set_idempotency(user.user_id, correlation_id, "processing", resp.dict())

                try:
                    payload = {
                        "aggregate_type": "ai_task_job", "aggregate_id": task_id,
                        "task_id": task_id, "intent": intent, "message": clean_msg, 
                        "lang_code": lang_code, "user_id": user.user_id, "cost": cost, "tx_id": tx_id,
                        "correlation_id": correlation_id, "target_engine": target_engine
                    }
                    
                    # OUTBOX KAFKA-CLASS DURABILITY: Direct Explicit SQLALchemy Row Injection bypassing async loss!
                    async with AsyncSessionLocal() as session:
                        outbox_job = OutboxEvent(tx_id=tx_id, payload=json.dumps(payload), status="pending")
                        session.add(outbox_job)
                        await session.commit()
                        
                except Exception as b_err:
                    span.record_exception(b_err)
                    logger.error(f"[{tx_id}] SQL OUTBOX INSERT DROP: {b_err}")
                    await rollback_credits(user.user_id, tx_id)
                    await clear_idempotency(user.user_id, correlation_id)
                    raise HTTPException(502, "Outbox Persistence error. Payment transaction implicitly cancelled.")
                
                return resp

            # ── DATA PLANE: Cache → Route → Multiplex → Cache ──────────
            data_plane = get_data_plane()
            dp_result = await data_plane.complete(
                message=clean_msg,
                model=target_engine,
                intent=intent,
                user_tier=user.role,
                max_tokens=1024,
            )
            final_content = dp_result["content"]

            if not final_content:
                # Fallback to legacy services if Data Plane returns empty
                from services.code_service import generate_code
                from services.chat_service import get_default_chat_response
                handler = generate_code if intent == "code" else get_default_chat_response
                result_dict = await handler(clean_msg, lang_code)
                final_content = result_dict.get("content", "")
            # ─────────────────────────────────────────────────────────────

            try:
                final_content = await run_post_generation_safety(final_content)
            except Exception:
                pass

            commit_success = await commit_credits(user.user_id, tx_id, exact_used_cost=max(1, len(final_content)//50))
            if not commit_success: raise HTTPException(500, "Ledger Commit Verify fault.")

            latency = (time.time() - start_exec_time) * 1000
            await record_circuit_latency(target_engine, latency_ms=latency, is_error=False)

            # Add Data Plane metadata to response
            resp = ChatResponse(
                response_type=intent,
                content=final_content,
                target_engine=dp_result.get("model_used", target_engine),
                source=f"{dp_result.get('provider','system')} | cache={'HIT' if dp_result.get('cache_hit') else 'MISS'} | complexity={dp_result.get('complexity_score',0)}",
            )
            await set_idempotency(user.user_id, correlation_id, "completed", resp.dict())
            return resp

        except HTTPException as e:
            status_c = e.status_code
            if is_reserved and status_c not in (402, 502): await rollback_credits(user.user_id, tx_id)
            if status_c in (500, 502, 503, 504): await clear_idempotency(user.user_id, correlation_id)
            span.record_exception(e)
            raise

        except Exception as e:
            if is_reserved: await rollback_credits(user.user_id, tx_id)
            await clear_idempotency(user.user_id, correlation_id)
            
            latency = (time.time() - start_exec_time) * 1000
            await record_circuit_latency(target_engine, latency_ms=latency, is_error=True)
            span.record_exception(e)
            logger.exception(f"[{tx_id}] Catch-all rollback: {str(e)}")
            raise HTTPException(500, "System fault cleanly rolled back.")


# ═══════════════════════════════════════════════════════════════
# DATA PLANE: NEW STREAMING ENDPOINT
# ═══════════════════════════════════════════════════════════════

class StreamRequest(BaseModel):
    message: constr(strip_whitespace=True, min_length=1, max_length=10000) = Field(...)
    model: str = Field(default="gpt-4o-mini", max_length=50)


@router.post("/stream")
async def ai_stream_chat(
    request: StreamRequest,
    user: TokenData = Depends(get_current_user),
):
    """
    Token-by-token streaming endpoint (SSE).
    
    Returns Server-Sent Events stream.
    Frontend usage:
        const es = new EventSource('/api/v1/ai/stream');
        es.addEventListener('token', e => {
            const data = JSON.parse(e.data);
            appendToUI(data.data.text);
        });
    """
    # Lightweight rate check for streaming
    await check_rate_limit(user.user_id, limit=20 if user.role == "pro" else 5, window=60)

    data_plane = get_data_plane()
    stream_id  = str(uuid.uuid4())

    source = data_plane.stream(
        message=request.message,
        model=request.model,
        intent="chat",
        user_tier=user.role,
        stream_id=stream_id,
    )

    return StreamingResponse(
        sse_response_generator(source),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",     # Disable Nginx buffering
            "X-Stream-Id": stream_id,
        }
    )


@router.get("/data-plane/status")
async def data_plane_status(user: TokenData = Depends(get_current_user)):
    """Data Plane health dashboard — model availability, cache stats, failover status."""
    data_plane = get_data_plane()
    return await data_plane.get_system_status()
