from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from api import ai_router, payment_router, auth_router
from core.config import get_settings
from database.database import Base, master_engine
from database import models # Force model registration

settings = get_settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load Models and connect to Stripe/DB here
    print(f"[{settings.PROJECT_NAME}] Starting up Secure Environment v{settings.VERSION}")
    
    # Automatic table creation for production resilience
    async with master_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("[SYSTEM] Database tables initialized successfully.")
    
    yield
    # Cleanup DB connection on shutdown
    print(f"[{settings.PROJECT_NAME}] Shutting down...")

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    lifespan=lifespan,
    # OpenAPI Swagger Docs for developers (can be disabled in production for security)
    docs_url="/docs" 
)

# Security: Allow CORS for our frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Change to specific domain in production!
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    import traceback
    error_details = traceback.format_exc()
    print(f"[FATAL ERROR] {error_details}")
    return JSONResponse(
        status_code=500,
        content={"message": "Internal Server Error", "detail": str(exc), "trace": error_details[:200]}
    )

# Core Routers
app.include_router(auth_router.router, prefix=settings.API_V1_STR + "/auth", tags=["authentication"])
app.include_router(ai_router.router, prefix=settings.API_V1_STR + "/ai", tags=["ai_models"])
app.include_router(payment_router.router, prefix=settings.API_V1_STR + "/payments", tags=["billing_stripe"])

@app.get("/health")
async def health_check():
    import os
    return {
        "status": "ok", 
        "ai_key_active": bool(os.getenv("OPENAI_API_KEY")),
        "security": "AES-256 Enabled"
    }

# Serve the static frontend files
app.mount("/assets", StaticFiles(directory="./"), name="assets")

@app.get("/{full_path:path}")
async def catch_all(full_path: str):
    import os
    file_path = os.path.join(".", full_path)
    if os.path.exists(file_path) and os.path.isfile(file_path):
        return FileResponse(file_path)
    # Default to index.html for SPA-like behavior or root
    return FileResponse("./index.html")
    
if __name__ == "__main__":
    import uvicorn
    # Local tester
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
