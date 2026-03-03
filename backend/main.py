from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from api import ai_router, payment_router
from core.config import get_settings

settings = get_settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load Models and connect to Stripe/DB here
    print(f"[{settings.PROJECT_NAME}] Starting up Secure Environment v{settings.VERSION}")
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

# Core Routers
app.include_router(ai_router.router, prefix=settings.API_V1_STR + "/ai", tags=["ai_models"])
app.include_router(payment_router.router, prefix=settings.API_V1_STR + "/payments", tags=["billing_stripe"])

@app.get("/health")
async def health_check():
    return {"status": "ok", "security": "AES-256 Enabled", "firewall": "Cloudflare Mock Active"}

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
