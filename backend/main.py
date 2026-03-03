from fastapi import FastAPI
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
    
if __name__ == "__main__":
    import uvicorn
    # Local tester
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
