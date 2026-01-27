from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from .config import settings
from .core.database import create_pool, close_pool
from .core.redis import create_redis_client, close_redis_client
from .core.logger import setup_logging

# Setup logging first
setup_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for FastAPI app"""
    # Startup
    logger.info(f"Starting {settings.app_name} v{settings.app_version} on Railway")

    try:
        # Initialize database pool
        await create_pool()
        logger.info("Database pool initialized")

        # Initialize Redis client
        await create_redis_client()
        logger.info("Redis client initialized")

        # Verify Playwright installation
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                await browser.close()
            logger.info("✅ Playwright chromium verified")
        except Exception as e:
            logger.error(f"❌ Playwright verification failed: {e}")
            # Don't crash the app, just log the error
            # Kaspi auth will fail gracefully with proper error message

    except Exception as e:
        logger.error(f"Failed to initialize application: {e}")
        raise

    yield

    # Shutdown
    logger.info("Shutting down application")
    await close_pool()
    await close_redis_client()
    logger.info("Application shutdown complete")


# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    debug=settings.debug,
    lifespan=lifespan,
    docs_url="/docs" if settings.debug else None,
    redoc_url="/redoc" if settings.debug else None,
)

# CORS middleware - production-ready configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    allow_headers=["*"],
    max_age=3600,  # Cache preflight requests for 1 hour
)


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "app": settings.app_name,
        "version": settings.app_version,
    }


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "app": settings.app_name,
        "version": settings.app_version,
        "docs": "/docs" if settings.debug else "disabled",
    }


# Include all routers
from .routers import auth, kaspi, preorders, whatsapp, ai, billing, admin, invoices

app.include_router(auth.router, prefix="/auth", tags=["Authentication"])
app.include_router(kaspi.router, prefix="/kaspi", tags=["Kaspi"])
app.include_router(preorders.router, prefix="/preorders", tags=["Preorders"])
app.include_router(whatsapp.router, prefix="/whatsapp", tags=["WhatsApp"])
app.include_router(ai.router, prefix="/ai", tags=["AI Assistants"])
app.include_router(billing.router, prefix="/billing", tags=["Billing"])
app.include_router(admin.router, prefix="/admin", tags=["Admin"])
app.include_router(invoices.router, prefix="/invoices", tags=["Invoices"])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
    )
