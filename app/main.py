from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import time
import asyncio

from .config import settings
from .core.database import create_pool, close_pool
from .core.redis import create_redis_client, close_redis_client
from .core.logger import setup_logging
from .core.http_client import close_http_client


class SecurityHeadersMiddleware:
    """Add security headers to all responses (pure ASGI, avoids BaseHTTPMiddleware CORS bug)"""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                extra_headers = [
                    (b"x-content-type-options", b"nosniff"),
                    (b"x-frame-options", b"DENY"),
                    (b"x-xss-protection", b"1; mode=block"),
                    (b"referrer-policy", b"strict-origin-when-cross-origin"),
                ]
                if not settings.debug:
                    extra_headers.append(
                        (b"strict-transport-security", b"max-age=31536000; includeSubDomains")
                    )
                message["headers"] = list(message.get("headers", [])) + extra_headers
            await send(message)

        await self.app(scope, receive, send_wrapper)

# Setup logging first
setup_logging()
logger = logging.getLogger(__name__)


def log_step(step_name: str, start_time: float) -> float:
    """Log step completion with timing"""
    elapsed = time.time() - start_time
    logger.info(f"[STARTUP] {step_name} completed in {elapsed:.2f}s")
    return time.time()


async def _safe_background_task(coro_func, *args, name="task", restart_delay=60):
    """Wrapper that restarts background tasks on crash. For long-running tasks only."""
    while True:
        try:
            await coro_func(*args)
            break  # If coroutine completes normally, exit
        except asyncio.CancelledError:
            logger.info(f"[BG] {name} cancelled")
            break
        except Exception as e:
            logger.error(f"[BG] {name} crashed: {e}, restarting in {restart_delay}s")
            await asyncio.sleep(restart_delay)


async def verify_playwright_background():
    """Verify Playwright in background (non-blocking)"""
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            await browser.close()
        logger.info("[STARTUP] ✅ Playwright chromium verified (background)")
    except Exception as e:
        logger.warning(f"[STARTUP] ⚠️ Playwright verification failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for FastAPI app"""
    total_start = time.time()

    # Startup
    logger.info(f"[STARTUP] Starting {settings.app_name} v{settings.app_version}")
    logger.info(f"[STARTUP] Environment: Railway, Debug: {settings.debug}")

    try:
        step_start = time.time()

        # Initialize database pool
        logger.info("[STARTUP] Connecting to database...")
        await create_pool()
        step_start = log_step("Database pool", step_start)

        # Initialize Redis client
        logger.info("[STARTUP] Connecting to Redis...")
        await create_redis_client()
        step_start = log_step("Redis client", step_start)

        # Start Playwright verification in background (don't block startup)
        logger.info("[STARTUP] Starting Playwright verification in background...")
        asyncio.create_task(verify_playwright_background())

        # Load legal documents into RAG system in background
        logger.info("[STARTUP] Starting legal docs loader in background...")
        from .services.legal_docs_loader import load_legal_docs_background
        from .core.database import get_db_pool
        pool = await get_db_pool()
        asyncio.create_task(load_legal_docs_background(pool))

        # Start periodic orders sync in background (every 60 min, auto-restart on crash)
        logger.info("[STARTUP] Starting periodic orders sync in background...")
        from .services.orders_sync_service import periodic_orders_sync
        asyncio.create_task(_safe_background_task(
            periodic_orders_sync, pool, name="orders_sync", restart_delay=60
        ))

        total_elapsed = time.time() - total_start
        logger.info(f"[STARTUP] ✅ Application ready in {total_elapsed:.2f}s")

    except Exception as e:
        logger.error(f"[STARTUP] ❌ Failed to initialize: {e}")
        raise

    yield

    # Shutdown
    logger.info("[SHUTDOWN] Shutting down application...")
    await close_http_client()
    await close_pool()
    await close_redis_client()
    logger.info("[SHUTDOWN] Application shutdown complete")


# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    debug=settings.debug,
    lifespan=lifespan,
    docs_url="/docs" if settings.debug else None,
    redoc_url="/redoc" if settings.debug else None,
)

# Security headers middleware
app.add_middleware(SecurityHeadersMiddleware)

# CORS middleware - production-ready configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "Accept"],
    max_age=3600,  # Cache preflight requests for 1 hour
)


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint with dependency validation"""
    from .core.database import get_db_pool
    from .core.redis import get_redis

    checks = {
        "database": "unknown",
        "redis": "unknown",
    }

    # Check database
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        checks["database"] = "healthy"
    except Exception as e:
        checks["database"] = f"unhealthy: {str(e)[:50]}"

    # Check Redis
    try:
        redis = await get_redis()
        await redis.ping()
        checks["redis"] = "healthy"
    except Exception as e:
        checks["redis"] = f"unhealthy: {str(e)[:50]}"

    # Overall status
    all_healthy = all(v == "healthy" for v in checks.values())

    return {
        "status": "healthy" if all_healthy else "degraded",
        "app": settings.app_name,
        "version": settings.app_version,
        "checks": checks,
    }


@app.get("/health/circuits")
async def circuit_breaker_health():
    """Circuit breaker status endpoint for monitoring"""
    from .core.circuit_breaker import get_all_circuit_breakers

    breakers = get_all_circuit_breakers()
    return {
        "circuits": {
            name: cb.get_stats() for name, cb in breakers.items()
        }
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
from .routers import auth, kaspi, preorders, whatsapp, ai, billing, admin, invoices, partner_auth, lawyer, health, support, unit_economics, referral, notifications, niches

app.include_router(auth.router, prefix="/auth", tags=["Authentication"])
app.include_router(kaspi.router, prefix="/kaspi", tags=["Kaspi"])
app.include_router(preorders.router, prefix="/preorders", tags=["Preorders"])
app.include_router(whatsapp.router, prefix="/whatsapp", tags=["WhatsApp"])
app.include_router(ai.router, prefix="/ai", tags=["AI Assistants"])
app.include_router(lawyer.router, prefix="/ai/lawyer", tags=["AI Lawyer"])
app.include_router(billing.router, prefix="/billing", tags=["Billing"])
app.include_router(admin.router, prefix="/admin", tags=["Admin"])
app.include_router(invoices.router, prefix="/invoices", tags=["Invoices"])
app.include_router(partner_auth.router, prefix="/partner", tags=["Partner Auth"])
app.include_router(support.router, prefix="/support", tags=["Support Chat"])
app.include_router(health.router, prefix="/health", tags=["Health & Monitoring"])
app.include_router(unit_economics.router, prefix="/unit-economics", tags=["Unit Economics"])
app.include_router(referral.router, prefix="/referral", tags=["Referral Program"])
app.include_router(notifications.router, prefix="/notifications", tags=["Notifications"])
app.include_router(niches.router, prefix="/niches", tags=["Niche Search"])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
    )
