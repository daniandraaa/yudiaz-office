"""Main entry point for Yudiaz Virtual HQ FastAPI server.

Initializes FastAPI application, lifecycle simulation hooks, CORS middleware,
REST and SSE routing, and static frontend assets mounting.
"""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from backend.config import get_settings
from backend.office_engine import office_engine
from backend.routes import health_check, router as api_router

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application startup and shutdown lifecycle."""
    # Startup: trigger autonomous simulation background worker
    await office_engine.start_simulation()
    try:
        yield
    finally:
        # Shutdown: gracefully cancel simulation worker and close streams
        await office_engine.stop_simulation()


# Initialize FastAPI instance
app = FastAPI(
    title=settings.title,
    description=(
        "Spatial Cyber-Luxury Virtual Headquarters & Autonomous Multi-Agent Command Center "
        "for Yudiaz Creative Studio."
    ),
    version=settings.version,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

# Configure CORS for permissive frontend connectivity
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Root-level health endpoint (aliases /api/v1/health)
@app.get("/health", include_in_schema=False)
async def root_health():
    """Top-level health check alias."""
    return await health_check()


# Register core API routes with /api/v1 prefix
app.include_router(api_router)

# Mount frontend static directory at root
FRONTEND_DIR = Path("/home/daniilham/yudiaz-office/frontend")
if FRONTEND_DIR.is_dir():
    app.mount(
        "/",
        StaticFiles(directory=str(FRONTEND_DIR), html=True),
        name="frontend",
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "backend.main:app",
        host=settings.host,
        port=settings.port,
        reload=False,
    )
