from fastapi import FastAPI
from .config import settings
from .api.streamer_routes import router as streamer_router

app = FastAPI(
    title="Partner Event Streamer Service",
    description="Microservice to simulate partner streaming encounter update events with out-of-order versions and duplicates.",
    version="1.0.0"
)

app.include_router(streamer_router)


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint for container probes."""
    return {"status": "ok", "service": settings.SERVICE_NAME}
