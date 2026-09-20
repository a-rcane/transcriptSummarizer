from fastapi import APIRouter
from .webhook import router as webhook_router
from .summary import router as summary_router

api_v1_router = APIRouter(prefix="/v1")
api_v1_router.include_router(webhook_router, tags=["Webhook"])
api_v1_router.include_router(summary_router, tags=["Summary"])

__all__ = ["api_v1_router"]
