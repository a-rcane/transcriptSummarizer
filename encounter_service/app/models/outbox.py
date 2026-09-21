from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class OutboxRecord(BaseModel):
    """
    Persistence model for Transactional Outbox Pattern.
    Guarantees at-least-once delivery to Kafka during process crashes.
    """
    id: str = Field(..., alias="_id", description="Unique outbox record ID e.g. enc-101-v13")
    encounter_id: str
    pId: str
    version: int
    payload: Dict[str, Any]
    status: str = Field(default="PENDING_DISPATCH", description="'PENDING_DISPATCH', 'DISPATCHED', 'FAILED'")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    dispatched_at: Optional[datetime] = None
    retry_count: int = 0
    last_error: Optional[str] = None

    class Config:
        populate_by_name = True
