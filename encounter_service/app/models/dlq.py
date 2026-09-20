from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


class DLQMessageRecord(BaseModel):
    """Persistence model for failed Kafka summarization messages."""
    id: Optional[str] = None
    encounter_id: str
    pId: str
    version: int
    payload: Dict[str, Any]
    error_message: str
    retry_attempts: int = 3
    status: str = Field(default="pending", description="'pending', 'resolved', 'discarded'")
    failed_at: datetime = Field(default_factory=datetime.utcnow)
    resolved_at: Optional[datetime] = None
    resolution_notes: Optional[str] = None


class DLQStatsResponse(BaseModel):
    """DLQ operational health metrics."""
    total_dlq_messages: int
    pending_count: int
    resolved_count: int
    discarded_count: int
    top_error_reasons: List[Dict[str, Any]]
