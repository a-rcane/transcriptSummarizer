from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class EncounterPayload(BaseModel):
    """Payload containing raw clinical transcriptions or notes."""
    transcriptions: str = Field(..., description="Updated encounter transcription notes")


class EncounterEventInput(BaseModel):
    """
    Inbound partner event schema representing encounter updates.
    Events may arrive out-of-order or duplicate.
    """
    event_id: str = Field(..., description="Unique event identifier for deduplication (e.g. evt-102)")
    encounter_id: str = Field(..., description="Encounter identifier (e.g. enc-42)")
    pId: str = Field(..., description="Patient identifier")
    version: int = Field(..., ge=1, description="Monotonically increasing version number")
    payload: EncounterPayload = Field(..., description="Event payload containing transcriptions")
    received_at: Optional[datetime] = Field(default_factory=datetime.utcnow, description="Timestamp when received")


class IngestResponse(BaseModel):
    """Response returned upon webhook ingestion."""
    status: str = Field(..., description="Status: 'accepted', 'ignored_duplicate', 'ignored_stale_version'")
    message: str = Field(...)
    event_id: str
    encounter_id: str
    version: int
    summarization_triggered: bool = False
