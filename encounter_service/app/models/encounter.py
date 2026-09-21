from datetime import datetime
from enum import Enum
from typing import Optional, List, Any, Dict
from pydantic import BaseModel, Field


class EncounterType(str, Enum):
    """Supported encounter classifications."""
    TRIAGE = "triage"
    APPOINTMENT_BOOKING = "appointment_booking"
    MEDICATION_REFILL = "medication_refill"
    OTHER = "other"


class UrgencyLevel(str, Enum):
    """Clinical triage urgency priority."""
    CRITICAL = "critical"  # Acute emergency, unstable vitals, severe chest pain
    URGENT = "urgent"      # Moderate distress, high fever, acute infection
    ROUTINE = "routine"    # Routine medication refills, checkups, bookings


class ReviewFeedbackRecord(BaseModel):
    """Audit log of clinician reviews and diff tracking against AI summaries."""
    encounter_id: str
    pId: str
    original_ai_summary: Optional[str] = None
    revised_summary: str
    reviewed_by: str
    reviewed_at: datetime = Field(default_factory=datetime.utcnow)
    is_modified: bool = False
    diff_summary: Optional[str] = None
    original_confidence: Optional[float] = None
    revised_encounter_type: Optional[EncounterType] = None


class EncounterEntity(BaseModel):
    """
    Persistence model for latest encounter state.
    Stored in MongoDB `encounters` collection.
    """
    encounter_id: str = Field(..., description="Unique Encounter ID")
    pId: str = Field(..., description="Patient ID")
    latest_version: int = Field(..., description="Highest version number processed so far")
    summary_version: int = Field(default=0, description="Highest version number whose summary has been applied")
    transcriptions: str = Field(..., description="Latest transcription content")
    encounter_type: Optional[EncounterType] = None
    summary: Optional[str] = None
    ai_generated_summary: Optional[str] = None
    confidence_score: Optional[float] = 1.0
    confidence_breakdown: Optional[Dict[str, float]] = None
    urgency_level: UrgencyLevel = UrgencyLevel.ROUTINE
    needs_human_review: bool = False
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    review_history: List[ReviewFeedbackRecord] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class EncounterVersionLog(BaseModel):
    """
    Persistence model for immutable raw event log / secondary store.
    Stored in MongoDB `encounter_events` collection with composite key (encounter_id, version).
    """
    event_id: str
    encounter_id: str
    pId: str
    version: int
    transcription: str
    summary_status: str = Field(default="PENDING", description="'PENDING', 'SUMMARIZED', 'OBSOLETE', 'FAILED', 'SLA_BREACH_RETRYING'")
    summary_text: Optional[str] = None
    received_at: datetime = Field(default_factory=datetime.utcnow)
    processed_at: Optional[datetime] = None
