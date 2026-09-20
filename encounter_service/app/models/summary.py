from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from .encounter import EncounterType, UrgencyLevel


class MedicationItem(BaseModel):
    name: str = Field(..., description="Medication name e.g. Metformin")
    dose: Optional[str] = Field(None, description="Dosage e.g. 500mg daily")
    action: str = Field(default="prescribed", description="prescribed, refilled, discontinued, or continued")


class VitalSigns(BaseModel):
    blood_pressure: Optional[str] = None
    heart_rate: Optional[int] = None
    temperature: Optional[float] = None
    respiratory_rate: Optional[int] = None
    oxygen_saturation: Optional[str] = None


class ConfidenceBreakdown(BaseModel):
    """Granular multi-dimensional clinical AI confidence metrics."""
    overall_score: float = Field(default=1.0, ge=0.0, le=1.0, description="Overall weighted confidence (0.0 to 1.0)")
    extraction_confidence: float = Field(default=1.0, ge=0.0, le=1.0, description="Confidence in vital signs & medication extraction")
    diagnosis_confidence: float = Field(default=1.0, ge=0.0, le=1.0, description="Certainty in identified active conditions")
    ambiguity_risk: float = Field(default=0.0, ge=0.0, le=1.0, description="Risk score for ambiguous/contradictory notes (0.0 to 1.0)")


class SummaryTemplate(BaseModel):
    """Structured clinical extraction output from LLM."""
    encounter_type: EncounterType = Field(..., description="Classified type of encounter")
    encounter_summary: str = Field(..., description="Concise summary of this encounter")
    active_problems: List[str] = Field(default_factory=list, description="Extracted diagnoses/conditions e.g. Hypertension")
    medications: List[MedicationItem] = Field(default_factory=list, description="Extracted medications & dosage")
    vital_signs: Optional[VitalSigns] = None
    confidence_score: float = Field(default=1.0, ge=0.0, le=1.0, description="Model self-reported confidence (0.0 to 1.0)")
    confidence_breakdown: Optional[ConfidenceBreakdown] = None
    urgency_level: UrgencyLevel = Field(default=UrgencyLevel.ROUTINE, description="Triage urgency classification")
    needs_human_review: bool = Field(default=False, description="Flagged for physician review if confidence < 0.75")
    last_visit: Optional[datetime] = None


class SummaryOutput(BaseModel):
    """API Output schema."""
    pId: str
    nId: Optional[str] = None
    encounter_id: Optional[str] = None
    complete_summary: str
    last_encounter_summary: Optional[str] = None
    ai_generated_summary: Optional[str] = None
    encounter_type: Optional[EncounterType] = None
    urgency_level: UrgencyLevel = UrgencyLevel.ROUTINE
    active_problems: List[str] = []
    current_medications: List[MedicationItem] = []
    confidence_score: float = 1.0
    confidence_breakdown: Optional[ConfidenceBreakdown] = None
    needs_human_review: bool = False
    is_reviewed: bool = False
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    last_updated: datetime = Field(default_factory=datetime.utcnow)


class HumanOverrideRequest(BaseModel):
    """Request schema for physicians/nurses overriding AI summaries."""
    revised_summary: str
    revised_encounter_type: Optional[EncounterType] = None
    reviewed_by: str = Field(..., description="Clinician ID or Name")


class ReconciliationResponse(BaseModel):
    """Response returned when an encounter history is manually replayed and reconciled."""
    status: str = "reconciled"
    encounter_id: str
    pId: str
    total_versions_replayed: int
    versions_replayed: List[int]
    reconciled_summary: SummaryOutput
    reconciled_at: datetime = Field(default_factory=datetime.utcnow)


class FeedbackMetricsResponse(BaseModel):
    """Aggregated clinical feedback and model accuracy metrics."""
    total_reviewed_encounters: int
    modified_by_clinician_count: int
    approved_unmodified_count: int
    clinician_modification_rate_pct: float
    avg_original_confidence: float
    avg_extraction_confidence: float
    avg_diagnosis_confidence: float
