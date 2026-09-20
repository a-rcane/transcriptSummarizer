from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field
from .summary import MedicationItem


class Patient(BaseModel):
    id: str
    name: str
    number: Optional[str] = None


class Nurse(BaseModel):
    id: str
    name: str
    number: Optional[str] = None


class PatientHistory(BaseModel):
    """
    Patient longitudinal history record with Optimistic Concurrency Control (version).
    """
    pId: str
    nId: Optional[str] = None
    complete_summary: str = ""
    last_encounter_summary: Optional[str] = None
    active_problems: List[str] = Field(default_factory=list)
    current_medications: List[MedicationItem] = Field(default_factory=list)
    version: int = Field(default=1, description="OCC version number for race-condition prevention")
    last_visit: Optional[datetime] = None
    updated_at: datetime = Field(default_factory=datetime.utcnow)
