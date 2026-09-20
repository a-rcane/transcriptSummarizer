from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict


class FHIRCoding(BaseModel):
    system: Optional[str] = "http://snomed.info/sct"
    code: Optional[str] = None
    display: str


class FHIRCodeableConcept(BaseModel):
    coding: Optional[List[FHIRCoding]] = None
    text: str


class FHIRReference(BaseModel):
    reference: str  # e.g., "Patient/pat-101"
    display: Optional[str] = None


class FHIRPeriod(BaseModel):
    start: datetime
    end: Optional[datetime] = None


class FHIREncounter(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    resourceType: str = "Encounter"
    id: str
    status: str = "finished"
    class_: Dict[str, str] = Field(
        default={"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "AMB", "display": "ambulatory"},
        alias="class"
    )
    subject: FHIRReference
    period: FHIRPeriod
    text: Optional[Dict[str, str]] = None  # Clinical summary narrative


class FHIRCondition(BaseModel):
    resourceType: str = "Condition"
    id: str
    clinicalStatus: Dict[str, Any] = {
        "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "active"}]
    }
    verificationStatus: Dict[str, Any] = {
        "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-ver-status", "code": "confirmed"}]
    }
    code: FHIRCodeableConcept
    subject: FHIRReference
    recordedDate: datetime = Field(default_factory=datetime.utcnow)


class FHIRMedicationRequest(BaseModel):
    resourceType: str = "MedicationRequest"
    id: str
    status: str = "active"
    intent: str = "order"
    medicationCodeableConcept: FHIRCodeableConcept
    subject: FHIRReference
    authoredOn: datetime = Field(default_factory=datetime.utcnow)
    dosageInstruction: Optional[List[Dict[str, Any]]] = None


class FHIRBundle(BaseModel):
    resourceType: str = "Bundle"
    type: str = "collection"
    total: int
    entry: List[Dict[str, Any]]
