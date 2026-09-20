from datetime import datetime
from typing import Dict, Any
from ..models.summary import SummaryOutput
from ..models.fhir import (
    FHIRBundle, FHIREncounter, FHIRCondition, 
    FHIRMedicationRequest, FHIRReference, FHIRCodeableConcept, FHIRPeriod
)


class FHIRConverter:
    """Converts internal Clinical Summaries into standard HL7 FHIR R4 Bundle."""

    @staticmethod
    def to_fhir_bundle(summary: SummaryOutput) -> Dict[str, Any]:
        patient_ref = FHIRReference(reference=f"Patient/{summary.pId}", display=f"Patient {summary.pId}")
        entries = []

        # 1. FHIR Encounter Resource
        fhir_encounter = FHIREncounter(
            id=summary.encounter_id or f"enc-{summary.pId}",
            subject=patient_ref,
            period=FHIRPeriod(start=summary.last_updated),
            text={
                "status": "generated",
                "div": f"<div xmlns=\"http://www.w3.org/1999/xhtml\">{summary.last_encounter_summary or summary.complete_summary}</div>"
            }
        )
        entries.append({"resource": fhir_encounter.model_dump(by_alias=True, mode="json")})

        # 2. FHIR Condition Resources (Active Problems)
        for idx, problem in enumerate(summary.active_problems):
            condition = FHIRCondition(
                id=f"cond-{summary.encounter_id or 'p'}-{idx+1}",
                code=FHIRCodeableConcept(text=problem),
                subject=patient_ref,
                recordedDate=summary.last_updated
            )
            entries.append({"resource": condition.model_dump(mode="json")})

        # 3. FHIR MedicationRequest Resources
        for idx, med in enumerate(summary.current_medications):
            dosage = [{"text": med.dose}] if med.dose else None
            med_req = FHIRMedicationRequest(
                id=f"med-{summary.encounter_id or 'p'}-{idx+1}",
                medicationCodeableConcept=FHIRCodeableConcept(text=med.name),
                subject=patient_ref,
                authoredOn=summary.last_updated,
                dosageInstruction=dosage
            )
            entries.append({"resource": med_req.model_dump(mode="json")})

        bundle = FHIRBundle(
            total=len(entries),
            entry=entries
        )
        return bundle.model_dump(mode="json")
