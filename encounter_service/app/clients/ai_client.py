import json
from datetime import datetime
from typing import Optional
from litellm import acompletion
from ..config import settings
from ..models.encounter import EncounterType, UrgencyLevel
from ..models.summary import SummaryTemplate, MedicationItem, VitalSigns, ConfidenceBreakdown


class UnifiedAIClient:
    def __init__(self, model: str = settings.AI_MODEL, api_base: Optional[str] = settings.AI_API_BASE):
        self.model = model
        self.api_base = api_base

    async def generate_summary(
        self,
        new_transcription: str,
        patient_history_summary: Optional[str] = None
    ) -> SummaryTemplate:
        prompt = f"""You are an expert clinical AI assistant.
Analyze the clinical transcription and update the patient's longitudinal record.

Return ONLY a valid JSON object matching this exact JSON schema:
{{
  "encounter_type": "triage" | "appointment_booking" | "medication_refill" | "other",
  "urgency_level": "critical" | "urgent" | "routine",
  "encounter_summary": "Concise 2-3 sentence summary of this encounter",
  "active_problems": ["List of identified medical conditions or symptoms"],
  "medications": [
    {{"name": "string", "dose": "string", "action": "prescribed|refilled|discontinued|continued"}}
  ],
  "vital_signs": {{
    "blood_pressure": "string or null",
    "heart_rate": null,
    "temperature": null,
    "respiratory_rate": null,
    "oxygen_saturation": null
  }},
  "confidence_breakdown": {{
    "overall_score": 0.95,
    "extraction_confidence": 0.95,
    "diagnosis_confidence": 0.90,
    "ambiguity_risk": 0.05
  }},
  "confidence_score": 0.95,
  "needs_human_review": false
}}

Urgency Guidelines:
- "critical": Acute chest pain, respiratory distress, stroke symptoms, unstable vitals.
- "urgent": High fever, acute severe headache, active infections, moderate distress.
- "routine": Medication refills, routine follow-ups, appointment bookings.

Rule: Set `needs_human_review` to true if confidence_score is below 0.75, ambiguity_risk > 0.35, or notes are ambiguous.

Longitudinal Patient History:
{patient_history_summary or 'No prior records.'}

New Encounter Notes:
{new_transcription}
"""
        try:
            response = await acompletion(
                model=self.model,
                api_base=self.api_base,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )

            content = response.choices[0].message.content
            clean_content = content.strip()
            if clean_content.startswith("```"):
                lines = clean_content.splitlines()
                if lines[0].startswith("```"):
                    lines = lines[1:]
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                clean_content = "\n".join(lines).strip()

            data = json.loads(clean_content)

            # Parse medications
            raw_meds = data.get("medications", [])
            meds = [MedicationItem(**m) for m in raw_meds if isinstance(m, dict)]

            # Parse vitals
            raw_vitals = data.get("vital_signs")
            vitals = VitalSigns(**raw_vitals) if isinstance(raw_vitals, dict) else None

            # Parse encounter type safely
            try:
                enc_type = EncounterType(data.get("encounter_type", "other").lower())
            except ValueError:
                enc_type = EncounterType.OTHER

            # Parse urgency level safely
            try:
                urgency = UrgencyLevel(data.get("urgency_level", "routine").lower())
            except ValueError:
                urgency = UrgencyLevel.ROUTINE

            # Parse confidence breakdown
            raw_breakdown = data.get("confidence_breakdown", {})
            if isinstance(raw_breakdown, dict):
                breakdown = ConfidenceBreakdown(
                    overall_score=float(raw_breakdown.get("overall_score", data.get("confidence_score", 0.9))),
                    extraction_confidence=float(raw_breakdown.get("extraction_confidence", 0.9)),
                    diagnosis_confidence=float(raw_breakdown.get("diagnosis_confidence", 0.9)),
                    ambiguity_risk=float(raw_breakdown.get("ambiguity_risk", 0.1))
                )
            else:
                conf = float(data.get("confidence_score", 0.9))
                breakdown = ConfidenceBreakdown(overall_score=conf, extraction_confidence=conf, diagnosis_confidence=conf, ambiguity_risk=0.1)

            confidence = breakdown.overall_score
            needs_review = data.get("needs_human_review", confidence < 0.75 or breakdown.ambiguity_risk > 0.35)

            return SummaryTemplate(
                encounter_type=enc_type,
                urgency_level=urgency,
                encounter_summary=data.get("encounter_summary", new_transcription),
                active_problems=data.get("active_problems", []),
                medications=meds,
                vital_signs=vitals,
                confidence_score=confidence,
                confidence_breakdown=breakdown,
                needs_human_review=needs_review,
                last_visit=datetime.utcnow()
            )
        except Exception as e:
            print(f"[AI Client Warning] LLM inference failed ({e}). Using clinical heuristic extractor fallback.")
            return self._heuristic_clinical_extraction(new_transcription)

    def _heuristic_clinical_extraction(self, transcription: str) -> SummaryTemplate:
        """Rule-based clinical extraction fallback when LLM service is offline."""
        import re
        text_lower = transcription.lower()

        # 1. Encounter type detection
        if any(w in text_lower for w in ["refill", "medication", "pharmacy", "prescription"]):
            enc_type = EncounterType.MEDICATION_REFILL
        elif any(w in text_lower for w in ["appointment", "booking", "schedule", "scheduled", "follow-up"]):
            enc_type = EncounterType.APPOINTMENT_BOOKING
        elif any(w in text_lower for w in ["triage", "emergency", "acute", "chest pain", "shortness of breath", "fever", "headache"]):
            enc_type = EncounterType.TRIAGE
        else:
            enc_type = EncounterType.OTHER

        # 2. Extract vital signs
        bp_match = re.search(r'(?:bp|blood pressure)[:\s]*(\d{2,3}/\d{2,3})', transcription, re.I)
        hr_match = re.search(r'(?:hr|heart rate)[:\s]*(\d{2,3})', transcription, re.I)
        temp_match = re.search(r'(?:temp|temperature)[:\s]*(\d{2,3}(?:\.\d)?\s*°?[fFcC]?)', transcription, re.I)
        o2_match = re.search(r'(?:o2|spo2|oxygen)[:\s]*(\d{2,3})%?', transcription, re.I)

        vitals = None
        if bp_match or hr_match or temp_match or o2_match:
            vitals = VitalSigns(
                blood_pressure=bp_match.group(1) if bp_match else None,
                heart_rate=int(hr_match.group(1)) if hr_match else None,
                temperature=float(re.sub(r'[^\d.]', '', temp_match.group(1))) if temp_match else None,
                oxygen_saturation=o2_match.group(1) if o2_match else None
            )

        # 3. Urgency level classification
        if any(w in text_lower for w in ["acute chest", "shortness of breath", "troponin", "dyspnea", "syncope", "stroke"]) or (vitals and vitals.heart_rate and vitals.heart_rate > 120):
            urgency = UrgencyLevel.CRITICAL
        elif any(w in text_lower for w in ["fever", "headache", "infection", "pain", "tachycardia"]):
            urgency = UrgencyLevel.URGENT
        else:
            urgency = UrgencyLevel.ROUTINE

        # 4. Extract common clinical medications
        med_patterns = [
            (r'(metformin(?:\s+\d+mg)?)', "Metformin", "500mg"),
            (r'(lisinopril(?:\s+\d+mg)?)', "Lisinopril", "10mg"),
            (r'(acetaminophen(?:\s+\d+mg)?)', "Acetaminophen", "500mg"),
            (r'(nitroglycerin(?:\s+\w+)?)', "Nitroglycerin", "0.4mg SL"),
            (r'(cetirizine(?:\s+\d+mg)?)', "Cetirizine", "10mg"),
            (r'(aspirin|asa(?:\s+\d+mg)?)', "Aspirin", "81mg"),
            (r'(atorvastatin(?:\s+\d+mg)?)', "Atorvastatin", "20mg"),
        ]
        extracted_meds = []
        for pat, default_name, default_dose in med_patterns:
            if re.search(pat, text_lower):
                action = "refilled" if enc_type == EncounterType.MEDICATION_REFILL else "prescribed"
                extracted_meds.append(MedicationItem(name=default_name, dose=default_dose, action=action))

        # 5. Extract active conditions / problems
        conditions = []
        problem_keywords = [
            ("chest discomfort", "Acute chest discomfort"),
            ("shortness of breath", "Dyspnea"),
            ("tachycardia", "Sinus tachycardia"),
            ("fever", "Fever"),
            ("headache", "Cephalgia"),
            ("allergy", "Seasonal allergies"),
            ("hypertension", "Hypertension"),
            ("diabetes", "Type 2 Diabetes Mellitus")
        ]
        for kw, diag in problem_keywords:
            if kw in text_lower:
                conditions.append(diag)

        # 6. Multi-dimensional confidence breakdown
        extraction_conf = 0.85 if (vitals or extracted_meds) else 0.70
        diagnosis_conf = 0.85 if conditions else 0.65
        ambiguity = 0.25 if urgency == UrgencyLevel.CRITICAL else 0.10
        overall = round((extraction_conf * 0.4) + (diagnosis_conf * 0.4) + ((1.0 - ambiguity) * 0.2), 2)

        breakdown = ConfidenceBreakdown(
            overall_score=overall,
            extraction_confidence=extraction_conf,
            diagnosis_confidence=diagnosis_conf,
            ambiguity_risk=ambiguity
        )

        summary = f"Clinical Note ({enc_type.value}): {transcription.strip()}"
        if len(summary) > 200:
            summary = summary[:197] + "..."

        return SummaryTemplate(
            encounter_type=enc_type,
            urgency_level=urgency,
            encounter_summary=summary,
            active_problems=conditions,
            medications=extracted_meds,
            vital_signs=vitals,
            confidence_score=overall,
            confidence_breakdown=breakdown,
            needs_human_review=(overall < 0.75 or urgency == UrgencyLevel.CRITICAL),
            last_visit=datetime.utcnow()
        )

    async def stream_summary_tokens(
        self,
        new_transcription: str,
        patient_history_summary: Optional[str] = None
    ):
        """Streams LLM tokens chunk by chunk as generated by Ollama/Gemini."""
        prompt = f"""You are a clinical AI assistant. Summarize the following encounter note concisely.
Patient History: {patient_history_summary or 'None'}
Encounter Notes: {new_transcription}
"""
        response = await acompletion(
            model=self.model,
            api_base=self.api_base,
            messages=[{"role": "user", "content": prompt}],
            stream=True  # Enables token streaming
        )
        async for chunk in response:
            delta = chunk.choices[0].delta.content
            if delta:
                yield delta