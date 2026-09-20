import random
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class StreamSimulationConfig(BaseModel):
    """Configuration options for synthetic event stream generation."""
    num_encounters: int = Field(default=5, description="Number of distinct encounters to simulate")
    events_per_encounter: int = Field(default=3, description="Versions per encounter")
    duplicate_probability: float = Field(default=0.2, ge=0.0, le=1.0, description="Chance to emit an exact duplicate event_id")
    out_of_order_probability: float = Field(default=0.25, ge=0.0, le=1.0, description="Chance to shuffle versions out of order")
    delay_ms: int = Field(default=200, description="Delay between events in milliseconds")


class EventGenerator:
    """
    Generator for synthetic partner encounter updates.
    Simulates real-world conditions: duplicates, out-of-order versions, and varied encounter types.
    """

    @staticmethod
    def generate_single_event(
        encounter_id: str,
        pId: str,
        version: int,
        encounter_type: str = "triage",
        event_id: Optional[str] = None
    ) -> Dict[str, Any]:
        templates = {
            "triage": [
                "Patient presenting with severe headache and fever.",
                "Vitals taken: BP 130/85, Temp 101.2F. Acetaminophen given.",
                "Symptoms reduced. Patient discharged with follow-up instructions."
            ],
            "medication_refill": [
                "Requesting 90-day refill for Metformin 500mg.",
                "Doctor reviewed lab records (HbA1c 6.8). Approved refill.",
                "Prescription sent to local pharmacy."
            ],
            "appointment_booking": [
                "Requesting appointment with cardiology specialist.",
                "Cardiologist slot booked for next Tuesday at 10:00 AM.",
                "Patient confirmed appointment via SMS."
            ]
        }
        notes = templates.get(encounter_type, templates["triage"])
        note_idx = min(version - 1, len(notes) - 1)

        return {
            "event_id": event_id or f"evt-{encounter_id}-v{version}",
            "encounter_id": encounter_id,
            "pId": pId,
            "version": version,
            "payload": {
                "transcriptions": notes[note_idx]
            }
        }

    @staticmethod
    def generate_event_stream(config: StreamSimulationConfig) -> List[Dict[str, Any]]:
        events = []
        types = ["triage", "medication_refill", "appointment_booking"]
        for i in range(config.num_encounters):
            enc_id = f"enc-{100 + i}"
            pid = f"pat-{100 + i}"
            enc_type = types[i % len(types)]
            
            # Generate versions 1..N
            for ver in range(1, config.events_per_encounter + 1):
                evt_id = f"evt-{enc_id}-v{ver}"
                event = EventGenerator.generate_single_event(
                    encounter_id=enc_id,
                    pId=pid,
                    version=ver,
                    encounter_type=enc_type,
                    event_id=evt_id
                )
                events.append(event)
                
                # Simulate duplicate
                if random.random() < config.duplicate_probability:
                    events.append(event.copy())

        # Simulate out-of-order arrival by shuffling a percentage of events
        if config.out_of_order_probability > 0:
            # Swap adjacent elements randomly to produce out-of-order versions
            for k in range(len(events) - 1):
                if random.random() < config.out_of_order_probability:
                    events[k], events[k + 1] = events[k + 1], events[k]

        return events
