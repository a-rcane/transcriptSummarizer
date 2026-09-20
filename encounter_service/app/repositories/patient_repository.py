from datetime import datetime
from typing import Optional, List
from motor.motor_asyncio import AsyncIOMotorDatabase
from ..models.patient import Patient, PatientHistory


class PatientRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.history_collection = db["patient_histories"]
        self.patients_collection = db["patients"]

    async def get_patient_history_by_pid(self, pId: str) -> Optional[PatientHistory]:
        doc = await self.history_collection.find_one({"pId": pId})
        if doc:
            return PatientHistory(**doc)
        return None

    async def upsert_patient_history_occ(self, history: PatientHistory, max_retries: int = 3) -> bool:
        """
        Optimistic Concurrency Control (OCC):
        Updates patient history only if the version in DB matches the version we read.
        Prevents race conditions across parallel workers.
        """
        for _ in range(max_retries):
            current = await self.get_patient_history_by_pid(history.pId)
            
            if current is None:
                # Insert first time
                history.version = 1
                try:
                    await self.history_collection.insert_one(history.model_dump())
                    return True
                except Exception:
                    continue  # Retry on duplicate race condition

            # Merge medications by name (case-insensitive deduplication)
            med_dict = {}
            for m in current.current_medications:
                m_data = m.model_dump() if hasattr(m, "model_dump") else (m if isinstance(m, dict) else dict(m))
                name = m_data.get("name", "").strip().lower()
                if name:
                    med_dict[name] = m_data

            for m in history.current_medications:
                m_data = m.model_dump() if hasattr(m, "model_dump") else (m if isinstance(m, dict) else dict(m))
                name = m_data.get("name", "").strip().lower()
                if name:
                    med_dict[name] = m_data

            merged_medications = list(med_dict.values())
            merged_problems = list(dict.fromkeys(current.active_problems + history.active_problems))

            # Atomic conditional update on version
            result = await self.history_collection.update_one(
                {"pId": history.pId, "version": current.version},
                {
                    "$set": {
                        "nId": history.nId or current.nId,
                        "complete_summary": history.complete_summary,
                        "last_encounter_summary": history.last_encounter_summary,
                        "active_problems": merged_problems,
                        "current_medications": merged_medications,
                        "last_visit": history.last_visit or current.last_visit,
                        "updated_at": datetime.utcnow()
                    },
                    "$inc": {"version": 1}
                }
            )
            if result.modified_count > 0:
                return True

        return False
