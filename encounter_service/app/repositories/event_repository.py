from datetime import datetime, timedelta
from typing import Optional, List
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import DuplicateKeyError
from ..models.encounter import EncounterVersionLog


class EventRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.collection = db["encounter_events"]

    async def event_exists(self, event_id: str) -> bool:
        doc = await self.collection.find_one({"event_id": event_id}, {"_id": 1})
        return doc is not None

    async def version_exists(self, encounter_id: str, version: int) -> bool:
        doc = await self.collection.find_one(
            {"encounter_id": encounter_id, "version": version}, 
            {"_id": 1}
        )
        return doc is not None

    async def save_event_log(self, log_entry: EncounterVersionLog) -> bool:
        try:
            await self.collection.insert_one(log_entry.model_dump())
            return True
        except DuplicateKeyError:
            return False

    async def get_all_versions_for_encounter(self, encounter_id: str) -> List[EncounterVersionLog]:
        cursor = self.collection.find({"encounter_id": encounter_id}).sort("version", 1)
        events = []
        async for doc in cursor:
            events.append(EncounterVersionLog(**doc))
        return events

    async def get_event_by_version(self, encounter_id: str, version: int) -> Optional[EncounterVersionLog]:
        """Fetches the exact-version event record and its summary status."""
        doc = await self.collection.find_one({"encounter_id": encounter_id, "version": version})
        return EncounterVersionLog(**doc) if doc else None

    async def update_event_summary_status(
        self,
        encounter_id: str,
        version: int,
        status: str,
        summary_text: Optional[str] = None
    ) -> bool:
        """Updates summary_status (SUMMARIZED, OBSOLETE, FAILED) for a specific version."""
        result = await self.collection.update_one(
            {"encounter_id": encounter_id, "version": version},
            {
                "$set": {
                    "summary_status": status,
                    "summary_text": summary_text,
                    "processed_at": datetime.utcnow()
                }
            }
        )
        return result.modified_count > 0

    async def get_stale_pending_events(self, older_than_seconds: int = 30, limit: int = 20) -> List[EncounterVersionLog]:
        """Fetches events that have exceeded the processing SLA threshold."""
        threshold = datetime.utcnow() - timedelta(seconds=older_than_seconds)
        cursor = self.collection.find({
            "summary_status": "PENDING",
            "received_at": {"$lt": threshold}
        }).limit(limit)

        events = []
        async for doc in cursor:
            events.append(EncounterVersionLog(**doc))
        return events
