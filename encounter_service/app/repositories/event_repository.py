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
