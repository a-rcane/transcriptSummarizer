from datetime import datetime
from typing import Optional, List, Dict, Any
from motor.motor_asyncio import AsyncIOMotorDatabase
from ..models.outbox import OutboxRecord


class OutboxRepository:
    """
    Data Access Layer for Transactional Outbox Pattern in MongoDB.
    Manages pending events to ensure crash-recovery and at-least-once delivery to Kafka.
    """

    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.collection = db["outbox_events"]

    async def save_outbox_record(
        self,
        encounter_id: str,
        pId: str,
        version: int,
        payload: Dict[str, Any]
    ) -> str:
        """Upserts an outbox record with PENDING_DISPATCH status."""
        outbox_id = f"{encounter_id}-v{version}"
        now = datetime.utcnow()
        await self.collection.update_one(
            {"_id": outbox_id},
            {
                "$set": {
                    "encounter_id": encounter_id,
                    "pId": pId,
                    "version": version,
                    "payload": payload,
                    "status": "PENDING_DISPATCH",
                    "created_at": now,
                    "retry_count": 0
                }
            },
            upsert=True
        )
        return outbox_id

    async def mark_dispatched(self, outbox_id: str) -> bool:
        """Marks an outbox record as successfully dispatched to Kafka."""
        result = await self.collection.update_one(
            {"_id": outbox_id},
            {
                "$set": {
                    "status": "DISPATCHED",
                    "dispatched_at": datetime.utcnow()
                }
            }
        )
        return result.modified_count > 0

    async def get_pending_records(self, older_than_seconds: int = 2, limit: int = 50) -> List[OutboxRecord]:
        """Fetches pending outbox records that were not dispatched (likely due to a crash)."""
        threshold = datetime.utcnow()
        if older_than_seconds > 0:
            from datetime import timedelta
            threshold = threshold - timedelta(seconds=older_than_seconds)

        cursor = self.collection.find({
            "status": "PENDING_DISPATCH",
            "created_at": {"$lt": threshold}
        }).limit(limit)

        records = []
        async for doc in cursor:
            records.append(OutboxRecord(**doc))
        return records

    async def increment_retry(self, outbox_id: str, error: Optional[str] = None) -> None:
        """Increments retry attempt counter and logs error."""
        await self.collection.update_one(
            {"_id": outbox_id},
            {
                "$inc": {"retry_count": 1},
                "$set": {"last_error": error, "updated_at": datetime.utcnow()}
            }
        )
