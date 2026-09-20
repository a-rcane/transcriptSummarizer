from datetime import datetime
from typing import Optional, List, Dict, Any
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from ..models.dlq import DLQMessageRecord, DLQStatsResponse


class DLQRepository:
    """Repository for persisting, inspecting, and resolving dead-letter queue (DLQ) messages."""

    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.collection = db["dlq_messages"]

    async def save_failed_message(
        self,
        encounter_id: str,
        pId: str,
        version: int,
        payload: Dict[str, Any],
        error_message: str,
        retry_attempts: int = 3
    ) -> str:
        record = {
            "encounter_id": encounter_id,
            "pId": pId,
            "version": version,
            "payload": payload,
            "error_message": error_message,
            "retry_attempts": retry_attempts,
            "status": "pending",
            "failed_at": datetime.utcnow(),
            "resolved_at": None,
            "resolution_notes": None
        }
        result = await self.collection.insert_one(record)
        return str(result.inserted_id)

    async def get_dlq_messages(
        self,
        status: Optional[str] = None,
        limit: int = 50,
        skip: int = 0
    ) -> List[DLQMessageRecord]:
        query = {"status": status} if status else {}
        cursor = self.collection.find(query).sort("failed_at", -1).skip(skip).limit(limit)
        results = []
        async for doc in cursor:
            doc_id = str(doc.pop("_id"))
            results.append(DLQMessageRecord(id=doc_id, **doc))
        return results

    async def get_dlq_message_by_encounter_id(self, encounter_id: str) -> Optional[DLQMessageRecord]:
        doc = await self.collection.find_one({"encounter_id": encounter_id, "status": "pending"})
        if not doc:
            doc = await self.collection.find_one({"encounter_id": encounter_id})
        if doc:
            doc_id = str(doc.pop("_id"))
            return DLQMessageRecord(id=doc_id, **doc)
        return None

    async def mark_resolved(self, encounter_id: str, notes: Optional[str] = "Manually redriven and successfully processed") -> bool:
        result = await self.collection.update_many(
            {"encounter_id": encounter_id, "status": "pending"},
            {"$set": {
                "status": "resolved",
                "resolved_at": datetime.utcnow(),
                "resolution_notes": notes
            }}
        )
        return result.modified_count > 0

    async def delete_dlq_message(self, dlq_id: str) -> bool:
        try:
            result = await self.collection.delete_one({"_id": ObjectId(dlq_id)})
            return result.deleted_count > 0
        except Exception:
            return False

    async def get_dlq_stats(self) -> DLQStatsResponse:
        total = await self.collection.count_documents({})
        pending = await self.collection.count_documents({"status": "pending"})
        resolved = await self.collection.count_documents({"status": "resolved"})
        discarded = await self.collection.count_documents({"status": "discarded"})

        pipeline = [
            {"$group": {"_id": "$error_message", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        top_errors = []
        async for doc in self.collection.aggregate(pipeline):
            top_errors.append({"error": doc["_id"], "count": doc["count"]})

        return DLQStatsResponse(
            total_dlq_messages=total,
            pending_count=pending,
            resolved_count=resolved,
            discarded_count=discarded,
            top_error_reasons=top_errors
        )
