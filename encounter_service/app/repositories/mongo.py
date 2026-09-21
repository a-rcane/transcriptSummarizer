from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from typing import Optional
from pymongo import ASCENDING, DESCENDING
from ..config import settings


class MongoDB:
    client: Optional[AsyncIOMotorClient] = None
    db: Optional[AsyncIOMotorDatabase] = None

    @classmethod
    async def connect_to_database(cls) -> None:
        cls.client = AsyncIOMotorClient(settings.MONGO_URI)
        cls.db = cls.client[settings.MONGO_DB_NAME]

        # 1. Unique index on event_id for fast deduplication
        await cls.db["encounter_events"].create_index([("event_id", ASCENDING)], unique=True)
        # 2. Composite index on (encounter_id, version)
        await cls.db["encounter_events"].create_index(
            [("encounter_id", ASCENDING), ("version", ASCENDING)], 
            unique=True
        )
        # 3. Unique index on encounter_id for latest state store
        await cls.db["encounters"].create_index([("encounter_id", ASCENDING)], unique=True)
        # 4. Unique index on patient id
        await cls.db["patient_histories"].create_index([("pId", ASCENDING)], unique=True)
        # 5. DLQ indexes
        await cls.db["dlq_messages"].create_index([("encounter_id", ASCENDING)])
        await cls.db["dlq_messages"].create_index([("status", ASCENDING), ("failed_at", DESCENDING)])
        # 6. Outbox indexes
        await cls.db["outbox_events"].create_index([("status", ASCENDING), ("created_at", ASCENDING)])

    @classmethod
    async def close_database_connection(cls) -> None:
        if cls.client:
            cls.client.close()


async def get_database() -> AsyncIOMotorDatabase:
    if MongoDB.db is None:
        await MongoDB.connect_to_database()
    return MongoDB.db
