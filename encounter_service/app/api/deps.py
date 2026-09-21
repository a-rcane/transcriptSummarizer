from fastapi import Depends
from motor.motor_asyncio import AsyncIOMotorDatabase
from ..repositories.mongo import get_database
from ..repositories.outbox_repository import OutboxRepository
from ..repositories.event_repository import EventRepository
from ..repositories.encounter_repository import EncounterRepository
from ..repositories.patient_repository import PatientRepository
from ..repositories.dlq_repository import DLQRepository
from ..clients.ai_client import UnifiedAIClient
from ..clients.redis_client import RedisClient
from ..messaging.kafka_producer import KafkaEventProducer
from ..services.ingest_service import IngestService
from ..services.summarize_service import SummarizeService

_kafka_producer: KafkaEventProducer = KafkaEventProducer()
_ai_client: UnifiedAIClient = UnifiedAIClient()
_redis_client: RedisClient = RedisClient()


def get_event_repository(db: AsyncIOMotorDatabase = Depends(get_database)) -> EventRepository:
    """Dependency provider for EventRepository."""
    return EventRepository(db)


def get_encounter_repository(db: AsyncIOMotorDatabase = Depends(get_database)) -> EncounterRepository:
    """Dependency provider for EncounterRepository."""
    return EncounterRepository(db)


def get_outbox_repository(db: AsyncIOMotorDatabase = Depends(get_database)) -> OutboxRepository:
    """Dependency provider for OutboxRepository."""
    return OutboxRepository(db)


def get_patient_repository(db: AsyncIOMotorDatabase = Depends(get_database)) -> PatientRepository:
    """Dependency provider for PatientRepository."""
    return PatientRepository(db)


def get_dlq_repository(db: AsyncIOMotorDatabase = Depends(get_database)) -> DLQRepository:
    """Dependency provider for DLQRepository."""
    return DLQRepository(db)


def get_ai_client() -> UnifiedAIClient:
    """Dependency provider for UnifiedAIClient (singleton)."""
    return _ai_client


def get_kafka_producer() -> KafkaEventProducer:
    """Dependency provider for KafkaEventProducer (singleton)."""
    return _kafka_producer

def get_redis_client() -> RedisClient:
    """Dependency provider for RedisClient (singleton)."""
    return _redis_client

def get_ingest_service(
    event_repo: EventRepository = Depends(get_event_repository),
    encounter_repo: EncounterRepository = Depends(get_encounter_repository),
    kafka_producer: KafkaEventProducer = Depends(get_kafka_producer),
    outbox_repo: OutboxRepository = Depends(get_outbox_repository)
) -> IngestService:
    """Dependency provider for IngestService."""
    return IngestService(event_repo, encounter_repo, kafka_producer, outbox_repo)



def get_summarize_service(
    encounter_repo: EncounterRepository = Depends(get_encounter_repository),
    patient_repo: PatientRepository = Depends(get_patient_repository),
    event_repo: EventRepository = Depends(get_event_repository),
    ai_client: UnifiedAIClient = Depends(get_ai_client),
    cache_client: RedisClient = Depends(get_redis_client),
    dlq_repo: DLQRepository = Depends(get_dlq_repository)
) -> SummarizeService:
    """Dependency provider for SummarizeService."""
    return SummarizeService(encounter_repo, patient_repo, event_repo, ai_client, cache_client, dlq_repo)