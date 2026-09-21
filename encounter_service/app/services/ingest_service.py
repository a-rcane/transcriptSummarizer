from typing import Optional
from ..models.event import EncounterEventInput, IngestResponse
from ..models.encounter import EncounterEntity, EncounterVersionLog
from ..repositories.event_repository import EventRepository
from ..repositories.encounter_repository import EncounterRepository
from ..repositories.outbox_repository import OutboxRepository
from ..messaging.kafka_producer import KafkaEventProducer


class IngestService:
    """
    Orchestration service for inbound encounter events.
    
    Responsibilities:
    1. Deduplicate by event_id or composite key (encounter_id, version).
    2. Enforce Patient Identity Immutability (prevent pId mismatch).
    3. Persist raw event in append-only secondary log for replay/out-of-order handling.
    4. Transactional Outbox write for zero data loss across service crashes.
    5. Atomic state upsert for strictly newer versions.
    6. Fast-path asynchronous Kafka dispatch.
    """

    def __init__(
        self,
        event_repo: EventRepository,
        encounter_repo: EncounterRepository,
        kafka_producer: Optional[KafkaEventProducer] = None,
        outbox_repo: Optional[OutboxRepository] = None
    ):
        self.event_repo = event_repo
        self.encounter_repo = encounter_repo
        self.kafka_producer = kafka_producer
        self.outbox_repo = outbox_repo

    async def ingest(self, event: EncounterEventInput) -> IngestResponse:
        # 1. Deduplication check
        if await self.event_repo.event_exists(event.event_id):
            return IngestResponse(
                status="ignored_duplicate",
                message=f"Event {event.event_id} has already been processed.",
                event_id=event.event_id,
                encounter_id=event.encounter_id,
                version=event.version,
                summarization_triggered=False
            )

        # 2. Patient Identity Immutability Guard
        existing_enc = await self.encounter_repo.get_by_encounter_id(event.encounter_id)
        if existing_enc and existing_enc.pId != event.pId:
            return IngestResponse(
                status="rejected_patient_mismatch",
                message=f"Patient ID mismatch for encounter {event.encounter_id}. Expected {existing_enc.pId}, got {event.pId}.",
                event_id=event.event_id,
                encounter_id=event.encounter_id,
                version=event.version,
                summarization_triggered=False
            )

        # 3. Append to immutable raw event log (with PENDING status)
        saved = await self.event_repo.save_event_log(EncounterVersionLog(
            event_id=event.event_id,
            encounter_id=event.encounter_id,
            pId=event.pId,
            version=event.version,
            transcription=event.payload.transcriptions,
            summary_status="PENDING"
        ))
        if not saved:
            # Duplicate event_id or duplicate (encounter_id, version) detected concurrently
            return IngestResponse(
                status="ignored_duplicate",
                message=f"Event {event.event_id} (encounter={event.encounter_id}, version={event.version}) is a duplicate.",
                event_id=event.event_id,
                encounter_id=event.encounter_id,
                version=event.version,
                summarization_triggered=False
            )

        # 4. Check current latest version for this encounter
        current_latest_version = existing_enc.latest_version if existing_enc else 0

        # 5. If incoming version is strictly newer:
        if event.version > current_latest_version:
            # Atomic upsert of latest state
            await self.encounter_repo.upsert_latest_encounter(EncounterEntity(
                encounter_id=event.encounter_id,
                pId=event.pId,
                latest_version=event.version,
                transcriptions=event.payload.transcriptions
            ))
            
            payload_data = {
                "encounter_id": event.encounter_id,
                "pId": event.pId,
                "version": event.version,
                "transcriptions": event.payload.transcriptions
            }

            # Transactional Outbox write to guarantee crash recovery
            outbox_id = None
            if self.outbox_repo:
                outbox_id = await self.outbox_repo.save_outbox_record(
                    encounter_id=event.encounter_id,
                    pId=event.pId,
                    version=event.version,
                    payload=payload_data
                )

            # Fast-path asynchronous Kafka dispatch
            published = False
            if self.kafka_producer:
                published = await self.kafka_producer.publish_summarization_job(
                    encounter_id=event.encounter_id,
                    payload=payload_data
                )
                if published and self.outbox_repo and outbox_id:
                    await self.outbox_repo.mark_dispatched(outbox_id)

            return IngestResponse(
                status="accepted",
                message=f"Version {event.version} accepted as latest state. AI summarization dispatched.",
                event_id=event.event_id,
                encounter_id=event.encounter_id,
                version=event.version,
                summarization_triggered=True
            )
        
        # 6. Out-of-order / stale version arrival
        return IngestResponse(
            status="ignored_stale_version",
            message=f"Stored in event log, but ignored for latest state (current={current_latest_version}, incoming={event.version}).",
            event_id=event.event_id,
            encounter_id=event.encounter_id,
            version=event.version,
            summarization_triggered=False
        )
