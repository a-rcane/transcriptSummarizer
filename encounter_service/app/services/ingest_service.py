from typing import Optional
from ..models.event import EncounterEventInput, IngestResponse
from ..models.encounter import EncounterEntity, EncounterVersionLog
from ..repositories.event_repository import EventRepository
from ..repositories.encounter_repository import EncounterRepository
from ..messaging.kafka_producer import KafkaEventProducer


class IngestService:
    """
    Orchestration service for inbound encounter events.
    
    Responsibilities:
    1. Deduplicate by event_id or composite key (encounter_id, version).
    2. Check if version is newer than current latest encounter state.
    3. Persist raw event in append-only secondary log for replay/out-of-order handling.
    4. If version is higher -> atomically update latest encounter state.
    5. If accepted as newer version -> trigger async AI summarization (via Kafka or background worker).
    """

    def __init__(
        self,
        event_repo: EventRepository,
        encounter_repo: EncounterRepository,
        kafka_producer: Optional[KafkaEventProducer] = None
    ):
        self.event_repo = event_repo
        self.encounter_repo = encounter_repo
        self.kafka_producer = kafka_producer

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

        # 2. Append to immutable raw event log (for out-of-order reconciliation & replay)
        saved = await self.event_repo.save_event_log(EncounterVersionLog(
            event_id=event.event_id,
            encounter_id=event.encounter_id,
            pId=event.pId,
            version=event.version,
            transcription=event.payload.transcriptions
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

        # 3. Check current latest version for this encounter
        current_latest_version = await self.encounter_repo.get_latest_version(event.encounter_id)

        # 4. If incoming version is strictly newer:
        if event.version > current_latest_version:
            # Atomic upsert of latest state
            await self.encounter_repo.upsert_latest_encounter(EncounterEntity(
                encounter_id=event.encounter_id,
                pId=event.pId,
                latest_version=event.version,
                transcriptions=event.payload.transcriptions
            ))
                
            # Trigger async AI summarization (publish to Kafka)
            published = False
            if self.kafka_producer:
                published = await self.kafka_producer.publish_summarization_job(
                    encounter_id=event.encounter_id,
                    payload={
                        "encounter_id": event.encounter_id,
                        "pId": event.pId,
                        "version": event.version,
                        "transcriptions": event.payload.transcriptions
                    }
                )

            return IngestResponse(
                status="accepted",
                message=f"Version {event.version} accepted as latest state. AI summarization triggered.",
                event_id=event.event_id,
                encounter_id=event.encounter_id,
                version=event.version,
                summarization_triggered=True
            )
        
        # 5. Out-of-order / stale version arrival
        return IngestResponse(
            status="ignored_stale_version",
            message=f"Stored in event log, but ignored for latest state (current={current_latest_version}, incoming={event.version}).",
            event_id=event.event_id,
            encounter_id=event.encounter_id,
            version=event.version,
            summarization_triggered=False
        )
