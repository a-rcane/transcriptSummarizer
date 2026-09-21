import asyncio
from typing import Optional
from ..repositories.outbox_repository import OutboxRepository
from ..repositories.event_repository import EventRepository
from ..messaging.kafka_producer import KafkaEventProducer


class OutboxSweeper:
    """
    Background worker that recovers orphaned outbox records and detects SLA breaches.
    
    Responsibilities:
    1. Transactional Outbox Recovery: Polls `outbox_events` collection for records that
       remained in `PENDING_DISPATCH` status due to mid-flight service crashes before
       Kafka publishing. Publishes them to Kafka and marks them `DISPATCHED`.
    2. SLA Breach Detection: Monitors `encounter_events` for any version still in `PENDING`
       beyond the SLA timeout threshold and logs alerts or attempts recovery.
    """

    def __init__(
        self,
        outbox_repo: OutboxRepository,
        kafka_producer: KafkaEventProducer,
        event_repo: Optional[EventRepository] = None,
        poll_interval_seconds: float = 3.0,
        pending_grace_seconds: int = 2,
        sla_timeout_seconds: int = 30
    ):
        self.outbox_repo = outbox_repo
        self.kafka_producer = kafka_producer
        self.event_repo = event_repo
        self.poll_interval_seconds = poll_interval_seconds
        self.pending_grace_seconds = pending_grace_seconds
        self.sla_timeout_seconds = sla_timeout_seconds

        self.is_running: bool = False
        self._sweeper_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Starts the background polling loop."""
        self.is_running = True
        self._sweeper_task = asyncio.create_task(self._run_loop())
        print(f"[Outbox Sweeper] Started with poll interval {self.poll_interval_seconds}s (grace: {self.pending_grace_seconds}s, SLA: {self.sla_timeout_seconds}s).")

    async def stop(self) -> None:
        """Gracefully stops the background polling loop."""
        self.is_running = False
        if self._sweeper_task:
            self._sweeper_task.cancel()
            try:
                await self._sweeper_task
            except (asyncio.CancelledError, Exception):
                pass
            print("[Outbox Sweeper] Stopped gracefully.")

    async def _run_loop(self) -> None:
        while self.is_running:
            try:
                await self._recover_pending_outbox()
                if self.event_repo:
                    await self._detect_sla_breaches()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[Outbox Sweeper Error] Unexpected error in sweeper cycle: {e}")

            try:
                await asyncio.sleep(self.poll_interval_seconds)
            except asyncio.CancelledError:
                break

    async def _recover_pending_outbox(self) -> None:
        """Finds orphaned outbox records and publishes them to Kafka."""
        pending_records = await self.outbox_repo.get_pending_records(
            older_than_seconds=self.pending_grace_seconds,
            limit=50
        )
        if not pending_records:
            return

        for record in pending_records:
            try:
                published = await self.kafka_producer.publish_summarization_job(
                    encounter_id=record.encounter_id,
                    payload=record.payload
                )
                if published:
                    await self.outbox_repo.mark_dispatched(record.id)
                    print(f"[Outbox Recovery] Successfully recovered and dispatched record {record.id} to Kafka.")
                else:
                    await self.outbox_repo.increment_retry(
                        record.id,
                        error="Kafka producer failed to dispatch record during recovery"
                    )
            except Exception as err:
                print(f"[Outbox Recovery Error] Failed to dispatch record {record.id}: {err}")
                await self.outbox_repo.increment_retry(record.id, error=str(err))

    async def _detect_sla_breaches(self) -> None:
        """Detects versions that have been PENDING longer than the SLA limit."""
        if not self.event_repo:
            return

        stale_events = await self.event_repo.get_stale_pending_events(
            older_than_seconds=self.sla_timeout_seconds,
            limit=20
        )
        for event in stale_events:
            print(
                f"[SLA Breach Alert] Encounter {event.encounter_id} v{event.version} has been PENDING "
                f"for >{self.sla_timeout_seconds}s (received at {event.received_at})."
            )
