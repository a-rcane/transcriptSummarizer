from datetime import datetime
from typing import Optional, List
from ..models.summary import (
    SummaryOutput, SummaryTemplate, HumanOverrideRequest,
    ConfidenceBreakdown, ReconciliationResponse, FeedbackMetricsResponse
)
from ..models.encounter import UrgencyLevel, ReviewFeedbackRecord
from ..models.patient import PatientHistory
from ..repositories.encounter_repository import EncounterRepository
from ..repositories.patient_repository import PatientRepository
from ..repositories.event_repository import EventRepository
from ..repositories.dlq_repository import DLQRepository
from ..clients.ai_client import UnifiedAIClient
from ..clients.redis_client import RedisClient


class SummarizeService:
    def __init__(
        self,
        encounter_repo: EncounterRepository,
        patient_repo: PatientRepository,
        event_repo: EventRepository,
        ai_client: UnifiedAIClient,
        cache_client: Optional[RedisClient] = None,
        dlq_repo: Optional[DLQRepository] = None
    ):
        self.encounter_repo = encounter_repo
        self.patient_repo = patient_repo
        self.event_repo = event_repo
        self.ai_client = ai_client
        self.cache = cache_client
        self.dlq_repo = dlq_repo

    async def summarize_encounter(
        self,
        encounter_id: str,
        pId: str,
        transcription: str,
        version: int = 1
    ) -> Optional[SummaryOutput]:
        # 1. Out-of-Order Full Re-aggregation
        all_events = await self.event_repo.get_all_versions_for_encounter(encounter_id)
        if all_events and len(all_events) > 1:
            events_up_to_version = [e for e in all_events if e.version <= version]
            if events_up_to_version:
                full_transcription = "\n".join([f"[v{e.version}]: {e.transcription}" for e in events_up_to_version])
            else:
                full_transcription = "\n".join([f"[v{e.version}]: {e.transcription}" for e in all_events])
        else:
            full_transcription = transcription

        # 2. Fetch prior history
        history = await self.patient_repo.get_patient_history_by_pid(pId)
        prev_summary = history.complete_summary if history else None

        # 3. Call AI LLM
        ai_result: SummaryTemplate = await self.ai_client.generate_summary(
            new_transcription=full_transcription,
            patient_history_summary=prev_summary
        )

        breakdown_dict = ai_result.confidence_breakdown.model_dump() if ai_result.confidence_breakdown else None

        # 4. Atomic CAS update (Scenario 1: Prevents late v12 from overwriting newer v13)
        cas_success = await self.encounter_repo.update_summary(
            encounter_id=encounter_id,
            summary=ai_result.encounter_summary,
            job_version=version,
            encounter_type=ai_result.encounter_type.value if hasattr(ai_result.encounter_type, "value") else str(ai_result.encounter_type),
            confidence_score=ai_result.confidence_score,
            confidence_breakdown=breakdown_dict,
            urgency_level=ai_result.urgency_level,
            needs_human_review=ai_result.needs_human_review
        )

        if cas_success:
            # 5a. Mark this version as SUMMARIZED
            await self.event_repo.update_event_summary_status(
                encounter_id=encounter_id,
                version=version,
                status="SUMMARIZED",
                summary_text=ai_result.encounter_summary
            )

            # 5b. Rolling longitudinal compression (Only winning version modifies longitudinal record)
            encounter_tag = f"[Encounter {encounter_id} - {ai_result.encounter_type.value}]: {ai_result.encounter_summary}"
            rolling_summary = f"{prev_summary}\n{encounter_tag}".strip() if prev_summary else encounter_tag

            # 6. Save to PatientHistory (OCC)
            patient_hist = PatientHistory(
                pId=pId,
                complete_summary=rolling_summary,
                last_encounter_summary=ai_result.encounter_summary,
                active_problems=ai_result.active_problems,
                current_medications=ai_result.medications,
                last_visit=ai_result.last_visit or datetime.utcnow()
            )
            await self.patient_repo.upsert_patient_history_occ(patient_hist)

            # 7. Invalidate cached summaries so next read pulls fresh state
            if self.cache:
                await self.cache.delete(f"summary:encounter:{encounter_id}")
                await self.cache.delete(f"summary:patient:{pId}")
        else:
            # 8. Late or obsolete version: mark OBSOLETE in event log without corrupting state
            await self.event_repo.update_event_summary_status(
                encounter_id=encounter_id,
                version=version,
                status="OBSOLETE",
                summary_text=ai_result.encounter_summary
            )
            print(f"[Summarize Service] Version {version} for encounter {encounter_id} marked OBSOLETE (superseded).")

        output = await self.get_summary_by_encounter_id(encounter_id)
        return output

    async def get_summary_by_encounter_id(self, encounter_id: str) -> Optional[SummaryOutput]:
        cache_key = f"summary:encounter:{encounter_id}"

        # 1. Cache Hit Check
        if self.cache:
            cached_data = await self.cache.get_json(cache_key)
            if cached_data:
                return SummaryOutput(**cached_data)

        # 2. Cache Miss: Fetch from MongoDB
        encounter = await self.encounter_repo.get_by_encounter_id(encounter_id)
        if not encounter:
            return None

        history = await self.patient_repo.get_patient_history_by_pid(encounter.pId)

        breakdown = None
        if encounter.confidence_breakdown:
            breakdown = ConfidenceBreakdown(**encounter.confidence_breakdown)

        output = SummaryOutput(
            pId=encounter.pId,
            nId=history.nId if history else None,
            encounter_id=encounter.encounter_id,
            complete_summary=history.complete_summary if history else (encounter.summary or ""),
            last_encounter_summary=encounter.summary or encounter.transcriptions,
            ai_generated_summary=encounter.ai_generated_summary,
            encounter_type=encounter.encounter_type,
            urgency_level=encounter.urgency_level or UrgencyLevel.ROUTINE,
            active_problems=history.active_problems if history else [],
            current_medications=history.current_medications if history else [],
            confidence_score=getattr(encounter, "confidence_score", 1.0) or 1.0,
            confidence_breakdown=breakdown,
            needs_human_review=getattr(encounter, "needs_human_review", False) or False,
            is_reviewed=bool(encounter.reviewed_by),
            reviewed_by=encounter.reviewed_by,
            reviewed_at=encounter.reviewed_at,
            last_updated=encounter.updated_at
        )

        # 3. Store in Redis
        if self.cache:
            await self.cache.set_json(cache_key, output.model_dump(mode="json"))

        return output

    async def get_summary_by_patient_id(self, pId: str) -> Optional[SummaryOutput]:
        cache_key = f"summary:patient:{pId}"

        # 1. Cache Hit Check
        if self.cache:
            cached_data = await self.cache.get_json(cache_key)
            if cached_data:
                return SummaryOutput(**cached_data)

        # 2. Cache Miss: Fetch from MongoDB
        history = await self.patient_repo.get_patient_history_by_pid(pId)
        if not history:
            return None

        output = SummaryOutput(
            pId=history.pId,
            nId=history.nId,
            complete_summary=history.complete_summary,
            last_encounter_summary=history.last_encounter_summary,
            active_problems=history.active_problems,
            current_medications=history.current_medications,
            last_updated=history.updated_at
        )

        # 3. Store in Redis
        if self.cache:
            await self.cache.set_json(cache_key, output.model_dump(mode="json"))

        return output

    async def override_encounter_summary(self, encounter_id: str, request: HumanOverrideRequest) -> bool:
        encounter = await self.encounter_repo.get_by_encounter_id(encounter_id)
        if not encounter:
            return False

        success = await self.encounter_repo.override_summary(
            encounter_id=encounter_id,
            revised_summary=request.revised_summary,
            reviewed_by=request.reviewed_by,
            revised_encounter_type=request.revised_encounter_type.value if request.revised_encounter_type else None
        )
        if success and self.cache:
            # Invalidate both encounter and patient cache
            await self.cache.delete(f"summary:encounter:{encounter_id}")
            await self.cache.delete(f"summary:patient:{encounter.pId}")

        return success

    async def get_pending_reviews(self, urgency: Optional[UrgencyLevel] = None) -> List[SummaryOutput]:
        encounters = await self.encounter_repo.get_pending_human_reviews(urgency)
        results = []
        for enc in encounters:
            breakdown = None
            if enc.confidence_breakdown:
                breakdown = ConfidenceBreakdown(**enc.confidence_breakdown)

            results.append(SummaryOutput(
                pId=enc.pId,
                encounter_id=enc.encounter_id,
                complete_summary=enc.summary or "",
                last_encounter_summary=enc.summary,
                ai_generated_summary=enc.ai_generated_summary,
                encounter_type=enc.encounter_type,
                urgency_level=enc.urgency_level or UrgencyLevel.ROUTINE,
                confidence_score=getattr(enc, "confidence_score", 1.0) or 1.0,
                confidence_breakdown=breakdown,
                needs_human_review=True,
                is_reviewed=bool(enc.reviewed_by),
                reviewed_by=enc.reviewed_by,
                reviewed_at=enc.reviewed_at,
                last_updated=enc.updated_at
            ))
        return results

    async def reconcile_encounter(self, encounter_id: str) -> Optional[ReconciliationResponse]:
        """Feature 4a: Replays and reconciles all raw versions of an encounter in true order."""
        encounter = await self.encounter_repo.get_by_encounter_id(encounter_id)
        if not encounter:
            return None

        all_events = await self.event_repo.get_all_versions_for_encounter(encounter_id)
        if not all_events:
            return None

        # Sort versions chronologically
        all_events.sort(key=lambda e: e.version)
        latest_event = all_events[-1]

        # Re-run full summarization with latest version
        updated_summary = await self.summarize_encounter(
            encounter_id=encounter_id,
            pId=encounter.pId,
            transcription=latest_event.transcription,
            version=latest_event.version
        )

        return ReconciliationResponse(
            status="reconciled",
            encounter_id=encounter_id,
            pId=encounter.pId,
            total_versions_replayed=len(all_events),
            versions_replayed=[e.version for e in all_events],
            reconciled_summary=updated_summary,
            reconciled_at=datetime.utcnow()
        )

    async def get_review_history(self, encounter_id: str) -> List[ReviewFeedbackRecord]:
        """Feature 3a: Fetches clinical review diff history for an encounter."""
        return await self.encounter_repo.get_review_history(encounter_id)

    async def get_feedback_metrics(self) -> FeedbackMetricsResponse:
        """Feature 3a: Returns aggregate clinical review feedback metrics."""
        return await self.encounter_repo.get_feedback_metrics()

    async def retry_dlq_message(self, encounter_id: str) -> Optional[SummaryOutput]:
        """Feature 4b: Re-drives a failed message from DLQ and marks it resolved."""
        if not self.dlq_repo:
            return None

        record = await self.dlq_repo.get_dlq_message_by_encounter_id(encounter_id)
        if not record:
            return None

        payload = record.payload
        transcription = payload.get("transcriptions") or payload.get("transcription", "")
        version = payload.get("version", record.version or 1)
        summary = await self.summarize_encounter(
            encounter_id=record.encounter_id,
            pId=record.pId,
            transcription=transcription,
            version=version
        )

        await self.dlq_repo.mark_resolved(encounter_id, notes="Successfully reprocessed via admin retry API")
        return summary

