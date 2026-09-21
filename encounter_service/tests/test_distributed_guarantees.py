import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from app.models.encounter import EncounterEntity, EncounterVersionLog, EncounterType, UrgencyLevel
from app.models.event import EncounterEventInput, EncounterPayload
from app.models.summary import SummaryTemplate, ConfidenceBreakdown
from app.models.outbox import OutboxRecord
from app.models.patient import PatientHistory
from app.services.summarize_service import SummarizeService
from app.services.ingest_service import IngestService
from app.services.outbox_sweeper import OutboxSweeper


# =====================================================================
# TEST 1: SCENARIO 1 (v13 finishes before v12 -> v12 marked OBSOLETE)
# =====================================================================

@pytest.mark.asyncio
async def test_late_v12_cannot_overwrite_newer_v13_summary():
    """
    Proves that if version 13 finishes summarizing before version 12:
    1. v13 successfully updates summary_version to 13 and marks status SUMMARIZED.
    2. Later-finishing v12 fails atomic CAS (summary_version 13 >= 12).
    3. v12 is marked as OBSOLETE in event log.
    4. Current encounter summary remains v13's summary.
    5. Patient longitudinal history is NOT corrupted by v12.
    """
    # Arrange Mocks
    mock_encounter_repo = AsyncMock()
    mock_patient_repo = AsyncMock()
    mock_event_repo = AsyncMock()
    mock_ai_client = AsyncMock()
    mock_cache = AsyncMock()

    service = SummarizeService(
        encounter_repo=mock_encounter_repo,
        patient_repo=mock_patient_repo,
        event_repo=mock_event_repo,
        ai_client=mock_ai_client,
        cache_client=mock_cache
    )

    # Mock AI Client outputs
    v13_ai_result = SummaryTemplate(
        encounter_summary="v13: Patient diagnosed with mild hypertension. Prescribed Lisinopril.",
        encounter_type=EncounterType.TRIAGE,
        confidence_score=0.95,
        confidence_breakdown=ConfidenceBreakdown(extraction_confidence=0.95, diagnosis_confidence=0.95, medication_confidence=0.95),
        urgency_level=UrgencyLevel.ROUTINE,
        needs_human_review=False,
        active_problems=["Hypertension"],
        medications=["Lisinopril 10mg"]
    )

    v12_ai_result = SummaryTemplate(
        encounter_summary="v12: Patient reported mild headache and fatigue.",
        encounter_type=EncounterType.TRIAGE,
        confidence_score=0.90,
        confidence_breakdown=ConfidenceBreakdown(extraction_confidence=0.90, diagnosis_confidence=0.90, medication_confidence=0.90),
        urgency_level=UrgencyLevel.ROUTINE,
        needs_human_review=False,
        active_problems=["Headache"],
        medications=[]
    )

    # Step 1: v13 completes FIRST -> update_summary CAS returns True
    mock_ai_client.generate_summary.return_value = v13_ai_result
    mock_encounter_repo.update_summary.return_value = True  # Won CAS (0 < 13)
    mock_patient_repo.get_patient_history_by_pid.return_value = None

    await service.summarize_encounter(
        encounter_id="enc-101",
        pId="p-001",
        transcription="v13 transcript",
        version=13
    )

    # Assert v13 won CAS and updated status to SUMMARIZED
    mock_encounter_repo.update_summary.assert_called_with(
        encounter_id="enc-101",
        summary=v13_ai_result.encounter_summary,
        job_version=13,
        encounter_type="triage",
        confidence_score=0.95,
        confidence_breakdown=v13_ai_result.confidence_breakdown.model_dump(),
        urgency_level=UrgencyLevel.ROUTINE,
        needs_human_review=False
    )
    mock_event_repo.update_event_summary_status.assert_called_with(
        encounter_id="enc-101",
        version=13,
        status="SUMMARIZED",
        summary_text=v13_ai_result.encounter_summary
    )
    # Patient history updated for v13
    assert mock_patient_repo.upsert_patient_history_occ.call_count == 1

    # Step 2: v12 completes LATER -> update_summary CAS returns False (13 >= 12)
    mock_ai_client.generate_summary.return_value = v12_ai_result
    mock_encounter_repo.update_summary.return_value = False  # Lost CAS!

    await service.summarize_encounter(
        encounter_id="enc-101",
        pId="p-001",
        transcription="v12 transcript",
        version=12
    )

    # Assert v12 was marked OBSOLETE
    mock_event_repo.update_event_summary_status.assert_called_with(
        encounter_id="enc-101",
        version=12,
        status="OBSOLETE",
        summary_text=v12_ai_result.encounter_summary
    )
    # Crucial Assertion: Patient history was NOT updated again for the obsolete v12!
    assert mock_patient_repo.upsert_patient_history_occ.call_count == 1


# =====================================================================
# TEST 2: SCENARIO 2 (Transactional Outbox Crash Recovery)
# =====================================================================

@pytest.mark.asyncio
async def test_outbox_recovers_pending_records_after_service_crash():
    """
    Proves that if the service crashes after writing to MongoDB but before
    publishing to Kafka, the OutboxSweeper detects the orphan and publishes it.
    """
    mock_outbox_repo = AsyncMock()
    mock_kafka_producer = AsyncMock()
    mock_event_repo = AsyncMock()

    # Simulate an orphaned outbox record in PENDING_DISPATCH
    orphaned_record = OutboxRecord(
        _id="enc-202-v1",
        encounter_id="enc-202",
        pId="p-002",
        version=1,
        payload={"encounter_id": "enc-202", "pId": "p-002", "version": 1, "transcriptions": "Patient checkup."},
        status="PENDING_DISPATCH",
        created_at=datetime.utcnow()
    )
    mock_outbox_repo.get_pending_records.return_value = [orphaned_record]
    mock_kafka_producer.publish_summarization_job.return_value = True

    sweeper = OutboxSweeper(
        outbox_repo=mock_outbox_repo,
        kafka_producer=mock_kafka_producer,
        event_repo=mock_event_repo,
        poll_interval_seconds=1.0,
        pending_grace_seconds=0
    )

    # Run one recovery cycle
    await sweeper._recover_pending_outbox()

    # Verify recovery published to Kafka
    mock_kafka_producer.publish_summarization_job.assert_called_once_with(
        encounter_id="enc-202",
        payload=orphaned_record.payload
    )

    # Verify record was transitioned to DISPATCHED
    mock_outbox_repo.mark_dispatched.assert_called_once_with("enc-202-v1")


# =====================================================================
# TEST 3: PATIENT IDENTITY IMMUTABILITY GUARD
# =====================================================================

@pytest.mark.asyncio
async def test_patient_identity_mismatch_rejected():
    """
    Proves that attempting to ingest a version with a different patient ID
    for an existing encounter is strictly rejected.
    """
    mock_event_repo = AsyncMock()
    mock_encounter_repo = AsyncMock()
    mock_kafka_producer = AsyncMock()
    mock_outbox_repo = AsyncMock()

    mock_event_repo.event_exists.return_value = False

    # Existing encounter belongs to patient "p-100"
    mock_encounter_repo.get_by_encounter_id.return_value = EncounterEntity(
        encounter_id="enc-303",
        pId="p-100",
        latest_version=1,
        transcriptions="Initial visit."
    )

    ingest_service = IngestService(
        event_repo=mock_event_repo,
        encounter_repo=mock_encounter_repo,
        kafka_producer=mock_kafka_producer,
        outbox_repo=mock_outbox_repo
    )

    # Inbound event tries to assign encounter to patient "p-999" (mismatch!)
    mismatched_event = EncounterEventInput(
        event_id="evt-303-v2",
        encounter_id="enc-303",
        pId="p-999",
        version=2,
        payload=EncounterPayload(transcriptions="Follow-up transcript")
    )

    response = await ingest_service.ingest(mismatched_event)

    assert response.status == "rejected_patient_mismatch"
    assert response.summarization_triggered is False
    # Verify no Kafka publish or Outbox write occurred
    assert mock_kafka_producer.publish_summarization_job.call_count == 0
    assert mock_outbox_repo.save_outbox_record.call_count == 0
