from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from ...models.summary import (
    SummaryOutput, HumanOverrideRequest,
    ReconciliationResponse, FeedbackMetricsResponse
)
from ...models.encounter import UrgencyLevel, ReviewFeedbackRecord
from ...models.dlq import DLQMessageRecord, DLQStatsResponse
from ...models.fhir_converter import FHIRConverter
from ...clients.ai_client import UnifiedAIClient
from ...repositories.encounter_repository import EncounterRepository
from ...repositories.dlq_repository import DLQRepository
from ..deps import get_summarize_service, get_encounter_repository, get_ai_client, get_dlq_repository
from ...services.summarize_service import SummarizeService

router = APIRouter()


@router.get(
    "/getSummary/{encounter_id}",
    response_model=SummaryOutput,
    summary="Get summary for a specific encounter"
)
async def get_encounter_summary(
    encounter_id: str,
    summarize_service: SummarizeService = Depends(get_summarize_service)
) -> SummaryOutput:
    result = await summarize_service.get_summary_by_encounter_id(encounter_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Encounter not found")
    return result


@router.get(
    "/patients/{pId}/summary",
    response_model=SummaryOutput,
    summary="Get longitudinal summary for a patient"
)
async def get_patient_summary(
    pId: str,
    summarize_service: SummarizeService = Depends(get_summarize_service)
) -> SummaryOutput:
    result = await summarize_service.get_summary_by_patient_id(pId)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Patient history not found")
    return result


@router.get(
    "/encounters/pending-review",
    response_model=List[SummaryOutput],
    summary="Fetch unverified summaries sorted by clinical urgency priority (3c)"
)
async def get_pending_reviews(
    urgency: Optional[UrgencyLevel] = Query(None, description="Optional filter by urgency: 'critical', 'urgent', 'routine'"),
    summarize_service: SummarizeService = Depends(get_summarize_service)
) -> List[SummaryOutput]:
    """Returns all summaries flagged for clinical review, prioritized by clinical acuity."""
    return await summarize_service.get_pending_reviews(urgency=urgency)


@router.post(
    "/encounters/{encounter_id}/override-summary",
    summary="Physician override for an AI summary with text diff tracking (3a)"
)
async def override_summary(
    encounter_id: str,
    request: HumanOverrideRequest,
    summarize_service: SummarizeService = Depends(get_summarize_service)
):
    """Allows physicians or nurses to edit and approve AI summaries, calculating modification diffs."""
    success = await summarize_service.override_encounter_summary(encounter_id, request)
    if not success:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Encounter not found")
    return {"status": "success", "message": f"Encounter {encounter_id} summary updated by {request.reviewed_by}."}


@router.get(
    "/encounters/{encounter_id}/review-history",
    response_model=List[ReviewFeedbackRecord],
    summary="Get clinical revision diff and audit history for an encounter (3a)"
)
async def get_encounter_review_history(
    encounter_id: str,
    summarize_service: SummarizeService = Depends(get_summarize_service)
) -> List[ReviewFeedbackRecord]:
    """Fetches full revision history and diffs between AI generation and clinician edits."""
    return await summarize_service.get_review_history(encounter_id)


@router.get(
    "/encounters/feedback-metrics",
    response_model=FeedbackMetricsResponse,
    summary="Get aggregated clinical AI review accuracy and modification metrics (3a)"
)
async def get_feedback_metrics(
    summarize_service: SummarizeService = Depends(get_summarize_service)
) -> FeedbackMetricsResponse:
    """Calculates clinician modification rates, acceptance metrics, and confidence distributions."""
    return await summarize_service.get_feedback_metrics()


@router.post(
    "/encounters/{encounter_id}/reconcile",
    response_model=ReconciliationResponse,
    summary="Manual event replay and out-of-order reconciliation API (4a)"
)
async def reconcile_encounter(
    encounter_id: str,
    summarize_service: SummarizeService = Depends(get_summarize_service)
) -> ReconciliationResponse:
    """Re-assembles all raw versions in true order and recalculates the latest encounter and longitudinal summary."""
    result = await summarize_service.reconcile_encounter(encounter_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Encounter {encounter_id} or raw events not found")
    return result


@router.get(
    "/admin/dlq/messages",
    response_model=List[DLQMessageRecord],
    summary="List dead-letter queue (DLQ) failed messages (4b)"
)
async def list_dlq_messages(
    status: Optional[str] = Query(None, description="'pending', 'resolved', 'discarded'"),
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    dlq_repo: DLQRepository = Depends(get_dlq_repository)
) -> List[DLQMessageRecord]:
    """Inspects failed summarization jobs routed to the Dead-Letter Queue."""
    return await dlq_repo.get_dlq_messages(status=status, limit=limit, skip=skip)


@router.post(
    "/admin/dlq/retry/{encounter_id}",
    response_model=SummaryOutput,
    summary="Retry and redrive a failed encounter message from DLQ (4b)"
)
async def retry_dlq_message(
    encounter_id: str,
    summarize_service: SummarizeService = Depends(get_summarize_service)
) -> SummaryOutput:
    """Re-processes a failed message from DLQ and marks it resolved upon success."""
    result = await summarize_service.retry_dlq_message(encounter_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"DLQ message for encounter {encounter_id} not found or failed")
    return result


@router.delete(
    "/admin/dlq/{dlq_id}",
    summary="Delete or discard a DLQ message (4b)"
)
async def delete_dlq_message(
    dlq_id: str,
    dlq_repo: DLQRepository = Depends(get_dlq_repository)
):
    """Deletes or purges a DLQ message record."""
    success = await dlq_repo.delete_dlq_message(dlq_id)
    if not success:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="DLQ message not found")
    return {"status": "success", "message": f"DLQ message {dlq_id} removed."}


@router.get(
    "/admin/dlq/stats",
    response_model=DLQStatsResponse,
    summary="Get DLQ health metrics and top failure reasons (4b)"
)
async def get_dlq_stats(
    dlq_repo: DLQRepository = Depends(get_dlq_repository)
) -> DLQStatsResponse:
    """Returns DLQ operational metrics and common error breakdowns."""
    return await dlq_repo.get_dlq_stats()


@router.get(
    "/encounters/{encounter_id}/fhir",
    summary="Export clinical summary as HL7 FHIR R4 JSON Bundle"
)
async def get_encounter_fhir_bundle(
    encounter_id: str,
    summarize_service: SummarizeService = Depends(get_summarize_service)
):
    """Generates an interoperable HL7 FHIR R4 Bundle (Encounter, Conditions, MedicationRequests)."""
    summary = await summarize_service.get_summary_by_encounter_id(encounter_id)
    if not summary:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Encounter not found")
    return FHIRConverter.to_fhir_bundle(summary)


@router.get(
    "/encounters/{encounter_id}/stream",
    summary="Stream AI summary generation in real-time (Server-Sent Events)"
)
async def stream_encounter_summary(
    encounter_id: str,
    enc_repo: EncounterRepository = Depends(get_encounter_repository),
    ai_client: UnifiedAIClient = Depends(get_ai_client)
):
    """Streams tokens in real-time over SSE (text/event-stream) as Ollama generates them."""
    encounter = await enc_repo.get_by_encounter_id(encounter_id)
    if not encounter:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Encounter not found")
    async def event_generator():
        async for token in ai_client.stream_summary_tokens(encounter.transcriptions):
            yield f"data: {token}\n\n"
        yield "data: [DONE]\n\n"
    return StreamingResponse(event_generator(), media_type="text/event-stream")