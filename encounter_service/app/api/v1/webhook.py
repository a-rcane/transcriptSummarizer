from fastapi import APIRouter, Depends, status
from ...models.event import EncounterEventInput, IngestResponse
from ..deps import get_ingest_service
from ...services.ingest_service import IngestService

router = APIRouter()

@router.post(
    "/webhook",
    response_model=IngestResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest encounter update event from partner"
)
async def ingest_encounter_webhook(
    event: EncounterEventInput,
    ingest_service: IngestService = Depends(get_ingest_service)
) -> IngestResponse:
    return await ingest_service.ingest(event)
