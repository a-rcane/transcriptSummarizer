from fastapi import APIRouter, BackgroundTasks, status
from typing import Dict, Any
from pydantic import BaseModel
from ..generator.event_generator import StreamSimulationConfig, EventGenerator
from ..services.http_streamer import HttpStreamer
from ..messaging.kafka_streamer import KafkaStreamer

router = APIRouter(prefix="/stream", tags=["Event Streamer"])


class CustomEventPayload(BaseModel):
    event_id: str
    encounter_id: str
    pId: str
    version: int
    transcription: str


http_streamer = HttpStreamer()
kafka_streamer = KafkaStreamer()


@router.post("/start", summary="Start synthetic event streaming simulation (HTTP Webhook)")
async def start_stream_simulation(
        config: StreamSimulationConfig,
        background_tasks: BackgroundTasks
):
    events = EventGenerator.generate_event_stream(config)
    background_tasks.add_task(http_streamer.stream_batch, events, config.delay_ms)
    return {
        "status": "started",
        "total_events_generated": len(events),
        "encounters": config.num_encounters
    }


@router.post("/kafka/start", summary="Start synthetic event streaming simulation directly to Kafka topic")
async def start_kafka_simulation(
        config: StreamSimulationConfig,
        background_tasks: BackgroundTasks
):
    events = EventGenerator.generate_event_stream(config)
    background_tasks.add_task(kafka_streamer.stream_events, events, config.delay_ms)
    return {
        "status": "started_kafka",
        "total_events_generated": len(events),
        "encounters": config.num_encounters
    }


@router.post("/send-single", summary="Send a single custom encounter event")
async def send_single_event(payload: CustomEventPayload):
    event_data = {
        "event_id": payload.event_id,
        "encounter_id": payload.encounter_id,
        "pId": payload.pId,
        "version": payload.version,
        "payload": {"transcriptions": payload.transcription}
    }
    return await http_streamer.send_single_event(event_data)


@router.get("/status", summary="Get streamer service status")
async def get_streamer_status():
    return {"service": "event-streamer", "status": "ready"}
