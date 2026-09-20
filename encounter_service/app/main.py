import asyncio
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from .config import settings
from .api.v1 import api_v1_router
from .api.v1.webhook import router as root_webhook_router
from .api.v1.summary import router as root_summary_router
from .repositories.mongo import MongoDB
from .repositories.encounter_repository import EncounterRepository
from .repositories.patient_repository import PatientRepository
from .repositories.event_repository import EventRepository
from .repositories.dlq_repository import DLQRepository
from .clients.ai_client import UnifiedAIClient
from .services.summarize_service import SummarizeService
from .messaging.kafka_consumer import KafkaSummarizationConsumer
from .api.deps import _kafka_producer, _redis_client, _ai_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: MongoDB, Kafka Producer, Redis, Kafka Consumer Worker
    try:
        await MongoDB.connect_to_database()
        print("[Startup] MongoDB connected and indexed.")
    except Exception as e:
        print(f"[Startup Warning] MongoDB connection failed: {e}")

    try:
        await _kafka_producer.start()
        print("[Startup] Kafka producer started.")
    except Exception as e:
        print(f"[Startup Warning] Kafka producer failed: {e}")

    try:
        await _redis_client.connect()
    except Exception as e:
        print(f"[Startup Warning] Redis connect failed: {e}")

    consumer = None
    consumer_task = None
    try:
        if MongoDB.db is not None:
            dlq_repo = DLQRepository(MongoDB.db)
            summarize_service = SummarizeService(
                encounter_repo=EncounterRepository(MongoDB.db),
                patient_repo=PatientRepository(MongoDB.db),
                event_repo=EventRepository(MongoDB.db),
                ai_client=_ai_client,
                cache_client=_redis_client,
                dlq_repo=dlq_repo
            )
            consumer = KafkaSummarizationConsumer()
            consumer_task = asyncio.create_task(consumer.start(summarize_service, dlq_repo))
    except Exception as e:
        print(f"[Startup Warning] Kafka consumer worker init failed: {e}")

    yield

    # Shutdown: Clean up connections and workers
    if consumer:
        await consumer.stop()
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except (asyncio.CancelledError, Exception):
            pass

    try:
        await _redis_client.close()
    except Exception:
        pass

    try:
        await _kafka_producer.stop()
    except Exception:
        pass

    try:
        await MongoDB.close_database_connection()
    except Exception:
        pass


app = FastAPI(
    title="Encounter Ingestion & Summarization Service",
    description="Microservice with MongoDB, Kafka, Redis caching, and LiteLLM.",
    version="1.0.0",
    lifespan=lifespan
)

# Enable CORS for browser access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Root-level route aliases matching problem statement
app.include_router(root_webhook_router, tags=["Webhook"])
app.include_router(root_summary_router, tags=["Summary"])

# Versioned API routes
app.include_router(api_v1_router)

STATIC_DIR = Path(__file__).resolve().parent / "static"


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def serve_dashboard():
    """Serves the interactive web UI dashboard."""
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        return HTMLResponse(content=index_file.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>Clinical AI Summarizer API Online</h1><p>Visit <a href='/docs'>/docs</a> for Swagger UI.</p>")


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint for container probes."""
    return {"status": "ok", "service": settings.SERVICE_NAME}

