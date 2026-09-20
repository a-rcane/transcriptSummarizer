from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    SERVICE_NAME: str = "encounter-service"
    PORT: int = 8000
    DEBUG: bool = True

    # MongoDB
    MONGO_URI: str = "mongodb://admin:password@mongodb:27017"
    MONGO_DB_NAME: str = "transcript_db"

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:29092"
    KAFKA_ENCOUNTER_TOPIC: str = "encounter-events"
    KAFKA_SUMMARIZATION_TOPIC: str = "encounter-summarization"
    KAFKA_CONSUMER_GROUP: str = "encounter-summarizer-group"
    KAFKA_MAX_CONCURRENT_TASKS: int = 5  # Bounded worker concurrency pool

    # LiteLLM Configuration
    AI_MODEL: str = "ollama/qwen2.5-coder:7b"
    AI_API_BASE: Optional[str] = "http://host.docker.internal:11434"
    GEMINI_API_KEY: Optional[str] = None
    OPENAI_API_KEY: Optional[str] = None

    # Redis Configuration
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_CACHE_TTL_SECONDS: int = 3600  # 1 hour default TTL



settings = Settings()
