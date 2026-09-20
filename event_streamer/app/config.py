from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    SERVICE_NAME: str = "event-streamer"
    PORT: int = 8001
    DEBUG: bool = True

    # Target Webhook URL on encounter-service
    ENCOUNTER_SERVICE_WEBHOOK_URL: str = "http://encounter-service:8000/webhook"

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:29092"
    KAFKA_ENCOUNTER_TOPIC: str = "encounter-events"

    # Simulation defaults
    DEFAULT_STREAM_INTERVAL_MS: int = 500
    DEFAULT_DUPLICATE_RATE: float = 0.15
    DEFAULT_OUT_OF_ORDER_RATE: float = 0.20


settings = Settings()
