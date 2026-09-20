import asyncio
import json
from typing import Optional, Dict, Any, List
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from ..config import settings


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS):
        self.bootstrap_servers = bootstrap_servers
        self.producer: Optional[Producer] = None

    async def start(self) -> None:
        try:
            self.producer = Producer({
                'bootstrap.servers': self.bootstrap_servers,
                'client.id': 'encounter-service-producer',
                'acks': 'all'
            })
            # Ensure Kafka topics exist proactively
            await self._ensure_topics_exist([
                settings.KAFKA_SUMMARIZATION_TOPIC,
                f"{settings.KAFKA_SUMMARIZATION_TOPIC}-dlq",
                settings.KAFKA_ENCOUNTER_TOPIC
            ])
        except Exception as e:
            print(f"Warning: Kafka Producer initialization skipped: {e}")

    async def _ensure_topics_exist(self, topic_names: List[str]) -> None:
        """Proactively creates topics if they do not already exist on the broker."""
        def _create():
            try:
                admin = AdminClient({'bootstrap.servers': self.bootstrap_servers})
                new_topics = [NewTopic(name, num_partitions=1, replication_factor=1) for name in topic_names]
                futures = admin.create_topics(new_topics)
                for topic, f in futures.items():
                    try:
                        f.result()
                        print(f"[Kafka Admin] Topic '{topic}' initialized.")
                    except Exception:
                        pass  # Already exists or broker managed
            except Exception as e:
                print(f"[Kafka Admin Warning] Could not check/create topics: {e}")

        await asyncio.to_thread(_create)

    async def stop(self) -> None:
        if self.producer:
            self.producer.flush(timeout=5)

    async def publish_summarization_job(self, encounter_id: str, payload: Dict[str, Any]) -> bool:
        if not self.producer:
            return False
        try:
            self.producer.produce(
                topic=settings.KAFKA_SUMMARIZATION_TOPIC,
                key=encounter_id.encode('utf-8'),
                value=json.dumps(payload).encode('utf-8')
            )
            self.producer.poll(0)
            return True
        except Exception as e:
            print(f"Failed to publish to Kafka: {e}")
            return False
