import asyncio
import json
from typing import List, Dict, Any, Optional
from confluent_kafka import Producer
from ..config import settings


class KafkaStreamer:
    """
    Producer service that streams synthetic encounter events directly into Kafka topic.
    """

    def __init__(
        self,
        bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS,
        topic: str = settings.KAFKA_ENCOUNTER_TOPIC
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer: Optional[Producer] = None

    def _get_producer(self) -> Optional[Producer]:
        if self.producer is None:
            try:
                self.producer = Producer({
                    'bootstrap.servers': self.bootstrap_servers,
                    'client.id': 'event-streamer-producer',
                    'acks': 'all'
                })
            except Exception as e:
                print(f"[KafkaStreamer Warning] Could not initialize Kafka Producer: {e}")
                self.producer = None
        return self.producer

    async def stream_events(self, events: List[Dict[str, Any]], delay_ms: int = 200) -> int:
        """
        Emits list of events to Kafka topic partitioned by encounter_id with simulated delay.
        Returns count of successfully emitted events.
        """
        producer = self._get_producer()
        if not producer:
            print("[KafkaStreamer Error] Producer unavailable.")
            return 0

        sent_count = 0
        for event in events:
            try:
                key = str(event.get("encounter_id", "")).encode('utf-8')
                value = json.dumps(event).encode('utf-8')
                producer.produce(topic=self.topic, key=key, value=value)
                producer.poll(0)
                sent_count += 1
            except Exception as e:
                print(f"[KafkaStreamer Error] Failed to publish event {event.get('event_id')}: {e}")

            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000.0)

        producer.flush(timeout=5)
        return sent_count
