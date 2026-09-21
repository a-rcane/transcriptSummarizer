import asyncio
import json
from typing import Optional, Set, Any
from confluent_kafka import Consumer, Producer, KafkaError, Message
from ..config import settings
from ..services.summarize_service import SummarizeService


class KafkaSummarizationConsumer:
    """
    High-throughput concurrent Kafka consumer worker.
    
    Features:
    - Bounded concurrency pool using `asyncio.Semaphore`.
    - Non-blocking message dispatch via `asyncio.create_task`.
    - 3-step exponential backoff retry policy for LLM / AI failures.
    - Dead-Letter Queue (DLQ) fallback routing.
    - Graceful draining of in-flight tasks during shutdown.
    """

    def __init__(
        self,
        bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS,
        topic: str = settings.KAFKA_SUMMARIZATION_TOPIC,
        group_id: str = settings.KAFKA_CONSUMER_GROUP,
        max_concurrent_tasks: int = settings.KAFKA_MAX_CONCURRENT_TASKS,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.dlq_topic = f"{topic}-dlq"
        self.group_id = group_id
        self.max_concurrent_tasks = max_concurrent_tasks

        self.consumer: Optional[Consumer] = None
        self.dlq_producer: Optional[Producer] = None
        self.semaphore: Optional[asyncio.Semaphore] = None
        self.running_tasks: Set[asyncio.Task] = set()
        self.is_running: bool = False

    async def start(self, summarize_service: SummarizeService, dlq_repo: Optional[Any] = None) -> None:
        """Starts the concurrent message poll loop using non-blocking thread execution."""
        try:
            self.consumer = Consumer({
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': self.group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            })
            self.dlq_producer = Producer({'bootstrap.servers': self.bootstrap_servers})
            self.consumer.subscribe([self.topic])
        except Exception as e:
            print(f"[Kafka Consumer Warning] Could not initialize Kafka at {self.bootstrap_servers}: {e}. Consumer disabled.")
            return

        self.semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
        self.is_running = True
        print(f"[Kafka Consumer] Started listening on '{self.topic}' with concurrency limit: {self.max_concurrent_tasks}")

        while self.is_running:
            # Acquire semaphore slot before polling next message to enforce backpressure
            await self.semaphore.acquire()

            try:
                # Poll in thread pool so it NEVER blocks the asyncio / uvicorn event loop
                msg: Optional[Message] = await asyncio.to_thread(self.consumer.poll, 0.5)

                if not self.is_running:
                    self.semaphore.release()
                    break

                if msg is None:
                    self.semaphore.release()
                    await asyncio.sleep(0.05)
                    continue

                if msg.error():
                    # Ignore EOF and transient unknown topic error while metadata syncs / topic initializes
                    if msg.error().code() not in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                        print(f"[Kafka Consumer Error] {msg.error()}")
                    self.semaphore.release()
                    await asyncio.sleep(0.5 if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART else 0.1)
                    continue

                # Dispatch message processing as an asynchronous concurrent task
                task = asyncio.create_task(
                    self._handle_message(msg, summarize_service, dlq_repo)
                )
                self.running_tasks.add(task)
                task.add_done_callback(self.running_tasks.discard)
            except asyncio.CancelledError:
                self.semaphore.release()
                break
            except Exception as e:
                self.semaphore.release()
                if self.is_running:
                    print(f"[Kafka Consumer Loop Exception] {e}")
                await asyncio.sleep(0.2)

    async def _handle_message(self, msg: Message, service: SummarizeService, dlq_repo: Optional[Any] = None) -> None:
        """Processes a single Kafka message concurrently and releases its semaphore slot when done."""
        encounter_id = "unknown"
        pid = "unknown"
        try:
            payload = json.loads(msg.value().decode('utf-8'))
            encounter_id = payload.get("encounter_id", "unknown")
            pid = payload.get("pId", "unknown")
            version = payload.get("version", 1)
            transcription = payload.get("transcriptions") or payload.get("transcription", "")

            # Process with 3-step exponential backoff retry
            success = await self._process_with_retry(service, encounter_id, pid, transcription, version=version)

            if not success:
                # Route to Dead-Letter Queue (DLQ)
                print(f"[Kafka DLQ] Moving encounter {encounter_id} v{version} to DLQ '{self.dlq_topic}'")
                if self.dlq_producer:
                    self.dlq_producer.produce(self.dlq_topic, key=msg.key(), value=msg.value())
                    self.dlq_producer.poll(0)

                # Save to persistent DLQ repository
                if dlq_repo:
                    await dlq_repo.save_failed_message(
                        encounter_id=encounter_id,
                        pId=pid,
                        version=version,
                        payload=payload,
                        error_message="AI summarization exhausted 3 retry attempts"
                    )

            # Safely commit message offset if consumer is still active
            if self.consumer and self.is_running:
                try:
                    self.consumer.commit(msg)
                except Exception as ce:
                    print(f"[Kafka Commit Warning] {ce}")
        except Exception as e:
            print(f"[Kafka Task Exception] Error handling message: {e}")
        finally:
            # Release semaphore slot so the consumer loop can poll the next message
            self.semaphore.release()

    async def _process_with_retry(
        self,
        service: SummarizeService,
        enc_id: str,
        pid: str,
        transcription: str,
        version: int = 1,
        retries: int = 3
    ) -> bool:
        for attempt in range(retries):
            try:
                await service.summarize_encounter(enc_id, pid, transcription, version=version)
                return True
            except Exception as err:
                delay = 2 ** attempt  # 1s, 2s, 4s
                print(f"[Retry {attempt + 1}/{retries}] Encounter {enc_id} v{version} failed: {err}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
        return False

    async def stop(self) -> None:
        """Gracefully drains in-flight tasks and closes Kafka connections."""
        print("[Kafka Consumer] Shutting down...")
        self.is_running = False

        # Wait for all currently executing in-flight tasks to finish
        if self.running_tasks:
            print(f"[Kafka Consumer] Waiting for {len(self.running_tasks)} in-flight tasks to drain...")
            await asyncio.gather(*self.running_tasks, return_exceptions=True)

        if self.dlq_producer:
            try:
                self.dlq_producer.flush(timeout=3)
            except Exception:
                pass

        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass
            print("[Kafka Consumer] Closed gracefully.")
