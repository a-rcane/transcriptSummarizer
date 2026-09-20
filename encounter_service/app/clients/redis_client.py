import json
from typing import Optional, Any
from redis.asyncio import Redis
from ..config import settings


class RedisClient:
    """Async Redis caching client with JSON serialization and graceful fallback."""

    def __init__(
        self,
        host: str = settings.REDIS_HOST,
        port: int = settings.REDIS_PORT,
        default_ttl: int = settings.REDIS_CACHE_TTL_SECONDS
    ):
        self.host = host
        self.port = port
        self.default_ttl = default_ttl
        self.client: Optional[Redis] = None

    async def connect(self) -> None:
        try:
            self.client = Redis(
                host=self.host,
                port=self.port,
                decode_responses=True,
                socket_timeout=2.0
            )
            await self.client.ping()
            print(f"[Redis] Connected successfully to {self.host}:{self.port}")
        except Exception as e:
            print(f"[Redis Warning] Connection failed: {e}. Running without cache.")
            self.client = None

    async def close(self) -> None:
        if self.client:
            await self.client.close()

    async def get_json(self, key: str) -> Optional[dict]:
        if not self.client:
            return None
        try:
            data = await self.client.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            print(f"[Redis Error] get_json({key}) failed: {e}")
            return None

    async def set_json(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        if not self.client:
            return False
        try:
            serialized = json.dumps(value, default=str)
            await self.client.set(key, serialized, ex=ttl or self.default_ttl)
            return True
        except Exception as e:
            print(f"[Redis Error] set_json({key}) failed: {e}")
            return False

    async def delete(self, key: str) -> bool:
        if not self.client:
            return False
        try:
            await self.client.delete(key)
            return True
        except Exception as e:
            print(f"[Redis Error] delete({key}) failed: {e}")
            return False
