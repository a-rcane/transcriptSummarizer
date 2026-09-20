import asyncio
import httpx
from typing import List, Dict, Any
from ..config import settings


class HttpStreamer:
    def __init__(self, target_url: str = settings.ENCOUNTER_SERVICE_WEBHOOK_URL):
        self.target_url = target_url

    async def send_single_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(self.target_url, json=event_data)
            return {
                "status_code": response.status_code,
                "response": response.json() if response.is_success else response.text
            }

    async def stream_batch(self, events: List[Dict[str, Any]], delay_ms: int = 200) -> Dict[str, Any]:
        results = []
        async with httpx.AsyncClient(timeout=10.0) as client:
            for event in events:
                try:
                    res = await client.post(self.target_url, json=event)
                    results.append({
                        "event_id": event["event_id"],
                        "version": event["version"],
                        "status": res.status_code,
                        "data": res.json() if res.is_success else res.text
                    })
                except Exception as e:
                    results.append({"event_id": event["event_id"], "error": str(e)})

                if delay_ms > 0:
                    await asyncio.sleep(delay_ms / 1000.0)

        return {"total_sent": len(events), "results": results}
