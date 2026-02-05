import httpx
import asyncio
import random
from src.core.logging import logger

class NotifyService:
    def __init__(self):
        self.notify_url = "http://mock-external-api:9000/notify"

    async def send_notification(self, ticket_id: str, tenant_id: str, urgency: str, reason: str):
        """
        Sends a notification to the external service.
        
        This should include some form of retry/backoff and must not block
        normal request handling. External retry helper libraries should not
        be required for this exercise.
        """
        payload = {
            "ticket_id": ticket_id,
            "tenant_id": tenant_id,
            "urgency": urgency,
            "reason": reason
        }
        max_attempts = 3
        base_backoff = 0.5

        async with httpx.AsyncClient(timeout=10) as client:
            for attempt in range(1, max_attempts + 1):
                try:
                    response = await client.post(self.notify_url, json=payload)
                    response.raise_for_status()
                    return
                except httpx.HTTPStatusError as exc:
                    status = exc.response.status_code
                    if status >= 500 or status == 429:
                        wait = base_backoff * (2 ** (attempt - 1))
                        await asyncio.sleep(wait)
                        continue
                    logger.warning("Notification failed: %s", exc)
                    return
                except httpx.HTTPError as exc:
                    wait = base_backoff * (2 ** (attempt - 1))
                    logger.warning("Notification error: %s", exc)
                    await asyncio.sleep(wait)
