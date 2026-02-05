from datetime import datetime
from typing import Any, Dict, List
import httpx
from pymongo import UpdateOne
from src.db.mongo import get_db
from src.db.models import TicketInDB
from src.services.classify_service import ClassifyService


class IngestionService:
    def __init__(self):
        self.external_api_url = "http://mock-external-api:9000/external/support-tickets"
        self.classify_service = ClassifyService()

    def _to_ticket_model(self, raw: Dict[str, Any], tenant_id: str) -> TicketInDB:
        classification = self.classify_service.classify(
            raw.get("message", ""),
            raw.get("subject", "")
        )
        payload = {
            **raw,
            "tenant_id": tenant_id,
            "urgency": classification["urgency"],
            "sentiment": classification["sentiment"],
            "requires_action": classification["requires_action"],
        }
        if hasattr(TicketInDB, "model_validate"):
            return TicketInDB.model_validate(payload)
        return TicketInDB.parse_obj(payload)

    async def run_ingestion(self, tenant_id: str) -> dict:
        db = await get_db()
        page = 1
        page_size = 50
        total_processed = 0

        async with httpx.AsyncClient(timeout=15) as client:
            while True:
                response = await client.get(
                    self.external_api_url,
                    params={"page": page, "page_size": page_size}
                )
                response.raise_for_status()
                payload = response.json()

                tickets: List[Dict[str, Any]] = payload.get("tickets", [])
                if not tickets:
                    break

                operations: List[UpdateOne] = []
                for ticket in tickets:
                    ticket_model = self._to_ticket_model(ticket, tenant_id)
                    ticket_doc = ticket_model.model_dump(by_alias=False) if hasattr(ticket_model, "model_dump") else ticket_model.dict(by_alias=False)

                    operations.append(
                        UpdateOne(
                            {"external_id": ticket["id"]},
                            {"$set": ticket_doc, "$setOnInsert": {"inserted_at": datetime.utcnow()}},
                            upsert=True
                        )
                    )

                if operations:
                    await db.tickets.bulk_write(operations, ordered=False)

                total_processed += len(tickets)
                page += 1

        return {"tenant_id": tenant_id, "processed": total_processed}
