from typing import Dict, List, Optional
from src.db.mongo import get_db
from src.db.models import TicketResponse


class TicketService:
    async def get_tickets(
        self,
        tenant_id: str,
        filters: Dict[str, Optional[str]],
        page: int,
        page_size: int
    ) -> dict:
        db = await get_db()

        query: Dict[str, Optional[str]] = {"tenant_id": tenant_id}
        if filters.get("status"):
            query["status"] = filters["status"]
        if filters.get("urgency"):
            query["urgency"] = filters["urgency"]
        if filters.get("source"):
            query["source"] = filters["source"]

        cursor = db.tickets.find(query).skip((page - 1) * page_size).limit(page_size)
        docs = await cursor.to_list(length=page_size)
        total = await db.tickets.count_documents(query)

        for doc in docs:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])

        return {
            "total": total,
            "items": [TicketResponse(**doc) for doc in docs]
        }
