from typing import Optional
from fastapi import APIRouter, Depends, Query
from src.services.ticket_service import TicketService
from src.services.sync_service import SyncService

router = APIRouter()


@router.get("/tickets")
async def list_tickets_top_level(
    tenant_id: str = Query(...),
    status: Optional[str] = None,
    urgency: Optional[str] = None,
    source: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    ticket_service: TicketService = Depends()
):
    filters = {"status": status, "urgency": urgency, "source": source}
    return await ticket_service.get_tickets(tenant_id, filters, page, page_size)


@router.get("/tenants/{tenant_id}/tickets")
async def list_tickets(
    tenant_id: str,
    status: Optional[str] = None,
    urgency: Optional[str] = None,
    source: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    ticket_service: TicketService = Depends()
):
    filters = {"status": status, "urgency": urgency, "source": source}
    return await ticket_service.get_tickets(tenant_id, filters, page, page_size)


@router.get("/tickets/{ticket_id}/history")
async def get_ticket_history(
    ticket_id: str,
    tenant_id: str,
    limit: int = Query(50, ge=1, le=200),
    sync_service: SyncService = Depends()
):
    history = await sync_service.get_ticket_history(ticket_id, tenant_id, limit)
    sanitized = []
    for entry in history:
        if "_id" in entry:
            entry["id"] = str(entry.pop("_id"))
        sanitized.append(entry)
    return {"ticket_id": ticket_id, "history": sanitized}
