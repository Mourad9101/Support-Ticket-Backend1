"""
Task 12: Data synchronization service.

Responsible for synchronizing ticket data with the external API.

Requirements:
1. Use the ticket `updated_at` field to update only tickets that have changed.
2. Apply soft delete (`deleted_at` field) for tickets deleted in the external system.
3. Record change history in the `ticket_history` collection.
"""

from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
from src.db.mongo import get_db


class SyncService:
    """
    Data synchronization service.
    """

    HISTORY_COLLECTION = "ticket_history"

    async def sync_ticket(self, external_ticket: dict, tenant_id: str) -> dict:
        """
        Synchronize a single ticket.
        """
        db = await get_db()
        external_id = external_ticket.get("id") or external_ticket.get("external_id")
        if not external_id:
            return {"action": "unchanged", "ticket_id": None, "changes": []}

        existing = await db.tickets.find_one({"tenant_id": tenant_id, "external_id": external_id})

        incoming_updated_at = self._normalize_dt(external_ticket.get("updated_at") or external_ticket.get("created_at"))

        if existing:
            existing_updated_at = self._normalize_dt(existing.get("updated_at") or existing.get("created_at"))
            if incoming_updated_at and existing_updated_at and incoming_updated_at <= existing_updated_at:
                return {"action": "unchanged", "ticket_id": external_id, "changes": []}

        update_doc = {"tenant_id": tenant_id, "external_id": external_id}
        if "subject" in external_ticket:
            update_doc["subject"] = external_ticket.get("subject")
        if "message" in external_ticket:
            update_doc["message"] = external_ticket.get("message")
        if "created_at" in external_ticket:
            update_doc["created_at"] = external_ticket.get("created_at")
        if incoming_updated_at is not None:
            update_doc["updated_at"] = incoming_updated_at

        if existing:
            fields = [key for key in update_doc.keys() if key not in {"tenant_id", "external_id"}]
            changes = self.compute_changes(existing, update_doc, fields)
            if not changes:
                return {"action": "unchanged", "ticket_id": external_id, "changes": []}

            await db.tickets.update_one(
                {"tenant_id": tenant_id, "external_id": external_id},
                {"$set": update_doc}
            )
            await self.record_history(external_id, tenant_id, "updated", changes)
            return {"action": "updated", "ticket_id": external_id, "changes": list(changes.keys())}

        await db.tickets.update_one(
            {"tenant_id": tenant_id, "external_id": external_id},
            {"$set": update_doc, "$setOnInsert": {"inserted_at": datetime.now(timezone.utc).replace(tzinfo=None)}},
            upsert=True
        )
        await self.record_history(external_id, tenant_id, "created", None)
        return {"action": "created", "ticket_id": external_id, "changes": []}

    async def mark_deleted(self, tenant_id: str, external_ids: List[str]) -> int:
        """
        Soft delete tickets and record history.
        """
        if not external_ids:
            return 0
        db = await get_db()
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        result = await db.tickets.update_many(
            {"tenant_id": tenant_id, "external_id": {"$in": external_ids}},
            {"$set": {"deleted_at": now}}
        )
        for ticket_id in external_ids:
            await self.record_history(ticket_id, tenant_id, "deleted", None)
        return result.modified_count

    async def detect_deleted_tickets(self, tenant_id: str, external_ids: List[str]) -> List[str]:
        """
        Detect tickets that appear to have been deleted externally.
        """
        db = await get_db()
        cursor = db.tickets.find(
            {"tenant_id": tenant_id, "external_id": {"$nin": external_ids}},
            {"external_id": 1}
        )
        docs = await cursor.to_list(length=10000)
        return [doc.get("external_id") for doc in docs if doc.get("external_id")]

    async def record_history(
        self,
        ticket_id: str,
        tenant_id: str,
        action: str,
        changes: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Record a change history entry.

        Args:
            ticket_id: Ticket identifier.
            tenant_id: Tenant identifier.
            action: "created" | "updated" | "deleted".
            changes: Change details (field -> {old: ..., new: ...}).

        Returns:
            ID of the created history document.
        """
        db = await get_db()

        history_doc = {
            "ticket_id": ticket_id,
            "tenant_id": tenant_id,
            "action": action,
            "changes": changes or {},
            "recorded_at": datetime.now(timezone.utc).replace(tzinfo=None)
        }

        result = await db[self.HISTORY_COLLECTION].insert_one(history_doc)
        return str(result.inserted_id)

    async def get_ticket_history(
        self,
        ticket_id: str,
        tenant_id: str,
        limit: int = 50
    ) -> List[dict]:
        """
        Retrieve ticket change history.

        Args:
            ticket_id: Ticket identifier.
            tenant_id: Tenant identifier.
            limit: Maximum number of records to return.

        Returns:
            List of history entries in reverse chronological order.
        """
        db = await get_db()

        cursor = db[self.HISTORY_COLLECTION].find(
            {"ticket_id": ticket_id, "tenant_id": tenant_id}
        ).sort("recorded_at", -1).limit(limit)

        return await cursor.to_list(length=limit)

    def compute_changes(self, old_doc: dict, new_doc: dict, fields: List[str]) -> Dict[str, Any]:
        """
        Compute field-level differences between two documents.

        Args:
            old_doc: Previous version of the document.
            new_doc: New version of the document.
            fields: List of fields to compare.

        Returns:
            A mapping of changed fields to their before/after values:
            {
                "field_name": {"old": ..., "new": ...},
                ...
            }
        """
        changes = {}

        for field in fields:
            old_value = old_doc.get(field)
            new_value = new_doc.get(field)

            if old_value != new_value:
                changes[field] = {
                    "old": old_value,
                    "new": new_value
                }

        return changes

    def _normalize_dt(self, value):
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            # Mongo stores datetimes with millisecond precision
            value = value.replace(microsecond=(value.microsecond // 1000) * 1000)
        return value
