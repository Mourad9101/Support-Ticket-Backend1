from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import asyncio
import httpx
import time
from pymongo import UpdateOne, InsertOne
from src.db.mongo import get_db
from src.db.models import TicketInDB
from src.services.classify_service import ClassifyService
from src.services.notify_service import NotifyService
from src.services.lock_service import LockService
from src.services.rate_limiter import global_rate_limiter
from src.services.sync_service import SyncService

_ingestion_cache = {}


class JobAlreadyRunningError(Exception):
    pass


class IngestService:
    def __init__(self):
        self.external_api_url = "http://mock-external-api:9000/external/support-tickets"
        self.classify_service = ClassifyService()
        self.notify_service = NotifyService()
        self.semaphore = asyncio.Semaphore(10)
        self.lock_service = LockService()
        self.sync_service = SyncService()

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

    async def _process_ticket(
        self,
        ticket: Dict[str, Any],
        tenant_id: str,
        existing_doc: Optional[Dict[str, Any]]
    ) -> tuple[Optional[UpdateOne], Optional[InsertOne]]:
        async with self.semaphore:
            ticket_model = self._to_ticket_model(ticket, tenant_id)
            ticket_doc = (
                ticket_model.model_dump(by_alias=False)
                if hasattr(ticket_model, "model_dump")
                else ticket_model.dict(by_alias=False)
            )

            incoming_updated_at = ticket.get("updated_at") or ticket.get("created_at")
            if incoming_updated_at is not None:
                ticket_doc["updated_at"] = incoming_updated_at
            ticket_doc["deleted_at"] = None

            if ticket_model.urgency == "high":
                asyncio.create_task(
                    self.notify_service.send_notification(
                        ticket_id=ticket["id"],
                        tenant_id=tenant_id,
                        urgency=ticket_model.urgency,
                        reason="high_urgency"
                    )
                )

            filter_doc = {"tenant_id": tenant_id, "external_id": ticket["id"]}

            if existing_doc is None:
                update_op = UpdateOne(
                    filter_doc,
                    {"$set": ticket_doc, "$setOnInsert": {"inserted_at": datetime.now(timezone.utc).replace(tzinfo=None)}},
                    upsert=True
                )
                history_op = InsertOne(
                    {
                        "ticket_id": ticket["id"],
                        "tenant_id": tenant_id,
                        "action": "created",
                        "changes": None,
                        "recorded_at": datetime.now(timezone.utc).replace(tzinfo=None),
                    }
                )
                return update_op, history_op

            existing_updated_at = existing_doc.get("updated_at") or existing_doc.get("created_at")
            if isinstance(incoming_updated_at, datetime) and isinstance(existing_updated_at, datetime):
                if incoming_updated_at <= existing_updated_at:
                    return None, None

            diffs = self.sync_service.compute_changes(
                existing_doc,
                ticket_doc,
                list(ticket_doc.keys())
            )

            if not diffs:
                return None, None

            update_op = UpdateOne(
                filter_doc,
                {"$set": ticket_doc, "$setOnInsert": {"inserted_at": datetime.now(timezone.utc).replace(tzinfo=None)}},
                upsert=True
            )
            history_op = InsertOne(
                {
                    "ticket_id": ticket["id"],
                    "tenant_id": tenant_id,
                    "action": "updated",
                    "changes": diffs,
                    "recorded_at": datetime.now(timezone.utc).replace(tzinfo=None),
                }
            )
            return update_op, history_op

    async def run_ingestion(
        self,
        tenant_id: str,
        lock_already_acquired: bool = False,
        log_id: Optional[object] = None,
        job_id: Optional[str] = None,
        ready_event: Optional[asyncio.Event] = None,
        release_lock: bool = True,
        max_pages: Optional[int] = None
    ) -> dict:
        db = await get_db()
        page = 1
        page_size = 50
        total_processed = 0
        cancelled = False
        seen_ids: set[str] = set()
        start_time = time.monotonic()
        if lock_already_acquired:
            lock_acquired = True
        else:
            lock_acquired = await self.lock_service.acquire_lock(tenant_id)
            if not lock_acquired:
                raise JobAlreadyRunningError(f"Ingestion already running for tenant {tenant_id}")

        if log_id is None:
            log_doc = {
                "tenant_id": tenant_id,
                "status": "running",
                "tickets_processed": 0,
                "total_pages": None,
                "processed_pages": 0,
                "job_id": "",
                "start_time": datetime.now(timezone.utc).replace(tzinfo=None),
                "end_time": None,
                "error_message": None,
            }
            log_result = await db.ingestion_logs.insert_one(log_doc)
            log_id = log_result.inserted_id
            job_id = str(log_id)
            await db.ingestion_logs.update_one(
                {"_id": log_id},
                {"$set": {"job_id": job_id}}
            )

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                try:
                    while True:
                        log_state = await db.ingestion_logs.find_one(
                            {"_id": log_id},
                            {"status": 1}
                        )
                        if log_state and log_state.get("status") == "cancelling":
                            cancelled = True
                            await db.ingestion_logs.update_one(
                                {"_id": log_id},
                                {
                                    "$set": {
                                        "status": "cancelled",
                                        "end_time": datetime.now(timezone.utc).replace(tzinfo=None),
                                        "tickets_processed": total_processed,
                                    }
                                }
                            )
                            break

                        await self.lock_service.refresh_lock(tenant_id)
                        max_retries = 3
                        backoff_seconds = 1
                        response = None

                        attempt = 1
                        while attempt <= max_retries:
                            try:
                                await global_rate_limiter.acquire()
                                response = await client.get(
                                    self.external_api_url,
                                    params={"page": page, "page_size": page_size}
                                )
                                response.raise_for_status()
                                break
                            except httpx.HTTPStatusError as exc:
                                status_code = exc.response.status_code
                                if status_code == 429:
                                    retry_after = exc.response.headers.get("Retry-After")
                                    try:
                                        wait_seconds = float(retry_after) if retry_after is not None else 60.0
                                    except ValueError:
                                        wait_seconds = 60.0
                                    await asyncio.sleep(wait_seconds)
                                    continue
                                if 500 <= status_code <= 599 and attempt < max_retries:
                                    await asyncio.sleep(backoff_seconds)
                                    backoff_seconds *= 2
                                    attempt += 1
                                    continue
                                raise
                            except httpx.RequestError:
                                if attempt < max_retries:
                                    await asyncio.sleep(backoff_seconds)
                                    backoff_seconds *= 2
                                    attempt += 1
                                    continue
                                raise
                            attempt += 1

                        if response is None:
                            raise RuntimeError("HTTP request failed without a response.")

                        payload = response.json()
                        tickets: List[Dict[str, Any]] = payload.get("tickets", [])
                        if not tickets:
                            if ready_event and not ready_event.is_set():
                                ready_event.set()
                            break

                        ids = [ticket.get("id") or ticket.get("external_id") for ticket in tickets]
                        ids = [ticket_id for ticket_id in ids if ticket_id is not None]
                        seen_ids.update(ids)

                        existing_docs = []
                        if ids:
                            existing_docs = await db.tickets.find(
                                {"external_id": {"$in": ids}, "tenant_id": tenant_id}
                            ).to_list(length=len(ids))
                        existing_map = {doc["external_id"]: doc for doc in existing_docs}

                        tasks = [
                            self._process_ticket(ticket, tenant_id, existing_map.get(ticket.get("id") or ticket.get("external_id")))
                            for ticket in tickets
                        ]
                        results = await asyncio.gather(*tasks)
                        ticket_ops: List[UpdateOne] = [op for op, _ in results if op is not None]
                        history_ops: List[InsertOne] = [op for _, op in results if op is not None]

                        if ticket_ops:
                            await db.tickets.bulk_write(ticket_ops, ordered=False)
                        if history_ops:
                            await db["ticket_history"].bulk_write(history_ops, ordered=False)

                        if ready_event and not ready_event.is_set():
                            ready_event.set()

                        total_processed += len(tickets)
                        await db.ingestion_logs.update_one(
                            {"_id": log_id},
                            {
                                "$set": {
                                    "processed_pages": page,
                                    "total_pages": payload.get("total_pages"),
                                    "tickets_processed": total_processed,
                                }
                            }
                        )
                        if max_pages is not None and page >= max_pages:
                            break
                        page += 1
                except Exception as exc:
                    if ready_event and not ready_event.is_set():
                        ready_event.set()
                    await db.ingestion_logs.update_one(
                        {"_id": log_id},
                        {
                            "$set": {
                                "status": "FAILED",
                                "end_time": datetime.now(timezone.utc).replace(tzinfo=None),
                                "error_message": str(exc),
                                "tickets_processed": total_processed,
                            }
                        }
                    )
                    raise

                if not cancelled:
                    now = datetime.now(timezone.utc).replace(tzinfo=None)
                    if seen_ids:
                        await db.tickets.update_many(
                            {"tenant_id": tenant_id, "external_id": {"$nin": list(seen_ids)}},
                            {"$set": {"deleted_at": now}}
                        )
                    else:
                        await db.tickets.update_many(
                            {"tenant_id": tenant_id},
                            {"$set": {"deleted_at": now}}
                        )
                    _ingestion_cache.clear()
                    await db.ingestion_logs.update_one(
                        {"_id": log_id},
                        {
                            "$set": {
                                "status": "SUCCESS",
                                "end_time": datetime.now(timezone.utc).replace(tzinfo=None),
                                "tickets_processed": total_processed,
                            }
                        }
                    )
        finally:
            if lock_acquired and release_lock:
                elapsed = time.monotonic() - start_time
                if elapsed < 0.5:
                    await asyncio.sleep(0.5 - elapsed)
                await self.lock_service.release_lock(tenant_id)

        return {"tenant_id": tenant_id, "processed": total_processed, "job_id": job_id}

    async def get_job_status(self, job_id: str) -> Optional[dict]:
        db = await get_db()
        from bson import ObjectId

        try:
            _id = ObjectId(job_id)
        except Exception:
            return None

        log = await db.ingestion_logs.find_one({"_id": _id})
        if not log:
            return None

        return {
            "job_id": str(log.get("_id")),
            "tenant_id": log.get("tenant_id"),
            "status": log.get("status"),
            "progress": None,
            "total_pages": log.get("total_pages"),
            "processed_pages": log.get("processed_pages", 0),
            "started_at": log.get("start_time").isoformat() if log.get("start_time") else None,
            "ended_at": log.get("end_time").isoformat() if log.get("end_time") else None,
        }

    async def cancel_job(self, job_id: str) -> bool:
        db = await get_db()
        from bson import ObjectId

        try:
            _id = ObjectId(job_id)
        except Exception:
            return False

        result = await db.ingestion_logs.update_one(
            {"_id": _id, "status": "running"},
            {"$set": {"status": "cancelling"}}
        )
        return result.modified_count > 0

    async def get_ingestion_status(self, tenant_id: str) -> Optional[dict]:
        db = await get_db()
        log = await db.ingestion_logs.find_one(
            {"tenant_id": tenant_id, "status": "running"},
            sort=[("start_time", -1)]
        )
        if not log:
            return None

        return {
            "job_id": str(log.get("_id")),
            "tenant_id": tenant_id,
            "status": log.get("status"),
            "started_at": log.get("start_time").isoformat() if log.get("start_time") else None,
        }
