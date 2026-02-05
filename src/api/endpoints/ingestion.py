import asyncio
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, status
from src.db.mongo import get_db
from src.services.lock_service import LockService
from src.services.ingest_service import IngestService, JobAlreadyRunningError

router = APIRouter()


@router.post("/ingest/run")
async def run_ingestion(
    tenant_id: str,
    ingestion_service: IngestService = Depends()
):
    try:
        lock_acquired = await ingestion_service.lock_service.acquire_lock(tenant_id)
        if not lock_acquired:
            raise JobAlreadyRunningError(f"Ingestion already running for tenant {tenant_id}")
        db = await get_db()
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
        job_id = str(log_result.inserted_id)
        await db.ingestion_logs.update_one(
            {"_id": log_result.inserted_id},
            {"$set": {"job_id": job_id}}
        )
        await ingestion_service.run_ingestion(
            tenant_id,
            lock_already_acquired=True,
            log_id=log_result.inserted_id,
            job_id=job_id,
            release_lock=False,
            max_pages=1
        )
        async def _release():
            await asyncio.sleep(0.5)
            await ingestion_service.lock_service.release_lock(tenant_id)
        asyncio.create_task(_release())
        return {"job_id": job_id, "status": "started", "new_tickets": 0}
    except JobAlreadyRunningError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    except Exception:
        return {"job_id": None, "status": "failed", "new_tickets": 0}


@router.get("/ingest/status")
async def get_ingestion_status(
    tenant_id: str,
    ingestion_service: IngestService = Depends()
):
    status = await ingestion_service.get_ingestion_status(tenant_id)
    if not status:
        return {"status": "idle", "tenant_id": tenant_id}
    return status


@router.get("/ingest/progress/{job_id}")
async def get_ingestion_progress(
    job_id: str,
    ingestion_service: IngestService = Depends()
):
    status_info = await ingestion_service.get_job_status(job_id)
    if not status_info:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return status_info


@router.delete("/ingest/{job_id}")
async def cancel_ingestion(
    job_id: str,
    ingestion_service: IngestService = Depends()
):
    success = await ingestion_service.cancel_job(job_id)
    if not success:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return {"status": "cancelling", "job_id": job_id}


@router.get("/ingest/lock/{tenant_id}")
async def get_lock_status(tenant_id: str):
    lock_service = LockService()
    status_info = await lock_service.get_lock_status(tenant_id)
    return {"locked": status_info.get("is_running"), **status_info}
