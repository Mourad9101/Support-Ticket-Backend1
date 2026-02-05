from fastapi import APIRouter, Depends
from src.services.ingestion import IngestionService

router = APIRouter()


@router.post("/ingest/run")
async def run_ingestion(
    tenant_id: str,
    ingestion_service: IngestionService = Depends()
):
    result = await ingestion_service.run_ingestion(tenant_id)
    return {"status": "ingestion_started", "result": result}
