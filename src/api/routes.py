from fastapi import APIRouter
from src.api.endpoints.ingestion import router as ingestion_router
from src.api.endpoints.stats import router as stats_router
from src.api.endpoints.health import router as health_router
from src.api.endpoints.circuit import router as circuit_router
from src.api.endpoints.tickets import router as tickets_router

router = APIRouter()

router.include_router(ingestion_router)
router.include_router(stats_router)
router.include_router(health_router)
router.include_router(circuit_router)
router.include_router(tickets_router)
