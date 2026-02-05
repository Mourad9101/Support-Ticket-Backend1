from fastapi import APIRouter
from src.services.circuit_breaker import get_circuit_breaker

router = APIRouter()


@router.get("/circuit/notify/status")
async def get_notify_circuit_status():
    cb = get_circuit_breaker("notify_api")
    return cb.get_status()


@router.post("/circuit/notify/reset")
async def reset_notify_circuit():
    cb = get_circuit_breaker("notify_api")
    cb.reset()
    return {"status": "reset"}
