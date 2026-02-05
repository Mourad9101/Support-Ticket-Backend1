from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
import httpx
import asyncio
from src.db.mongo import get_db

router = APIRouter()


@router.get("/health")
async def health_check():
    db_status = "up"
    api_status = "up"

    try:
        db = await get_db()
        await db.command("ping")
    except Exception:
        db_status = "down"

    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                await client.get("http://mock-external-api:9000/")
            api_status = "up"
            break
        except Exception:
            api_status = "down"
            if attempt < 2:
                await asyncio.sleep(0.2)

    if db_status == "up":
        return {"status": "ok"}

    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"status": "error", "components": {"db": db_status, "api": api_status}}
    )
