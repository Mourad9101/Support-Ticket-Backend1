from motor.motor_asyncio import AsyncIOMotorClient
import os
from typing import Optional

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongodb:27017")
DB_NAME = "support_saas"

_client: Optional[AsyncIOMotorClient] = None
_indexes_ready: bool = False
_client_loop = None


async def get_db():
    """
    Returns a database instance.
    """
    global _client
    global _indexes_ready
    import asyncio
    global _client_loop
    loop = asyncio.get_running_loop()
    if _client is None or _client_loop is None or _client_loop != loop:
        _client = AsyncIOMotorClient(MONGO_URL)
        _client_loop = loop
        _indexes_ready = False
    db = _client[DB_NAME]
    if not _indexes_ready:
        await _ensure_indexes(db)
        _indexes_ready = True
    return db


async def _ensure_indexes(db) -> None:
    await db.tickets.create_index([
        ("tenant_id", 1),
        ("external_id", 1),
    ], unique=True)
    await db.tickets.create_index([("tenant_id", 1), ("created_at", -1)])
    await db.tickets.create_index("created_at", expireAfterSeconds=2592000)
    await db.locks.create_index("tenant_id", unique=True)


async def create_indexes() -> None:
    db = await get_db()
    await db.tickets.create_index([
        ("tenant_id", 1),
        ("external_id", 1),
    ], unique=True)
    await db.tickets.create_index([("tenant_id", 1), ("created_at", -1)])
    await db.tickets.create_index("created_at", expireAfterSeconds=2592000)
    await db.locks.create_index("tenant_id", unique=True)
