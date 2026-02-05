"""
Task 8: Distributed lock service.

Implement a distributed lock using MongoDB atomic operations.
Do not use external distributed lock libraries (redis-lock, pottery, etc.).

Requirements:
1. Prevent concurrent ingestion for the same tenant.
2. Return 409 Conflict when lock acquisition fails.
3. Automatically release locks when they are not refreshed within 60 seconds (zombie lock prevention).
4. Provide lock status inspection APIs.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError
from src.db.mongo import get_db


class LockService:
    """
    MongoDB-based distributed lock service.

    Hints:
    - Use `findOneAndUpdate` with `upsert` to acquire locks.
    - Use TTL-like behaviour for automatic expiration.
    - Acquire/release locks atomically.
    """

    LOCK_COLLECTION = "locks"
    LOCK_TTL_SECONDS = 60

    def __init__(self):
        self._indexes_ready = False

    async def _ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        db = await get_db()
        await db[self.LOCK_COLLECTION].create_index("tenant_id", unique=True)
        self._indexes_ready = True

    async def acquire_lock(self, tenant_id: str) -> bool:
        """
        Attempt to acquire a lock.

        Args:
            tenant_id: ID of the resource to lock (e.g., tenant_id).

        Returns:
            True if lock acquired, False otherwise.

        TODO: Implement:
        - If a non-expired lock already exists, return False.
        - If no lock or an expired lock exists, create/refresh a lock and return True.
        - Use an atomic MongoDB operation.
        """
        await self._ensure_indexes()
        db = await get_db()
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        cutoff_time = now - timedelta(seconds=self.LOCK_TTL_SECONDS)

        try:
            result = await db[self.LOCK_COLLECTION].find_one_and_update(
                {
                    "tenant_id": tenant_id,
                    "$or": [
                        {"locked_at": {"$lte": cutoff_time}},
                        {"locked_at": {"$exists": False}}
                    ]
                },
                {"$set": {"tenant_id": tenant_id, "locked_at": now}},
                upsert=True,
                return_document=ReturnDocument.AFTER
            )
            return result is not None
        except DuplicateKeyError:
            return False

    async def release_lock(self, tenant_id: str) -> bool:
        """
        Release a lock.

        Args:
            tenant_id: ID of the resource to unlock.

        Returns:
            True if lock released, False otherwise.

        TODO: Implement:
        - Only release the lock when `owner_id` matches the stored owner.
        """
        db = await get_db()
        result = await db[self.LOCK_COLLECTION].delete_one({"tenant_id": tenant_id})
        return result.deleted_count > 0

    async def refresh_lock(self, tenant_id: str) -> bool:
        """
        Refresh a lock's TTL to prevent expiration.

        Args:
            tenant_id: ID of the lock to refresh.

        Returns:
            True if lock refreshed, False otherwise.

        TODO: Implement:
        - For long-running jobs, call this periodically to keep the lock alive.
        """
        db = await get_db()
        result = await db[self.LOCK_COLLECTION].update_one(
            {"tenant_id": tenant_id},
            {"$set": {"locked_at": datetime.now(timezone.utc).replace(tzinfo=None)}}
        )
        return result.modified_count > 0

    async def get_lock_status(self, tenant_id: str) -> Optional[dict]:
        """
        Get current lock status for a resource.

        Returns:
            A dict describing the lock or None if no lock exists:
            {
                "is_running": bool,
                "locked_at": datetime
            }
        """
        db = await get_db()
        lock = await db[self.LOCK_COLLECTION].find_one({"tenant_id": tenant_id})

        if not lock:
            return {"is_running": False, "locked_at": None}

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        locked_at = lock.get("locked_at")
        is_running = False
        if locked_at:
            is_running = locked_at > now - timedelta(seconds=self.LOCK_TTL_SECONDS)

        return {"is_running": is_running, "locked_at": locked_at}

    async def cleanup_expired_locks(self) -> int:
        """
        Clean up expired locks (optional helper).

        Returns:
            Number of deleted locks.
        """
        db = await get_db()
        result = await db[self.LOCK_COLLECTION].delete_many({
            "locked_at": {"$lt": datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=self.LOCK_TTL_SECONDS)}
        })
        return result.deleted_count
