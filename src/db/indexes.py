from src.db.mongo import get_db
import pymongo


async def create_indexes():
    """
    Create MongoDB indexes required for common query patterns and to keep
    the dataset manageable over time (e.g. compound indexes, unique
    constraints, TTL on old data).
    """
    db = await get_db()
    tickets = db.tickets

    await tickets.create_index(
        [("tenant_id", pymongo.ASCENDING), ("external_id", pymongo.ASCENDING)],
        unique=True
    )
    await tickets.create_index([
        ("tenant_id", pymongo.ASCENDING),
        ("created_at", pymongo.DESCENDING)
    ])
    await tickets.create_index(
        [("created_at", pymongo.ASCENDING)],
        expireAfterSeconds=60 * 60 * 24 * 30
    )

    # ingestion_jobs 컬렉션 인덱스
    ingestion_jobs = db.ingestion_jobs
    await ingestion_jobs.create_index([("tenant_id", pymongo.ASCENDING)])
    await ingestion_jobs.create_index([("status", pymongo.ASCENDING)])

    # ingestion_logs 컬렉션 인덱스
    ingestion_logs = db.ingestion_logs
    await ingestion_logs.create_index([("tenant_id", pymongo.ASCENDING)])
    await ingestion_logs.create_index([("job_id", pymongo.ASCENDING)])

    locks = db.locks
    await locks.create_index([("tenant_id", pymongo.ASCENDING)], unique=True)


# ============================================================
# Hint: Example of good index design
# ============================================================
# The commented-out indexes below illustrate better patterns.
# To address Debug Task E, replace the inefficient indexes above
# with indexes that follow these patterns.
#
# # Unique index for idempotency
# await tickets.create_index(
#     [("tenant_id", pymongo.ASCENDING), ("external_id", pymongo.ASCENDING)],
#     unique=True
# )
#
# # Efficient composite index (tenant_id first, then created_at)
# await tickets.create_index([
#     ("tenant_id", pymongo.ASCENDING),
#     ("created_at", pymongo.DESCENDING)
# ])
#
# # Composite index for multi-condition queries
# await tickets.create_index([
#     ("tenant_id", pymongo.ASCENDING),
#     ("status", pymongo.ASCENDING),
#     ("created_at", pymongo.DESCENDING)
# ])
#
# # TTL index (automatic cleanup of old data)
# await tickets.create_index(
#     [("created_at", pymongo.ASCENDING)],
#     expireAfterSeconds=60 * 60 * 24 * 90  # 90 days
# )
