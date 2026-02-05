import pytest
import pytest_asyncio
import httpx
from httpx import ASGITransport
from src.main import app
from src.db.mongo import get_db


class PatchedAsyncClient(httpx.AsyncClient):
    def __init__(self, *args, app=None, **kwargs):
        if app is not None and "transport" not in kwargs:
            kwargs["transport"] = ASGITransport(app=app)
            kwargs.setdefault("base_url", "http://test")
        super().__init__(*args, **kwargs)


httpx.AsyncClient = PatchedAsyncClient


@pytest_asyncio.fixture
async def client():
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture
async def db():
    return await get_db()


@pytest_asyncio.fixture(autouse=True)
async def _clean_collections():
    db = await get_db()
    for name in ("tickets", "ticket_history", "ingestion_logs", "locks"):
        await db[name].delete_many({})
    yield
