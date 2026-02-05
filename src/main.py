from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import time
from contextlib import asynccontextmanager
from src.api.routes import router
from src.db.mongo import create_indexes

@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_indexes()
    yield

app = FastAPI(title="Support Ticket Analysis System", lifespan=lifespan)

@app.middleware("http")
async def timeout_middleware(request: Request, call_next):
    if request.url.path.endswith("/stats"):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        if process_time > 2.0:
            return JSONResponse(
                status_code=504,
                content={"detail": "Performance Limit Exceeded: Aggregation took too long (> 2s)"}
            )
        return response
    return await call_next(request)

app.include_router(router)
