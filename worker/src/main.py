from contextlib import asynccontextmanager
import logging
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse, Response

try:
    from worker.src import config
    from worker.src.heartbeat import HeartbeatClient
    from worker.src.storage import StorageEngine
except (ImportError, ModuleNotFoundError):
    from src import config
    from src.heartbeat import HeartbeatClient
    from src.storage import StorageEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("worker.api")

storage = StorageEngine(config.DATA_DIR)
heartbeat_client = HeartbeatClient()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Worker %s on port %d...", config.WORKER_ID, config.WORKER_PORT)
    await heartbeat_client.start()
    yield
    logger.info("Shutting down Worker %s...", config.WORKER_ID)
    await heartbeat_client.stop()


app = FastAPI(
    title=f"Mini Distributed Cloud Object Store - Worker ({config.WORKER_ID})",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"error": str(exc.detail)})


@app.get("/health")
async def health_check():
    return {"status": "ok", "worker_id": config.WORKER_ID}


@app.put("/internal/chunks/{chunk_id}", status_code=status.HTTP_200_OK)
async def store_chunk(chunk_id: str, request: Request):
    """
    Store a raw binary chunk in local persistent storage.
    """
    chunk_bytes = await request.body()
    try:
        written = storage.store_chunk(chunk_id, chunk_bytes)
        return {
            "status": "stored",
            "chunk_id": chunk_id,
            "size": written,
            "worker_id": config.WORKER_ID,
        }
    except Exception as e:
        logger.error("Error storing chunk %s: %s", chunk_id, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to store chunk {chunk_id}: {str(e)}",
        )


@app.get("/internal/chunks/{chunk_id}")
async def get_chunk(chunk_id: str):
    """
    Retrieve raw chunk binary from local persistent storage.
    """
    chunk_bytes = storage.get_chunk(chunk_id)
    if chunk_bytes is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Chunk {chunk_id} not found on worker {config.WORKER_ID}",
        )
    return Response(
        content=chunk_bytes,
        media_type="application/octet-stream",
        headers={"Content-Length": str(len(chunk_bytes))},
    )


@app.delete("/internal/chunks/{chunk_id}")
async def delete_chunk(chunk_id: str):
    """
    Delete a chunk from local persistent storage.
    """
    deleted = storage.delete_chunk(chunk_id)
    if not deleted and not storage.chunk_exists(chunk_id):
        # Already deleted or didn't exist
        return {"status": "not_found_or_already_deleted", "chunk_id": chunk_id}
    return {"status": "deleted", "chunk_id": chunk_id}


@app.get("/internal/workers/{worker_id}/chunks")
@app.get("/internal/chunks")
async def list_chunks(worker_id: Optional[str] = None):
    """
    Retrieve information about chunks stored on this worker.
    """
    chunks = storage.list_chunks()
    return {
        "worker_id": config.WORKER_ID,
        "chunk_count": len(chunks),
        "chunks": chunks,
    }
