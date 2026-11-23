import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import hashlib
import json
import logging
from typing import Any, Dict, List, Optional
import uuid

from fastapi import (
    FastAPI,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    Response,
    UploadFile,
    status,
)
from fastapi.responses import JSONResponse, Response, StreamingResponse

try:
    from controller.src import config
    from controller.src.database import Database
    from controller.src.journal_client import JournalClient
    from controller.src.manager import ClusterManager
    from controller.src.models import (
        ErrorResponse,
        ObjectDetailResponse,
        ObjectListResponse,
        ObjectSummary,
        WorkerHeartbeatRequest,
        WorkerRegisterRequest,
    )
except (ImportError, ModuleNotFoundError):
    from src import config
    from src.database import Database
    from src.journal_client import JournalClient
    from src.manager import ClusterManager
    from src.models import (
        ErrorResponse,
        ObjectDetailResponse,
        ObjectListResponse,
        ObjectSummary,
        WorkerHeartbeatRequest,
        WorkerRegisterRequest,
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("controller.api")

db = Database(config.DATABASE_PATH)
journal_client = JournalClient(config.JOURNAL_NODES)
manager = ClusterManager(db, journal_client=journal_client)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Controller service (Role: %s)...", config.CONTROLLER_ROLE)
    await journal_client.initialize()
    await manager.start()
    yield
    # Shutdown
    logger.info("Shutting down Controller service...")
    await manager.stop()
    await journal_client.close()


app = FastAPI(
    title="Mini Distributed Cloud Object Store - Controller",
    version="1.0.0",
    lifespan=lifespan,
)


# Global Exception Handler to return consistent error JSON
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    detail = exc.detail
    if isinstance(detail, dict) and "error" in detail:
        msg = detail["error"]
    else:
        msg = str(detail)
    return JSONResponse(status_code=exc.status_code, content={"error": msg})


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.error("Unhandled server exception: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": f"Internal server error: {str(exc)}"},
    )


# -------------------------------------------------------------
# 5. Client REST API
# -------------------------------------------------------------

@app.post(
    "/api/v1/objects",
    status_code=status.HTTP_201_CREATED,
    response_model=ObjectDetailResponse,
    responses={
        400: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
async def upload_object(
    file: UploadFile = File(...),
    metadata: Optional[str] = Form(None),
):
    """
    5.1 Upload Object
    Upload an arbitrary binary object. Slices the file into 1MB chunks, computes SHA-256,
    distributes chunks to workers with replication factor = 2, and saves metadata.
    """
    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File is required and must have a filename",
        )

    # Parse custom metadata if provided
    custom_metadata: Dict[str, Any] = {}
    if metadata:
        try:
            custom_metadata = json.loads(metadata)
            if not isinstance(custom_metadata, dict):
                raise ValueError("Metadata must be a JSON object")
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid JSON metadata: {str(e)}",
            )

    # Check available healthy workers
    healthy_workers = await manager.get_healthy_workers()
    if len(healthy_workers) < config.REPLICATION_FACTOR:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Insufficient healthy storage workers available ({len(healthy_workers)} available, {config.REPLICATION_FACTOR} required)",
        )

    object_id = str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).isoformat()
    whole_hasher = hashlib.sha256()

    chunks_data = []
    chunk_index = 0
    total_size = 0

    # Read and slice file into 1MB chunks
    while True:
        chunk_bytes = await file.read(config.CHUNK_SIZE)
        if not chunk_bytes:
            break
        total_size += len(chunk_bytes)
        whole_hasher.update(chunk_bytes)

        chunk_checksum = hashlib.sha256(chunk_bytes).hexdigest()
        chunk_id = f"{object_id}_chunk_{chunk_index}"
        chunks_data.append(
            {
                "chunk_id": chunk_id,
                "chunk_index": chunk_index,
                "size": len(chunk_bytes),
                "checksum": chunk_checksum,
                "data": chunk_bytes,
            }
        )
        chunk_index += 1

    # In case of empty file upload
    if total_size == 0:
        chunk_bytes = b""
        chunk_checksum = hashlib.sha256(chunk_bytes).hexdigest()
        chunk_id = f"{object_id}_chunk_0"
        chunks_data.append(
            {
                "chunk_id": chunk_id,
                "chunk_index": 0,
                "size": 0,
                "checksum": chunk_checksum,
                "data": chunk_bytes,
            }
        )

    whole_checksum = whole_hasher.hexdigest()
    tentative_loads = db.get_worker_chunk_counts()

    # Assign workers and upload chunks
    stored_chunks_info = []
    rollback_chunks = []

    for item in chunks_data:
        try:
            chosen_workers = manager.select_workers_for_chunk(healthy_workers, tentative_loads)
        except RuntimeError as e:
            # Rollback any chunks uploaded so far
            await manager.delete_object_chunks(object_id, rollback_chunks)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=str(e),
            )

        upload_tasks = [
            manager.store_chunk_on_worker(w, item["chunk_id"], item["data"])
            for w in chosen_workers
        ]
        results = await asyncio.gather(*upload_tasks)

        # Verify all replicas were written
        if not all(results):
            await manager.delete_object_chunks(object_id, rollback_chunks)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to store replicas for chunk {item['chunk_id']}",
            )

        worker_ids = [w["worker_id"] for w in chosen_workers]
        stored_chunks_info.append(
            {
                "chunk_id": item["chunk_id"],
                "chunk_index": item["chunk_index"],
                "size": item["size"],
                "checksum": item["checksum"],
                "replicas": worker_ids,
            }
        )
        rollback_chunks.append({"chunk_id": item["chunk_id"], "replicas": worker_ids})

    # Save to database
    content_type = file.content_type or "application/octet-stream"
    created_obj = db.create_object(
        object_id=object_id,
        name=file.filename,
        size=total_size,
        content_type=content_type,
        created_at=now_iso,
        checksum=whole_checksum,
        total_chunks=len(stored_chunks_info),
        custom_metadata=custom_metadata,
    )

    for ch in stored_chunks_info:
        db.add_chunk(
            chunk_id=ch["chunk_id"],
            object_id=object_id,
            chunk_index=ch["chunk_index"],
            size=ch["size"],
            checksum=ch["checksum"],
            replicas=ch["replicas"],
        )

    # Log to distributed Journal Nodes (EditLog)
    try:
        await journal_client.log_edit("OP_CREATE_OBJECT", created_obj)
        for ch in stored_chunks_info:
            await journal_client.log_edit("OP_ADD_CHUNK", ch)
    except Exception as e:
        logger.warning("Failed to log object upload to journal: %s", e)

    logger.info("Successfully uploaded object %s (%s, %d bytes, %d chunks)",
                object_id, file.filename, total_size, len(stored_chunks_info))

    return {
        **created_obj,
        "chunks": stored_chunks_info,
    }


@app.get(
    "/api/v1/objects/{object_id}",
    responses={
        200: {"description": "Binary object data"},
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
async def download_object(object_id: str):
    """
    5.2 Download Object
    Returns the complete original object as binary data with appropriate Content-Type.
    Performs data integrity verification and automatic healing if corruption is detected.
    """
    obj = db.get_object(object_id)
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object {object_id} not found",
        )

    chunks = db.get_chunks_for_object(object_id)
    if not chunks and obj["size"] > 0:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"No chunks registered for object {object_id}",
        )

    async def chunk_generator():
        for ch in chunks:
            chunk_bytes = await manager.retrieve_chunk_with_integrity(
                chunk_id=ch["chunk_id"],
                expected_checksum=ch["checksum"],
                replica_worker_ids=ch["replicas"],
            )
            if chunk_bytes is None:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Failed to retrieve chunk {ch['chunk_id']} from available replicas",
                )
            yield chunk_bytes

    headers = {
        "Content-Disposition": f'attachment; filename="{obj["name"]}"',
        "Content-Length": str(obj["size"]),
        "X-Object-Checksum": obj["checksum"],
    }
    return StreamingResponse(
        chunk_generator(),
        media_type=obj["content_type"],
        headers=headers,
    )


@app.get(
    "/api/v1/objects/{object_id}/metadata",
    response_model=ObjectDetailResponse,
    responses={404: {"model": ErrorResponse}},
)
async def get_object_metadata(object_id: str):
    """
    5.3 Get Object Metadata
    Returns metadata of specified object without downloading data.
    """
    obj = db.get_object(object_id)
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object {object_id} not found",
        )
    chunks = db.get_chunks_for_object(object_id)
    return {
        **obj,
        "chunks": chunks,
    }


@app.patch(
    "/api/v1/objects/{object_id}/metadata",
    response_model=ObjectDetailResponse,
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
async def update_object_metadata(object_id: str, request: Request):
    """
    5.4 Update Object Metadata
    Accepts JSON body of custom metadata to add/update without re-uploading file.
    """
    try:
        body = await request.json()
        if not isinstance(body, dict):
            raise ValueError("Body must be a JSON object")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid JSON request body: {str(e)}",
        )

    now_iso = datetime.now(timezone.utc).isoformat()
    updated = db.update_object_metadata(object_id, body, now_iso)
    if not updated:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object {object_id} not found",
        )

    # Log to distributed Journal Nodes (EditLog)
    try:
        await journal_client.log_edit("OP_UPDATE_METADATA", {
            "object_id": object_id,
            "custom_metadata": body,
            "modified_at": now_iso,
        })
    except Exception as e:
        logger.warning("Failed to log metadata update to journal: %s", e)

    chunks = db.get_chunks_for_object(object_id)
    return {
        **updated,
        "chunks": chunks,
    }


@app.delete(
    "/api/v1/objects/{object_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"model": ErrorResponse}},
)
async def delete_object(object_id: str):
    """
    5.5 Delete Object
    Deletes the specified object and its associated stored chunk data across workers.
    """
    obj = db.get_object(object_id)
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object {object_id} not found",
        )

    chunks = db.get_chunks_for_object(object_id)
    # Delete chunks from worker disks
    await manager.delete_object_chunks(object_id, chunks)
    # Delete object from metadata store
    db.delete_object(object_id)

    # Log to distributed Journal Nodes (EditLog)
    try:
        await journal_client.log_edit("OP_DELETE_OBJECT", {"object_id": object_id})
    except Exception as e:
        logger.warning("Failed to log object deletion to journal: %s", e)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get(
    "/api/v1/objects",
    response_model=ObjectListResponse,
)
async def list_objects(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
):
    """
    5.6 List Objects
    Returns list of objects currently stored with pagination.
    """
    offset = (page - 1) * limit
    objects, total = db.list_objects(limit=limit, offset=offset)
    return {
        "objects": objects,
        "total": total,
        "page": page,
        "limit": limit,
    }


# -------------------------------------------------------------
# 6. Internal Worker APIs
# -------------------------------------------------------------

@app.get("/health")
async def health_check():
    """
    Health check endpoint for active/standby monitoring.
    """
    return {"status": "ok", "role": config.CONTROLLER_ROLE}


@app.get("/internal/ha/status")
async def get_ha_status():
    """
    High Availability diagnostic status endpoint.
    """
    return {
        "role": config.CONTROLLER_ROLE,
        "current_txid": journal_client.current_txid,
        "journal_nodes": config.JOURNAL_NODES,
    }


@app.post("/internal/workers/register")
async def register_worker(req: WorkerRegisterRequest):
    """
    Register a worker with the Controller.
    """
    worker = db.register_worker(req.worker_id, req.host, req.port)
    logger.info("Worker registered: %s (%s:%d)", req.worker_id, req.host, req.port)
    try:
        await journal_client.log_edit("OP_REGISTER_WORKER", {
            "worker_id": req.worker_id,
            "host": req.host,
            "port": req.port,
        })
    except Exception as e:
        logger.warning("Failed to log worker registration to journal: %s", e)
    return {"status": "registered", "worker": worker}


@app.post("/internal/workers/heartbeat")
async def worker_heartbeat(req: WorkerHeartbeatRequest):
    """
    Report worker availability.
    """
    updated = db.update_heartbeat(req.worker_id)
    if not updated:
        # Worker wasn't registered yet
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"error": f"Worker {req.worker_id} not registered. Please register first."},
        )
    return {"status": "ok", "worker_id": req.worker_id}


@app.get("/internal/workers/{worker_id}/chunks")
async def get_worker_chunks(worker_id: str):
    """
    Retrieve information about chunks stored by a worker from the Controller's registry.
    """
    worker = db.get_worker(worker_id)
    if not worker:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Worker {worker_id} not found",
        )
    all_chunks = db.get_all_chunks()
    worker_chunks = [ch for ch in all_chunks if worker_id in ch["replicas"]]
    return {
        "worker_id": worker_id,
        "status": worker["status"],
        "chunk_count": len(worker_chunks),
        "chunks": worker_chunks,
    }


@app.get("/internal/cluster/status")
async def get_cluster_status():
    """
    Diagnostic endpoint showing health of all workers and replication status.
    """
    workers = db.get_all_workers()
    healthy_workers = await manager.get_healthy_workers()
    all_chunks = db.get_all_chunks()

    under_replicated = []
    for ch in all_chunks:
        active = [r for r in ch["replicas"] if any(w["worker_id"] == r and w["status"] == "HEALTHY" for w in workers)]
        if len(active) < config.REPLICATION_FACTOR:
            under_replicated.append({
                "chunk_id": ch["chunk_id"],
                "active_replicas": active,
                "required": config.REPLICATION_FACTOR
            })

    return {
        "total_workers": len(workers),
        "healthy_workers": len(healthy_workers),
        "workers": workers,
        "total_chunks": len(all_chunks),
        "under_replicated_chunks": under_replicated,
    }
