import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
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
    from standby_controller.src import config
    from standby_controller.src.checkpoint import FSImageManager
    from standby_controller.src.database import StandbyDatabase
    from standby_controller.src.failover import FailoverManager
    from standby_controller.src.manager import StandbyClusterManager
    from standby_controller.src.models import (
        ErrorResponse,
        ObjectDetailResponse,
        ObjectListResponse,
        WorkerHeartbeatRequest,
        WorkerRegisterRequest,
    )
    from standby_controller.src.syncer import EditLogSyncer
except (ImportError, ModuleNotFoundError):
    from src import config
    from src.checkpoint import FSImageManager
    from src.database import StandbyDatabase
    from src.failover import FailoverManager
    from src.manager import StandbyClusterManager
    from src.models import (
        ErrorResponse,
        ObjectDetailResponse,
        ObjectListResponse,
        WorkerHeartbeatRequest,
        WorkerRegisterRequest,
    )
    from src.syncer import EditLogSyncer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("standby.api")

state = {
    "role": "STANDBY",  # "STANDBY" or "ACTIVE"
}

db = StandbyDatabase(config.DATABASE_PATH)
fsimage_mgr = FSImageManager(config.FSIMAGE_DIR, config.JOURNAL_NODES)
syncer = EditLogSyncer(db, fsimage_mgr)
cluster_manager = StandbyClusterManager(db)


async def on_promoted_to_active():
    """Callback when promoted from STANDBY to ACTIVE."""
    state["role"] = "ACTIVE"
    logger.info("Starting cluster watchdog as ACTIVE controller...")
    await cluster_manager.start()


failover_mgr = FailoverManager(syncer, on_promoted_to_active)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Initializing Standby Controller...")
    await syncer.initialize()
    await syncer.start()
    await failover_mgr.start()
    yield
    # Shutdown
    logger.info("Shutting down Standby Controller...")
    await failover_mgr.stop()
    await syncer.stop()
    if state["role"] == "ACTIVE":
        await cluster_manager.stop()


app = FastAPI(
    title="Mini Distributed Cloud Object Store - Standby Controller",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    detail = exc.detail
    msg = detail.get("error", str(detail)) if isinstance(detail, dict) else str(detail)
    return JSONResponse(status_code=exc.status_code, content={"error": msg})


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.error("Unhandled server exception: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": f"Internal server error: {str(exc)}"},
    )


# -------------------------------------------------------------
# HA & Diagnostics Endpoints
# -------------------------------------------------------------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "role": state["role"],
        "last_applied_txid": syncer.last_applied_txid,
    }


@app.get("/internal/ha/status")
def ha_status():
    return {
        "role": state["role"],
        "last_applied_txid": syncer.last_applied_txid,
        "last_checkpoint_txid": syncer.last_checkpoint_txid,
        "active_controller_url": config.ACTIVE_CONTROLLER_URL,
        "journal_nodes": config.JOURNAL_NODES,
    }


@app.post("/internal/ha/failover")
async def trigger_failover():
    """Manually force promotion to ACTIVE (useful for tests and demos)."""
    logger.info("Manual failover triggered via API.")
    await failover_mgr.promote()
    return {
        "status": "promoted",
        "role": state["role"],
        "last_applied_txid": syncer.last_applied_txid,
    }


@app.post("/internal/ha/checkpoint")
async def trigger_checkpoint():
    """Manually trigger FSImage checkpoint creation."""
    res = await syncer.trigger_checkpoint()
    return res


@app.get("/internal/ha/fsimage")
def get_latest_fsimage():
    """View latest local FSImage checkpoint metadata."""
    img = fsimage_mgr.load_latest_local_fsimage()
    if not img:
        raise HTTPException(status_code=404, detail="No FSImage found")
    txid, snapshot = img
    return {"checkpoint_txid": txid, "snapshot": snapshot}


# -------------------------------------------------------------
# Client REST API
# -------------------------------------------------------------

@app.post(
    "/api/v1/objects",
    status_code=status.HTTP_201_CREATED,
    response_model=ObjectDetailResponse,
)
async def upload_object(
    file: UploadFile = File(...),
    metadata: Optional[str] = Form(None),
):
    if state["role"] != "ACTIVE":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Standby Controller is in read-only standby mode. Please upload to the active controller.",
        )

    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File is required and must have a filename",
        )

    custom_metadata: Dict[str, Any] = {}
    if metadata:
        try:
            custom_metadata = json.loads(metadata)
            if not isinstance(custom_metadata, dict):
                raise ValueError()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON metadata")

    healthy_workers = await cluster_manager.get_healthy_workers()
    if len(healthy_workers) < config.REPLICATION_FACTOR:
        raise HTTPException(
            status_code=503,
            detail=f"Insufficient healthy storage workers available ({len(healthy_workers)} available, {config.REPLICATION_FACTOR} required)",
        )

    object_id = str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).isoformat()
    whole_hasher = hashlib.sha256()

    chunks_data = []
    chunk_index = 0
    total_size = 0

    while True:
        chunk_bytes = await file.read(config.CHUNK_SIZE)
        if not chunk_bytes:
            break
        total_size += len(chunk_bytes)
        whole_hasher.update(chunk_bytes)
        c_hash = hashlib.sha256(chunk_bytes).hexdigest()
        c_id = f"{object_id}_chunk_{chunk_index}"
        chunks_data.append({
            "chunk_id": c_id,
            "chunk_index": chunk_index,
            "size": len(chunk_bytes),
            "checksum": c_hash,
            "data": chunk_bytes,
        })
        chunk_index += 1

    whole_checksum = whole_hasher.hexdigest()
    tentative_loads = db.get_worker_chunk_counts()
    stored_chunks_info = []

    for item in chunks_data:
        chosen_workers = cluster_manager.select_workers_for_chunk(healthy_workers, tentative_loads)
        upload_tasks = [
            cluster_manager.store_chunk_on_worker(w, item["chunk_id"], item["data"])
            for w in chosen_workers
        ]
        results = await asyncio.gather(*upload_tasks)
        if not all(results):
            raise HTTPException(status_code=500, detail=f"Failed to store replicas for chunk {item['chunk_id']}")

        worker_ids = [w["worker_id"] for w in chosen_workers]
        stored_chunks_info.append({
            "chunk_id": item["chunk_id"],
            "chunk_index": item["chunk_index"],
            "size": item["size"],
            "checksum": item["checksum"],
            "replicas": worker_ids,
        })
        for w_id in worker_ids:
            tentative_loads[w_id] = tentative_loads.get(w_id, 0) + 1

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

    return {**created_obj, "chunks": stored_chunks_info}


@app.get("/api/v1/objects/{object_id}")
async def download_object(object_id: str):
    obj = db.get_object(object_id)
    if not obj:
        raise HTTPException(status_code=404, detail=f"Object {object_id} not found")

    chunks = db.get_chunks_for_object(object_id)
    if not chunks and obj["size"] > 0:
        raise HTTPException(status_code=500, detail="No chunks registered")

    async def chunk_generator():
        for ch in chunks:
            chunk_bytes = await cluster_manager.retrieve_chunk_with_integrity(
                chunk_id=ch["chunk_id"],
                expected_checksum=ch["checksum"],
                replica_worker_ids=ch["replicas"],
            )
            if chunk_bytes is None:
                raise HTTPException(status_code=503, detail="Failed to retrieve chunk")
            yield chunk_bytes

    headers = {
        "Content-Disposition": f'attachment; filename="{obj["name"]}"',
        "Content-Length": str(obj["size"]),
        "X-Object-Checksum": obj["checksum"],
    }
    return StreamingResponse(chunk_generator(), media_type=obj["content_type"], headers=headers)


@app.get("/api/v1/objects/{object_id}/metadata", response_model=ObjectDetailResponse)
async def get_object_metadata(object_id: str):
    obj = db.get_object(object_id)
    if not obj:
        raise HTTPException(status_code=404, detail=f"Object {object_id} not found")
    chunks = db.get_chunks_for_object(object_id)
    return {**obj, "chunks": chunks}


@app.patch("/api/v1/objects/{object_id}/metadata", response_model=ObjectDetailResponse)
async def update_object_metadata(object_id: str, request: Request):
    if state["role"] != "ACTIVE":
        raise HTTPException(status_code=503, detail="Standby Controller is read-only")

    try:
        body = await request.json()
        if not isinstance(body, dict):
            raise ValueError()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON request body")

    now_iso = datetime.now(timezone.utc).isoformat()
    updated = db.update_object_metadata(object_id, body, now_iso)
    if not updated:
        raise HTTPException(status_code=404, detail=f"Object {object_id} not found")
    chunks = db.get_chunks_for_object(object_id)
    return {**updated, "chunks": chunks}


@app.delete("/api/v1/objects/{object_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_object(object_id: str):
    if state["role"] != "ACTIVE":
        raise HTTPException(status_code=503, detail="Standby Controller is read-only")

    obj = db.get_object(object_id)
    if not obj:
        raise HTTPException(status_code=404, detail=f"Object {object_id} not found")
    chunks = db.get_chunks_for_object(object_id)
    await cluster_manager.delete_object_chunks(object_id, chunks)
    db.delete_object(object_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/api/v1/objects", response_model=ObjectListResponse)
async def list_objects(page: int = Query(1, ge=1), limit: int = Query(20, ge=1, le=100)):
    offset = (page - 1) * limit
    objects, total = db.list_objects(limit=limit, offset=offset)
    return {"objects": objects, "total": total, "page": page, "limit": limit}


# -------------------------------------------------------------
# Internal Worker APIs
# -------------------------------------------------------------

@app.post("/internal/workers/register")
async def register_worker(req: WorkerRegisterRequest):
    worker = db.register_worker(req.worker_id, req.host, req.port)
    return {"status": "registered", "worker": worker}


@app.post("/internal/workers/heartbeat")
async def worker_heartbeat(req: WorkerHeartbeatRequest):
    updated = db.update_heartbeat(req.worker_id)
    if not updated:
        return JSONResponse(status_code=404, content={"error": f"Worker {req.worker_id} not registered."})
    return {"status": "ok", "worker_id": req.worker_id}


@app.get("/internal/workers/{worker_id}/chunks")
async def get_worker_chunks(worker_id: str):
    worker = db.get_worker(worker_id)
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")
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
    workers = db.get_all_workers()
    healthy_workers = await cluster_manager.get_healthy_workers()
    all_chunks = db.get_all_chunks()

    under_replicated = []
    for ch in all_chunks:
        active = [r for r in ch["replicas"] if any(w["worker_id"] == r and w["status"] == "HEALTHY" for w in workers)]
        if len(active) < config.REPLICATION_FACTOR:
            under_replicated.append({
                "chunk_id": ch["chunk_id"],
                "active_replicas": active,
                "required": config.REPLICATION_FACTOR,
            })

    return {
        "role": state["role"],
        "total_workers": len(workers),
        "healthy_workers": len(healthy_workers),
        "workers": workers,
        "total_chunks": len(all_chunks),
        "under_replicated_chunks": under_replicated,
    }

