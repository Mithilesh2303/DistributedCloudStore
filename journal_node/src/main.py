import logging
import time
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import JSONResponse

try:
    from journal_node.src import config
    from journal_node.src.journal import JournalEngine
    from journal_node.src.models import (
        AppendEditsRequest,
        AppendEditsResponse,
        EditsListResponse,
        FSImageCheckpointRequest,
        FSImageResponse,
        JournalStatusResponse,
    )
except (ImportError, ModuleNotFoundError):
    from src import config
    from src.journal import JournalEngine
    from src.models import (
        AppendEditsRequest,
        AppendEditsResponse,
        EditsListResponse,
        FSImageCheckpointRequest,
        FSImageResponse,
        JournalStatusResponse,
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("journal_node.api")

engine = JournalEngine(config.JOURNAL_DIR)
app = FastAPI(
    title=f"JournalNode {config.JOURNAL_NODE_ID}",
    description="Distributed EditLog and FSImage Storage Service",
    version="1.0.0",
)


@app.get("/health")
def health_check():
    return {"status": "ok", "journal_node_id": config.JOURNAL_NODE_ID}


@app.get("/v1/journal/status", response_model=JournalStatusResponse)
def get_journal_status():
    latest_txid = engine.get_latest_txid()
    total_edits = engine.get_total_edits_count()
    latest_img = engine.get_latest_fsimage()
    latest_checkpoint = latest_img["checkpoint_txid"] if latest_img else 0
    return JournalStatusResponse(
        journal_node_id=config.JOURNAL_NODE_ID,
        status="ONLINE",
        latest_txid=latest_txid,
        total_edits=total_edits,
        latest_checkpoint_txid=latest_checkpoint,
    )


@app.post("/v1/journal/edits", response_model=AppendEditsResponse)
def append_edits(req: AppendEditsRequest):
    edits_data = [e.model_dump() for e in req.edits]
    latest_txid, added = engine.append_edits(edits_data)
    logger.info("Appended %d edit(s), latest txid: %d", added, latest_txid)
    return AppendEditsResponse(status="success", latest_txid=latest_txid, count=added)


@app.get("/v1/journal/edits", response_model=EditsListResponse)
def get_edits(
    since_txid: int = Query(0, ge=0, description="Fetch edits with txid > since_txid"),
    limit: int = Query(1000, ge=1, le=5000),
):
    edits = engine.get_edits(since_txid=since_txid, limit=limit)
    latest_txid = engine.get_latest_txid()
    return EditsListResponse(
        edits=edits,
        since_txid=since_txid,
        latest_txid=latest_txid,
        count=len(edits),
    )


@app.get("/v1/journal/latest_txid")
def get_latest_txid():
    return {"latest_txid": engine.get_latest_txid()}


@app.post("/v1/journal/fsimage")
def save_fsimage(req: FSImageCheckpointRequest):
    engine.save_fsimage(
        checkpoint_txid=req.checkpoint_txid,
        timestamp=req.timestamp,
        snapshot=req.snapshot,
    )
    logger.info("Saved FSImage checkpoint up to txid %d", req.checkpoint_txid)
    return {"status": "saved", "checkpoint_txid": req.checkpoint_txid}


@app.get("/v1/journal/fsimage", response_model=Optional[FSImageResponse])
def get_latest_fsimage():
    img = engine.get_latest_fsimage()
    if not img:
        raise HTTPException(status_code=404, detail="No FSImage checkpoint found")
    return FSImageResponse(
        checkpoint_txid=img["checkpoint_txid"],
        timestamp=img["timestamp"],
        snapshot=img["snapshot"],
    )


@app.post("/v1/journal/compact")
def compact_edits(up_to_txid: int = Query(..., ge=1)):
    deleted = engine.compact_edits(up_to_txid)
    logger.info("Compacted %d edits up to txid %d", deleted, up_to_txid)
    return {"status": "compacted", "deleted_count": deleted}

