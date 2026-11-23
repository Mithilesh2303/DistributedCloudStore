from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class EditLogEntry(BaseModel):
    txid: int
    op: str
    timestamp: float
    data: Dict[str, Any] = Field(default_factory=dict)


class AppendEditsRequest(BaseModel):
    edits: List[EditLogEntry]


class AppendEditsResponse(BaseModel):
    status: str
    latest_txid: int
    count: int


class EditsListResponse(BaseModel):
    edits: List[EditLogEntry]
    since_txid: int
    latest_txid: int
    count: int


class FSImageCheckpointRequest(BaseModel):
    checkpoint_txid: int
    timestamp: float
    snapshot: Dict[str, Any]


class FSImageResponse(BaseModel):
    checkpoint_txid: int
    timestamp: float
    snapshot: Dict[str, Any]


class JournalStatusResponse(BaseModel):
    journal_node_id: str
    status: str
    latest_txid: int
    total_edits: int
    latest_checkpoint_txid: int

