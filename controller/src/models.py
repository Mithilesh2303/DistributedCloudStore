from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class WorkerRegisterRequest(BaseModel):
    worker_id: str
    host: str
    port: int = 8001


class WorkerHeartbeatRequest(BaseModel):
    worker_id: str


class ChunkInfo(BaseModel):
    chunk_id: str
    chunk_index: int
    size: int
    checksum: str
    replicas: List[str] = Field(default_factory=list)


class ObjectSummary(BaseModel):
    object_id: str
    name: str
    size: int
    content_type: str
    created_at: str
    modified_at: str
    total_chunks: int
    custom_metadata: Dict[str, Any] = Field(default_factory=dict)


class ObjectDetailResponse(ObjectSummary):
    checksum: str
    chunks: List[ChunkInfo] = Field(default_factory=list)


class ObjectListResponse(BaseModel):
    objects: List[ObjectSummary]
    total: int
    page: int
    limit: int


class ErrorResponse(BaseModel):
    error: str
