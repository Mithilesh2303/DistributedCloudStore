import logging
import os
import tempfile
from typing import Any, Dict, List, Optional

logger = logging.getLogger("worker.storage")


class StorageEngine:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

    def _chunk_path(self, chunk_id: str) -> str:
        # Sanitize chunk_id to avoid path traversal
        safe_id = os.path.basename(chunk_id)
        return os.path.join(self.data_dir, safe_id)

    def store_chunk(self, chunk_id: str, data: bytes) -> int:
        target_path = self._chunk_path(chunk_id)
        # Atomic write using temp file in the same directory to ensure same filesystem
        fd, temp_path = tempfile.mkstemp(dir=self.data_dir, prefix=f"tmp_{chunk_id}_")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, target_path)
            logger.info("Stored chunk %s (%d bytes)", chunk_id, len(data))
            return len(data)
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            logger.error("Failed to store chunk %s: %s", chunk_id, e)
            raise

    def get_chunk(self, chunk_id: str) -> Optional[bytes]:
        path = self._chunk_path(chunk_id)
        if not os.path.isfile(path):
            return None
        with open(path, "rb") as f:
            return f.read()

    def delete_chunk(self, chunk_id: str) -> bool:
        path = self._chunk_path(chunk_id)
        if os.path.isfile(path):
            try:
                os.remove(path)
                logger.info("Deleted chunk %s", chunk_id)
                return True
            except Exception as e:
                logger.error("Error deleting chunk %s: %s", chunk_id, e)
                return False
        return False

    def list_chunks(self) -> List[Dict[str, Any]]:
        chunks = []
        if not os.path.exists(self.data_dir):
            return chunks

        for filename in os.listdir(self.data_dir):
            if filename.startswith("tmp_") or filename.startswith("."):
                continue
            path = os.path.join(self.data_dir, filename)
            if os.path.isfile(path):
                size = os.path.getsize(path)
                chunks.append({"chunk_id": filename, "size": size})
        return chunks

    def chunk_exists(self, chunk_id: str) -> bool:
        path = self._chunk_path(chunk_id)
        return os.path.isfile(path)
