import json
import logging
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("controller.database")


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        # Ensure parent directory exists
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self.init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def init_db(self):
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS objects (
                        object_id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        size INTEGER NOT NULL,
                        content_type TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        modified_at TEXT NOT NULL,
                        checksum TEXT NOT NULL,
                        total_chunks INTEGER NOT NULL,
                        custom_metadata TEXT NOT NULL DEFAULT '{}'
                    );
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS chunks (
                        chunk_id TEXT PRIMARY KEY,
                        object_id TEXT NOT NULL,
                        chunk_index INTEGER NOT NULL,
                        size INTEGER NOT NULL,
                        checksum TEXT NOT NULL,
                        FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE
                    );
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS chunk_replicas (
                        chunk_id TEXT NOT NULL,
                        worker_id TEXT NOT NULL,
                        PRIMARY KEY (chunk_id, worker_id),
                        FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id) ON DELETE CASCADE
                    );
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS workers (
                        worker_id TEXT PRIMARY KEY,
                        host TEXT NOT NULL,
                        port INTEGER NOT NULL,
                        status TEXT NOT NULL DEFAULT 'HEALTHY',
                        last_heartbeat REAL NOT NULL,
                        registered_at REAL NOT NULL
                    );
                    """
                )
                conn.commit()
                logger.info("Database initialized at %s", self.db_path)

    # --- Worker Management ---
    def register_worker(self, worker_id: str, host: str, port: int) -> Dict[str, Any]:
        now = time.time()
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO workers (worker_id, host, port, status, last_heartbeat, registered_at)
                    VALUES (?, ?, ?, 'HEALTHY', ?, ?)
                    ON CONFLICT(worker_id) DO UPDATE SET
                        host = excluded.host,
                        port = excluded.port,
                        status = 'HEALTHY',
                        last_heartbeat = excluded.last_heartbeat;
                    """,
                    (worker_id, host, port, now, now),
                )
                conn.commit()
                return {
                    "worker_id": worker_id,
                    "host": host,
                    "port": port,
                    "status": "HEALTHY",
                    "last_heartbeat": now,
                }

    def update_heartbeat(self, worker_id: str) -> bool:
        now = time.time()
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE workers
                    SET last_heartbeat = ?, status = 'HEALTHY'
                    WHERE worker_id = ?;
                    """,
                    (now, worker_id),
                )
                conn.commit()
                return cursor.rowcount > 0

    def get_worker(self, worker_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM workers WHERE worker_id = ?;", (worker_id,))
                row = cursor.fetchone()
                return dict(row) if row else None

    def get_all_workers(self) -> List[Dict[str, Any]]:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM workers;")
                return [dict(r) for r in cursor.fetchall()]

    def mark_worker_status(self, worker_id: str, status: str):
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE workers SET status = ? WHERE worker_id = ?;", (status, worker_id))
                conn.commit()

    # --- Object Management ---
    def create_object(
        self,
        object_id: str,
        name: str,
        size: int,
        content_type: str,
        created_at: str,
        checksum: str,
        total_chunks: int,
        custom_metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO objects (
                        object_id, name, size, content_type, created_at, modified_at, checksum, total_chunks, custom_metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        object_id,
                        name,
                        size,
                        content_type,
                        created_at,
                        created_at,
                        checksum,
                        total_chunks,
                        json.dumps(custom_metadata),
                    ),
                )
                conn.commit()
                return {
                    "object_id": object_id,
                    "name": name,
                    "size": size,
                    "content_type": content_type,
                    "created_at": created_at,
                    "modified_at": created_at,
                    "checksum": checksum,
                    "total_chunks": total_chunks,
                    "custom_metadata": custom_metadata,
                }

    def get_object(self, object_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM objects WHERE object_id = ?;", (object_id,))
                row = cursor.fetchone()
                if not row:
                    return None
                data = dict(row)
                data["custom_metadata"] = json.loads(data["custom_metadata"]) if data.get("custom_metadata") else {}
                return data

    def update_object_metadata(
        self, object_id: str, custom_metadata: Dict[str, Any], modified_at: str
    ) -> Optional[Dict[str, Any]]:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM objects WHERE object_id = ?;", (object_id,))
                row = cursor.fetchone()
                if not row:
                    return None
                existing_data = dict(row)
                existing_meta = json.loads(existing_data["custom_metadata"]) if existing_data.get("custom_metadata") else {}
                existing_meta.update(custom_metadata)

                cursor.execute(
                    """
                    UPDATE objects
                    SET custom_metadata = ?, modified_at = ?
                    WHERE object_id = ?;
                    """,
                    (json.dumps(existing_meta), modified_at, object_id),
                )
                conn.commit()
                existing_data["custom_metadata"] = existing_meta
                existing_data["modified_at"] = modified_at
                return existing_data

    def delete_object(self, object_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM objects WHERE object_id = ?;", (object_id,))
                row = cursor.fetchone()
                if not row:
                    return None
                obj = dict(row)
                obj["custom_metadata"] = json.loads(obj["custom_metadata"]) if obj.get("custom_metadata") else {}
                # Deleting object cascades to chunks and chunk_replicas
                cursor.execute("DELETE FROM objects WHERE object_id = ?;", (object_id,))
                conn.commit()
                return obj

    def list_objects(self, limit: int = 50, offset: int = 0) -> Tuple[List[Dict[str, Any]], int]:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) as total FROM objects;")
                total = cursor.fetchone()["total"]

                cursor.execute(
                    """
                    SELECT object_id, name, size, content_type, created_at, modified_at, total_chunks, custom_metadata
                    FROM objects
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?;
                    """,
                    (limit, offset),
                )
                rows = cursor.fetchall()
                results = []
                for r in rows:
                    item = dict(r)
                    item["custom_metadata"] = json.loads(item["custom_metadata"]) if item.get("custom_metadata") else {}
                    results.append(item)
                return results, total

    # --- Chunk Management ---
    def add_chunk(
        self,
        chunk_id: str,
        object_id: str,
        chunk_index: int,
        size: int,
        checksum: str,
        replicas: List[str],
    ):
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO chunks (chunk_id, object_id, chunk_index, size, checksum)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(chunk_id) DO UPDATE SET
                        size = excluded.size,
                        checksum = excluded.checksum;
                    """,
                    (chunk_id, object_id, chunk_index, size, checksum),
                )
                for worker_id in replicas:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO chunk_replicas (chunk_id, worker_id)
                        VALUES (?, ?);
                        """,
                        (chunk_id, worker_id),
                    )
                conn.commit()

    def get_chunks_for_object(self, object_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT c.chunk_id, c.object_id, c.chunk_index, c.size, c.checksum,
                           GROUP_CONCAT(cr.worker_id) as replicas_str
                    FROM chunks c
                    LEFT JOIN chunk_replicas cr ON c.chunk_id = cr.chunk_id
                    WHERE c.object_id = ?
                    GROUP BY c.chunk_id
                    ORDER BY c.chunk_index ASC;
                    """,
                    (object_id,),
                )
                rows = cursor.fetchall()
                results = []
                for r in rows:
                    d = dict(r)
                    d["replicas"] = d["replicas_str"].split(",") if d.get("replicas_str") else []
                    del d["replicas_str"]
                    results.append(d)
                return results

    def get_all_chunks(self) -> List[Dict[str, Any]]:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT c.chunk_id, c.object_id, c.chunk_index, c.size, c.checksum,
                           GROUP_CONCAT(cr.worker_id) as replicas_str
                    FROM chunks c
                    LEFT JOIN chunk_replicas cr ON c.chunk_id = cr.chunk_id
                    GROUP BY c.chunk_id;
                    """
                )
                rows = cursor.fetchall()
                results = []
                for r in rows:
                    d = dict(r)
                    d["replicas"] = d["replicas_str"].split(",") if d.get("replicas_str") else []
                    del d["replicas_str"]
                    results.append(d)
                return results

    def add_replica(self, chunk_id: str, worker_id: str):
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO chunk_replicas (chunk_id, worker_id)
                    VALUES (?, ?);
                    """,
                    (chunk_id, worker_id),
                )
                conn.commit()

    def remove_replica(self, chunk_id: str, worker_id: str):
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    DELETE FROM chunk_replicas
                    WHERE chunk_id = ? AND worker_id = ?;
                    """,
                    (chunk_id, worker_id),
                )
                conn.commit()

    def get_worker_chunk_counts(self) -> Dict[str, int]:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT worker_id, COUNT(chunk_id) as cnt
                    FROM chunk_replicas
                    GROUP BY worker_id;
                    """
                )
                return {row["worker_id"]: row["cnt"] for row in cursor.fetchall()}
