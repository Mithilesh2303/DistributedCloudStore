from contextlib import contextmanager
import json
import logging
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("standby.database")


class StandbyDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self.init_db()

    @contextmanager
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        try:
            yield conn
        finally:
            conn.close()

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
                logger.info("StandbyDatabase initialized at %s", self.db_path)

    # --- Replay Engine: Apply Edit Log Operations ---
    def apply_edit(self, op: str, data: Dict[str, Any]):
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                if op == "OP_CREATE_OBJECT":
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO objects (
                            object_id, name, size, content_type, created_at, modified_at, checksum, total_chunks, custom_metadata
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            data["object_id"],
                            data["name"],
                            data["size"],
                            data["content_type"],
                            data["created_at"],
                            data.get("modified_at", data["created_at"]),
                            data["checksum"],
                            data["total_chunks"],
                            json.dumps(data.get("custom_metadata", {})),
                        ),
                    )
                elif op == "OP_ADD_CHUNK":
                    obj_id = data.get("object_id") or (data["chunk_id"].rsplit("_chunk_", 1)[0] if "_chunk_" in data["chunk_id"] else "")
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO chunks (chunk_id, object_id, chunk_index, size, checksum)
                        VALUES (?, ?, ?, ?, ?);
                        """,
                        (
                            data["chunk_id"],
                            obj_id,
                            data["chunk_index"],
                            data["size"],
                            data["checksum"],
                        ),
                    )
                    for worker_id in data.get("replicas", []):
                        cursor.execute(
                            """
                            INSERT OR IGNORE INTO chunk_replicas (chunk_id, worker_id)
                            VALUES (?, ?);
                            """,
                            (data["chunk_id"], worker_id),
                        )
                elif op == "OP_UPDATE_METADATA":
                    obj_id = data["object_id"]
                    cursor.execute("SELECT custom_metadata FROM objects WHERE object_id = ?;", (obj_id,))
                    row = cursor.fetchone()
                    if row:
                        meta = json.loads(row["custom_metadata"]) if row["custom_metadata"] else {}
                        meta.update(data.get("custom_metadata", {}))
                        cursor.execute(
                            """
                            UPDATE objects
                            SET custom_metadata = ?, modified_at = ?
                            WHERE object_id = ?;
                            """,
                            (json.dumps(meta), data.get("modified_at", ""), obj_id),
                        )
                elif op == "OP_DELETE_OBJECT":
                    cursor.execute("DELETE FROM objects WHERE object_id = ?;", (data["object_id"],))
                elif op == "OP_ADD_REPLICA":
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO chunk_replicas (chunk_id, worker_id)
                        VALUES (?, ?);
                        """,
                        (data["chunk_id"], data["worker_id"]),
                    )
                elif op == "OP_REMOVE_REPLICA":
                    cursor.execute(
                        """
                        DELETE FROM chunk_replicas
                        WHERE chunk_id = ? AND worker_id = ?;
                        """,
                        (data["chunk_id"], data["worker_id"]),
                    )
                elif op == "OP_REGISTER_WORKER":
                    now = time.time()
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
                        (data["worker_id"], data["host"], data["port"], now, now),
                    )
                conn.commit()

    # --- FSImage Snapshot & Restore Engine ---
    def create_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM objects;")
                objects = [dict(r) for r in cursor.fetchall()]

                cursor.execute("SELECT * FROM chunks;")
                chunks = [dict(r) for r in cursor.fetchall()]

                cursor.execute("SELECT * FROM chunk_replicas;")
                chunk_replicas = [dict(r) for r in cursor.fetchall()]

                cursor.execute("SELECT * FROM workers;")
                workers = [dict(r) for r in cursor.fetchall()]

                return {
                    "version": 1,
                    "objects": objects,
                    "chunks": chunks,
                    "chunk_replicas": chunk_replicas,
                    "workers": workers,
                }

    def restore_from_snapshot(self, snapshot: Dict[str, Any]):
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                # Clear existing tables
                cursor.execute("DELETE FROM chunk_replicas;")
                cursor.execute("DELETE FROM chunks;")
                cursor.execute("DELETE FROM objects;")
                cursor.execute("DELETE FROM workers;")

                for obj in snapshot.get("objects", []):
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO objects (
                            object_id, name, size, content_type, created_at, modified_at, checksum, total_chunks, custom_metadata
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            obj["object_id"],
                            obj["name"],
                            obj["size"],
                            obj["content_type"],
                            obj["created_at"],
                            obj["modified_at"],
                            obj["checksum"],
                            obj["total_chunks"],
                            obj["custom_metadata"] if isinstance(obj["custom_metadata"], str) else json.dumps(obj["custom_metadata"]),
                        ),
                    )

                for ch in snapshot.get("chunks", []):
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO chunks (chunk_id, object_id, chunk_index, size, checksum)
                        VALUES (?, ?, ?, ?, ?);
                        """,
                        (
                            ch["chunk_id"],
                            ch["object_id"],
                            ch["chunk_index"],
                            ch["size"],
                            ch["checksum"],
                        ),
                    )

                for cr in snapshot.get("chunk_replicas", []):
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO chunk_replicas (chunk_id, worker_id)
                        VALUES (?, ?);
                        """,
                        (cr["chunk_id"], cr["worker_id"]),
                    )

                for w in snapshot.get("workers", []):
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO workers (worker_id, host, port, status, last_heartbeat, registered_at)
                        VALUES (?, ?, ?, ?, ?, ?);
                        """,
                        (
                            w["worker_id"],
                            w["host"],
                            w["port"],
                            w.get("status", "HEALTHY"),
                            w.get("last_heartbeat", time.time()),
                            w.get("registered_at", time.time()),
                        ),
                    )
                conn.commit()
                logger.info(
                    "Restored snapshot with %d objects, %d chunks, %d replicas, %d workers",
                    len(snapshot.get("objects", [])),
                    len(snapshot.get("chunks", [])),
                    len(snapshot.get("chunk_replicas", [])),
                    len(snapshot.get("workers", [])),
                )

    # --- Queries and Active Mode Operations ---
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

