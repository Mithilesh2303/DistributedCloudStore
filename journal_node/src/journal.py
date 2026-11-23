from contextlib import contextmanager
import json
import logging
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("journal_node.engine")


class JournalEngine:
    def __init__(self, journal_dir: str):
        self.journal_dir = journal_dir
        os.makedirs(self.journal_dir, exist_ok=True)
        self.db_path = os.path.join(self.journal_dir, "journal.db")
        self._lock = threading.Lock()
        self.init_db()

    @contextmanager
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
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
                    CREATE TABLE IF NOT EXISTS edits (
                        txid INTEGER PRIMARY KEY,
                        op TEXT NOT NULL,
                        timestamp REAL NOT NULL,
                        data TEXT NOT NULL
                    );
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS fsimages (
                        checkpoint_txid INTEGER PRIMARY KEY,
                        timestamp REAL NOT NULL,
                        snapshot TEXT NOT NULL
                    );
                    """
                )
                conn.commit()
                logger.info("JournalEngine initialized at %s", self.db_path)

    def append_edits(self, edits: List[Dict[str, Any]]) -> Tuple[int, int]:
        if not edits:
            return self.get_latest_txid(), 0

        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                added = 0
                for edit in edits:
                    txid = edit["txid"]
                    op = edit["op"]
                    ts = edit.get("timestamp", time.time())
                    data_str = json.dumps(edit.get("data", {}))
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO edits (txid, op, timestamp, data)
                        VALUES (?, ?, ?, ?);
                        """,
                        (txid, op, ts, data_str),
                    )
                    if cursor.rowcount > 0:
                        added += 1
                conn.commit()

                cursor.execute("SELECT MAX(txid) as max_txid FROM edits;")
                row = cursor.fetchone()
                latest_txid = row["max_txid"] if (row and row["max_txid"] is not None) else 0
                return latest_txid, added

    def get_edits(self, since_txid: int = 0, limit: int = 1000) -> List[Dict[str, Any]]:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT txid, op, timestamp, data
                    FROM edits
                    WHERE txid > ?
                    ORDER BY txid ASC
                    LIMIT ?;
                    """,
                    (since_txid, limit),
                )
                rows = cursor.fetchall()
                results = []
                for r in rows:
                    results.append({
                        "txid": r["txid"],
                        "op": r["op"],
                        "timestamp": r["timestamp"],
                        "data": json.loads(r["data"]) if r["data"] else {},
                    })
                return results

    def get_latest_txid(self) -> int:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT MAX(txid) as max_txid FROM edits;")
                row = cursor.fetchone()
                return row["max_txid"] if (row and row["max_txid"] is not None) else 0

    def get_total_edits_count(self) -> int:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) as cnt FROM edits;")
                row = cursor.fetchone()
                return row["cnt"] if row else 0

    def save_fsimage(self, checkpoint_txid: int, timestamp: float, snapshot: Dict[str, Any]):
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO fsimages (checkpoint_txid, timestamp, snapshot)
                    VALUES (?, ?, ?);
                    """,
                    (checkpoint_txid, timestamp, json.dumps(snapshot)),
                )
                conn.commit()
                # Also save a human-readable / exportable file
                snapshot_file = os.path.join(self.journal_dir, f"fsimage_txid_{checkpoint_txid}.json")
                try:
                    with open(snapshot_file, "w") as f:
                        json.dump({
                            "checkpoint_txid": checkpoint_txid,
                            "timestamp": timestamp,
                            "snapshot": snapshot,
                        }, f, indent=2)
                except Exception as e:
                    logger.warning("Could not write snapshot file %s: %s", snapshot_file, e)

    def get_latest_fsimage(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT checkpoint_txid, timestamp, snapshot
                    FROM fsimages
                    ORDER BY checkpoint_txid DESC
                    LIMIT 1;
                    """
                )
                row = cursor.fetchone()
                if not row:
                    return None
                return {
                    "checkpoint_txid": row["checkpoint_txid"],
                    "timestamp": row["timestamp"],
                    "snapshot": json.loads(row["snapshot"]) if row["snapshot"] else {},
                }

    def compact_edits(self, up_to_txid: int) -> int:
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM edits WHERE txid <= ?;", (up_to_txid,))
                deleted = cursor.rowcount
                conn.commit()
                return deleted

