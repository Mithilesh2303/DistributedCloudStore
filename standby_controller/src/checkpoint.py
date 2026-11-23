import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple
import httpx

logger = logging.getLogger("standby.checkpoint")


class FSImageManager:
    """
    Manages point-in-time metadata snapshots (FSImage).
    Mirroring HDFS Secondary/Standby NameNode checkpointing:
    Dumps local SQLite metadata to persistent JSON image and propagates to JournalNodes.
    """

    def __init__(self, fsimage_dir: str, journal_nodes: List[str]):
        self.fsimage_dir = fsimage_dir
        self.journal_nodes = [u.rstrip("/") for u in journal_nodes]
        os.makedirs(self.fsimage_dir, exist_ok=True)

    def save_fsimage(self, checkpoint_txid: int, snapshot: Dict[str, Any]) -> str:
        timestamp = time.time()
        file_path = os.path.join(self.fsimage_dir, f"fsimage_txid_{checkpoint_txid}.json")
        data = {
            "checkpoint_txid": checkpoint_txid,
            "timestamp": timestamp,
            "snapshot": snapshot,
        }
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

        # Also write a pointer to the current latest fsimage
        latest_pointer = os.path.join(self.fsimage_dir, "fsimage_latest.json")
        try:
            with open(latest_pointer, "w") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

        logger.info("Saved local FSImage snapshot at %s (txid: %d)", file_path, checkpoint_txid)
        return file_path

    async def broadcast_fsimage_to_journal(self, client: httpx.AsyncClient, checkpoint_txid: int, snapshot: Dict[str, Any]):
        if not self.journal_nodes:
            return
        payload = {
            "checkpoint_txid": checkpoint_txid,
            "timestamp": time.time(),
            "snapshot": snapshot,
        }
        for node in self.journal_nodes:
            try:
                resp = await client.post(f"{node}/v1/journal/fsimage", json=payload, timeout=5.0)
                if resp.status_code == 200:
                    logger.info("Uploaded FSImage txid=%d to JournalNode %s", checkpoint_txid, node)
            except Exception as e:
                logger.warning("Failed to upload FSImage to %s: %s", node, e)

    def load_latest_local_fsimage(self) -> Optional[Tuple[int, Dict[str, Any]]]:
        if not os.path.exists(self.fsimage_dir):
            return None

        best_txid = -1
        best_file = None
        for fname in os.listdir(self.fsimage_dir):
            if fname.startswith("fsimage_txid_") and fname.endswith(".json"):
                try:
                    part = fname.replace("fsimage_txid_", "").replace(".json", "")
                    tx = int(part)
                    if tx > best_txid:
                        best_txid = tx
                        best_file = os.path.join(self.fsimage_dir, fname)
                except ValueError:
                    continue

        if best_file and os.path.exists(best_file):
            try:
                with open(best_file, "r") as f:
                    content = json.load(f)
                    return content["checkpoint_txid"], content["snapshot"]
            except Exception as e:
                logger.error("Failed to load local FSImage from %s: %s", best_file, e)

        return None

    async def fetch_latest_journal_fsimage(self, client: httpx.AsyncClient) -> Optional[Tuple[int, Dict[str, Any]]]:
        if not self.journal_nodes:
            return None

        for node in self.journal_nodes:
            try:
                resp = await client.get(f"{node}/v1/journal/fsimage", timeout=5.0)
                if resp.status_code == 200:
                    data = resp.json()
                    return data["checkpoint_txid"], data["snapshot"]
            except Exception:
                continue
        return None

