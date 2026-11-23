import asyncio
import logging
import time
from typing import Any, Dict, List, Optional
import httpx

try:
    from standby_controller.src import config
    from standby_controller.src.checkpoint import FSImageManager
    from standby_controller.src.database import StandbyDatabase
except (ImportError, ModuleNotFoundError):
    from src import config
    from src.checkpoint import FSImageManager
    from src.database import StandbyDatabase

logger = logging.getLogger("standby.syncer")


class EditLogSyncer:
    """
    Continuously tails edit logs from Journal Nodes and applies them to local StandbyDatabase.
    Periodically checkpoints metadata state to FSImage.
    """

    def __init__(self, db: StandbyDatabase, fsimage_mgr: FSImageManager):
        self.db = db
        self.fsimage_mgr = fsimage_mgr
        self.journal_nodes = config.JOURNAL_NODES
        self.client = httpx.AsyncClient(timeout=4.0)
        self.last_applied_txid = 0
        self.last_checkpoint_txid = 0
        self._running = False
        self._sync_task: Optional[asyncio.Task] = None
        self._checkpoint_task: Optional[asyncio.Task] = None

    async def initialize(self):
        """
        Bootstrap metadata:
        1. Check local or journal FSImage first.
        2. If available, restore snapshot and set last_applied_txid = checkpoint_txid.
        3. Replay newer edit logs (txid > checkpoint_txid).
        """
        # Try local fsimage
        local_img = self.fsimage_mgr.load_latest_local_fsimage()
        if local_img:
            txid, snapshot = local_img
            logger.info("Bootstrapping Standby state from local FSImage (txid: %d)", txid)
            self.db.restore_from_snapshot(snapshot)
            self.last_applied_txid = txid
            self.last_checkpoint_txid = txid
        else:
            # Try journal nodes for fsimage
            journal_img = await self.fsimage_mgr.fetch_latest_journal_fsimage(self.client)
            if journal_img:
                txid, snapshot = journal_img
                logger.info("Bootstrapping Standby state from Journal FSImage (txid: %d)", txid)
                self.db.restore_from_snapshot(snapshot)
                self.fsimage_mgr.save_fsimage(txid, snapshot)
                self.last_applied_txid = txid
                self.last_checkpoint_txid = txid

        # Replay any pending edits immediately
        await self.sync_once()

    async def start(self):
        self._running = True
        self._sync_task = asyncio.create_task(self._sync_loop())
        self._checkpoint_task = asyncio.create_task(self._checkpoint_loop())
        logger.info("EditLogSyncer and FSImage Checkpointer started.")

    async def stop(self):
        self._running = False
        if self._sync_task:
            self._sync_task.cancel()
        if self._checkpoint_task:
            self._checkpoint_task.cancel()
        await self.client.aclose()
        logger.info("EditLogSyncer stopped.")

    async def sync_once(self) -> int:
        """
        Queries Journal Nodes for edits with txid > last_applied_txid and applies them.
        Returns count of applied edits.
        """
        if not self.journal_nodes:
            return 0

        edits: List[Dict[str, Any]] = []
        for node in self.journal_nodes:
            try:
                url = f"{node}/v1/journal/edits?since_txid={self.last_applied_txid}&limit=1000"
                resp = await self.client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    node_edits = data.get("edits", [])
                    if len(node_edits) > len(edits):
                        edits = node_edits
            except Exception as e:
                logger.debug("Could not poll edits from %s: %s", node, e)

        if not edits:
            return 0

        # Sort strictly by txid
        edits.sort(key=lambda e: e["txid"])
        applied = 0
        for e in edits:
            tx = e["txid"]
            if tx > self.last_applied_txid:
                try:
                    self.db.apply_edit(e["op"], e.get("data", {}))
                    self.last_applied_txid = tx
                    applied += 1
                except Exception as err:
                    logger.error("Failed to apply edit txid=%d op=%s: %s", tx, e["op"], err)

        if applied > 0:
            logger.info("Standby applied %d new edit(s). Current txid: %d", applied, self.last_applied_txid)
        return applied

    async def _sync_loop(self):
        while self._running:
            try:
                await self.sync_once()
            except Exception as e:
                logger.error("Error in sync loop: %s", e)
            await asyncio.sleep(config.SYNC_INTERVAL)

    async def trigger_checkpoint(self) -> Dict[str, Any]:
        """
        Creates an FSImage checkpoint of current metadata state and saves it.
        """
        snapshot = self.db.create_snapshot()
        txid = self.last_applied_txid
        path = self.fsimage_mgr.save_fsimage(txid, snapshot)
        await self.fsimage_mgr.broadcast_fsimage_to_journal(self.client, txid, snapshot)
        self.last_checkpoint_txid = txid
        logger.info("FSImage Checkpoint created at txid %d (%s)", txid, path)
        return {
            "checkpoint_txid": txid,
            "path": path,
            "total_objects": len(snapshot.get("objects", [])),
            "total_chunks": len(snapshot.get("chunks", [])),
        }

    async def _checkpoint_loop(self):
        while self._running:
            await asyncio.sleep(config.CHECKPOINT_INTERVAL)
            try:
                if self.last_applied_txid > self.last_checkpoint_txid:
                    await self.trigger_checkpoint()
            except Exception as e:
                logger.error("Error in checkpoint loop: %s", e)

