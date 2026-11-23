import asyncio
import logging
import time
from typing import Any, Dict, List, Optional
import httpx

logger = logging.getLogger("controller.journal_client")


class JournalClient:
    """
    Client for logging metadata mutations to distributed Journal Nodes (Quorum Journaling).
    Each mutation is assigned a monotonically increasing transaction ID (txid).
    Mutations require quorum acknowledgement before proceeding.
    """

    def __init__(self, journal_node_urls: List[str]):
        self.journal_nodes = [u.rstrip("/") for u in journal_node_urls]
        self.client = httpx.AsyncClient(timeout=4.0)
        self._current_txid = 0
        self._lock = asyncio.Lock()
        self._initialized = False

    @property
    def current_txid(self) -> int:
        return self._current_txid

    async def initialize(self):
        """
        Contact JournalNodes to find the latest committed txid.
        """
        if not self.journal_nodes:
            logger.info("No journal nodes configured. Running with local txid tracking.")
            self._initialized = True
            return

        highest_txid = 0
        for node in self.journal_nodes:
            try:
                resp = await self.client.get(f"{node}/v1/journal/latest_txid")
                if resp.status_code == 200:
                    data = resp.json()
                    highest_txid = max(highest_txid, data.get("latest_txid", 0))
            except Exception as e:
                logger.warning("Could not reach journal node %s during init: %s", node, e)

        self._current_txid = highest_txid
        self._initialized = True
        logger.info("JournalClient initialized with starting txid %d across %d nodes",
                    self._current_txid, len(self.journal_nodes))

    async def close(self):
        await self.client.aclose()

    async def log_edit(self, op: str, data: Dict[str, Any]) -> int:
        """
        Assign an incrementing txid, broadcast edit to all JournalNodes,
        and wait for majority quorum ACK.
        """
        async with self._lock:
            self._current_txid += 1
            txid = self._current_txid

        entry = {
            "txid": txid,
            "op": op,
            "timestamp": time.time(),
            "data": data,
        }

        if not self.journal_nodes:
            return txid

        payload = {"edits": [entry]}
        quorum_needed = (len(self.journal_nodes) // 2) + 1

        async def send_to_node(node_url: str) -> bool:
            try:
                resp = await self.client.post(f"{node_url}/v1/journal/edits", json=payload)
                return resp.status_code == 200
            except Exception as err:
                logger.warning("Failed to write edit txid=%d to %s: %s", txid, node_url, err)
                return False

        tasks = [send_to_node(node) for node in self.journal_nodes]
        results = await asyncio.gather(*tasks)
        success_count = sum(1 for r in results if r)

        if success_count < quorum_needed:
            logger.error(
                "Quorum NOT reached for edit txid=%d op=%s! Got %d ACKs, needed %d",
                txid, op, success_count, quorum_needed
            )
            # In distributed systems, if quorum fails, the operation cannot be guaranteed
            raise RuntimeError(
                f"Journal quorum write failure: received {success_count}/{quorum_needed} ACKs for txid {txid}"
            )

        logger.debug("Committed edit txid=%d op=%s to journal quorum (%d/%d ACKs)",
                     txid, op, success_count, len(self.journal_nodes))
        return txid

