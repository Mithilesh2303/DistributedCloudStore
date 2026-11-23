import asyncio
import hashlib
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx
try:
    from controller.src import config
    from controller.src.database import Database
except (ImportError, ModuleNotFoundError):
    from src import config
    from src.database import Database

logger = logging.getLogger("controller.manager")


class ClusterManager:
    def __init__(self, db: Database, journal_client=None):
        self.db = db
        self.journal_client = journal_client
        self.client = httpx.AsyncClient(timeout=10.0)
        self._running = False
        self._watchdog_task: Optional[asyncio.Task] = None

    async def start(self):
        self._running = True
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())
        logger.info("ClusterManager background watchdog started.")

    async def stop(self):
        self._running = False
        if self._watchdog_task:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
        await self.client.aclose()
        logger.info("ClusterManager stopped.")

    def _get_worker_url(self, worker: Dict[str, Any], path: str) -> str:
        return f"http://{worker['host']}:{worker['port']}{path}"

    async def get_healthy_workers(self) -> List[Dict[str, Any]]:
        all_workers = self.db.get_all_workers()
        now = time.time()
        healthy = []
        for w in all_workers:
            if (now - w["last_heartbeat"]) <= config.HEARTBEAT_TIMEOUT and w["status"] == "HEALTHY":
                healthy.append(w)
            elif w["status"] == "HEALTHY":
                # Worker timed out
                self.db.mark_worker_status(w["worker_id"], "OFFLINE")
                logger.warning("Worker %s timed out (last heartbeat %fs ago). Marked OFFLINE.",
                               w["worker_id"], now - w["last_heartbeat"])
        return healthy

    def select_workers_for_chunk(
        self, healthy_workers: List[Dict[str, Any]], tentative_loads: Dict[str, int]
    ) -> List[Dict[str, Any]]:
        if len(healthy_workers) < config.REPLICATION_FACTOR:
            raise RuntimeError(
                f"Not enough healthy workers. Required: {config.REPLICATION_FACTOR}, Available: {len(healthy_workers)}"
            )

        # Sort workers by current load (tentative chunk assignments)
        sorted_workers = sorted(
            healthy_workers,
            key=lambda w: tentative_loads.get(w["worker_id"], 0),
        )
        selected = sorted_workers[: config.REPLICATION_FACTOR]
        for w in selected:
            tentative_loads[w["worker_id"]] = tentative_loads.get(w["worker_id"], 0) + 1
        return selected

    async def store_chunk_on_worker(self, worker: Dict[str, Any], chunk_id: str, data: bytes) -> bool:
        url = self._get_worker_url(worker, f"/internal/chunks/{chunk_id}")
        try:
            resp = await self.client.put(
                url,
                content=data,
                headers={"Content-Type": "application/octet-stream"},
            )
            if resp.status_code in (200, 201):
                return True
            logger.error("Failed to store chunk %s on %s: HTTP %d %s",
                         chunk_id, worker["worker_id"], resp.status_code, resp.text)
            return False
        except Exception as e:
            logger.error("Error sending chunk %s to worker %s (%s): %s",
                         chunk_id, worker["worker_id"], url, e)
            return False

    async def fetch_chunk_from_worker(
        self, worker: Dict[str, Any], chunk_id: str
    ) -> Optional[bytes]:
        url = self._get_worker_url(worker, f"/internal/chunks/{chunk_id}")
        try:
            resp = await self.client.get(url)
            if resp.status_code == 200:
                return resp.content
            logger.warning("Worker %s returned status %d for chunk %s",
                           worker["worker_id"], resp.status_code, chunk_id)
            return None
        except Exception as e:
            logger.warning("Error fetching chunk %s from %s: %s",
                           chunk_id, worker["worker_id"], e)
            return None

    async def delete_chunk_from_worker(self, worker: Dict[str, Any], chunk_id: str) -> bool:
        url = self._get_worker_url(worker, f"/internal/chunks/{chunk_id}")
        try:
            resp = await self.client.delete(url)
            return resp.status_code in (200, 204, 404)
        except Exception as e:
            logger.warning("Error deleting chunk %s from %s: %s",
                           chunk_id, worker["worker_id"], e)
            return False

    async def delete_object_chunks(self, object_id: str, chunks: List[Dict[str, Any]]):
        workers_map = {w["worker_id"]: w for w in self.db.get_all_workers()}
        tasks = []
        for ch in chunks:
            for w_id in ch["replicas"]:
                worker = workers_map.get(w_id)
                if worker:
                    tasks.append(self.delete_chunk_from_worker(worker, ch["chunk_id"]))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def retrieve_chunk_with_integrity(
        self, chunk_id: str, expected_checksum: str, replica_worker_ids: List[str]
    ) -> Optional[bytes]:
        """
        Retrieves a chunk from one of its healthy replicas.
        If a replica returns corrupt data (checksum mismatch), it tries another replica,
        and automatically heals the corrupted replica with the verified data!
        """
        workers_map = {w["worker_id"]: w for w in self.db.get_all_workers()}
        corrupted_workers = []
        valid_data = None

        for w_id in replica_worker_ids:
            worker = workers_map.get(w_id)
            if not worker or worker.get("status") != "HEALTHY":
                continue

            data = await self.fetch_chunk_from_worker(worker, chunk_id)
            if data is None:
                continue

            calculated_hash = hashlib.sha256(data).hexdigest()
            if calculated_hash == expected_checksum:
                valid_data = data
                break
            else:
                logger.warning(
                    "DATA CORRUPTION DETECTED on worker %s for chunk %s! "
                    "Expected SHA256: %s, Got: %s",
                    w_id, chunk_id, expected_checksum, calculated_hash
                )
                corrupted_workers.append(worker)

        if valid_data is not None:
            # Self-healing: if any worker had corrupted data, overwrite it with the valid data
            for corrupt_worker in corrupted_workers:
                logger.info("Auto-repairing corrupted replica on worker %s for chunk %s...",
                            corrupt_worker["worker_id"], chunk_id)
                asyncio.create_task(
                    self.store_chunk_on_worker(corrupt_worker, chunk_id, valid_data)
                )
            return valid_data

        logger.error("Failed to retrieve valid chunk %s from any replica: %s",
                     chunk_id, replica_worker_ids)
        return None

    # --- Worker Liveness & Self-Healing Watchdog ---
    async def _watchdog_loop(self):
        while self._running:
            try:
                await self._check_health_and_rebalance()
            except Exception as e:
                logger.error("Error in watchdog loop: %s", e, exc_info=True)
            await asyncio.sleep(config.HEAL_CHECK_INTERVAL)

    async def _check_health_and_rebalance(self):
        healthy_workers = await self.get_healthy_workers()
        healthy_map = {w["worker_id"]: w for w in healthy_workers}
        workers_map = {w["worker_id"]: w for w in self.db.get_all_workers()}

        all_chunks = self.db.get_all_chunks()
        worker_counts = self.db.get_worker_chunk_counts()

        for ch in all_chunks:
            chunk_id = ch["chunk_id"]
            expected_checksum = ch["checksum"]
            replicas = ch["replicas"]

            active_replicas = [r for r in replicas if r in healthy_map]

            # Under-replication: fewer active replicas than REPLICATION_FACTOR
            if len(active_replicas) < config.REPLICATION_FACTOR and len(active_replicas) > 0:
                logger.warning(
                    "Chunk %s is UNDER-REPLICATED (Active: %d / Required: %d). Triggering re-replication...",
                    chunk_id, len(active_replicas), config.REPLICATION_FACTOR
                )

                # Available healthy workers that don't hold this chunk
                candidates = [
                    w for w in healthy_workers
                    if w["worker_id"] not in replicas
                ]

                if not candidates:
                    logger.warning("No candidate workers available to replicate chunk %s", chunk_id)
                    continue

                # Pick candidate with lowest chunk count
                candidates.sort(key=lambda w: worker_counts.get(w["worker_id"], 0))
                target_worker = candidates[0]

                # Fetch chunk from one of the active surviving replicas
                survivor_worker = healthy_map[active_replicas[0]]
                chunk_data = await self.fetch_chunk_from_worker(survivor_worker, chunk_id)

                if chunk_data is None:
                    logger.error("Failed to fetch chunk %s from survivor %s",
                                 chunk_id, survivor_worker["worker_id"])
                    continue

                # Verify integrity of survivor's copy
                if hashlib.sha256(chunk_data).hexdigest() != expected_checksum:
                    logger.error("Survivor copy of chunk %s on %s is corrupt!",
                                 chunk_id, survivor_worker["worker_id"])
                    continue

                # Push to target worker
                stored = await self.store_chunk_on_worker(target_worker, chunk_id, chunk_data)
                if stored:
                    self.db.add_replica(chunk_id, target_worker["worker_id"])
                    if self.journal_client:
                        try:
                            await self.journal_client.log_edit(
                                "OP_ADD_REPLICA",
                                {"chunk_id": chunk_id, "worker_id": target_worker["worker_id"]},
                            )
                        except Exception as e:
                            logger.warning("Failed to log OP_ADD_REPLICA to journal: %s", e)

                    # Remove the offline replica so we maintain the exact replication factor
                    offline_replicas = [r for r in replicas if r not in healthy_map]
                    for off_r in offline_replicas:
                        self.db.remove_replica(chunk_id, off_r)
                        if self.journal_client:
                            try:
                                await self.journal_client.log_edit(
                                    "OP_REMOVE_REPLICA",
                                    {"chunk_id": chunk_id, "worker_id": off_r},
                                )
                            except Exception as e:
                                logger.warning("Failed to log OP_REMOVE_REPLICA to journal: %s", e)

                    worker_counts[target_worker["worker_id"]] = worker_counts.get(target_worker["worker_id"], 0) + 1
                    logger.info(
                        "Self-healing complete: Chunk %s successfully re-replicated to %s. "
                        "Replication factor %d restored.",
                        chunk_id, target_worker["worker_id"], config.REPLICATION_FACTOR
                    )
