import asyncio
import hashlib
import logging
import time
from typing import Any, Dict, List, Optional

import httpx

try:
    from standby_controller.src import config
    from standby_controller.src.database import StandbyDatabase
except (ImportError, ModuleNotFoundError):
    from src import config
    from src.database import StandbyDatabase

logger = logging.getLogger("standby.manager")


class StandbyClusterManager:
    """
    ClusterManager for StandbyController when promoted to ACTIVE:
    Handles chunk placement, integrity verification, auto-healing, and worker monitoring.
    """

    def __init__(self, db: StandbyDatabase, journal_client=None):
        self.db = db
        self.journal_client = journal_client
        self.client = httpx.AsyncClient(timeout=10.0)
        self._running = False
        self._watchdog_task: Optional[asyncio.Task] = None

    async def start(self):
        self._running = True
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())
        logger.info("StandbyClusterManager background watchdog started.")

    async def stop(self):
        self._running = False
        if self._watchdog_task:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
        await self.client.aclose()
        logger.info("StandbyClusterManager stopped.")

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
                self.db.mark_worker_status(w["worker_id"], "OFFLINE")
                logger.warning("Worker %s timed out. Marked OFFLINE.", w["worker_id"])
        return healthy

    def select_workers_for_chunk(
        self, healthy_workers: List[Dict[str, Any]], tentative_loads: Dict[str, int]
    ) -> List[Dict[str, Any]]:
        if len(healthy_workers) < config.REPLICATION_FACTOR:
            raise RuntimeError(
                f"Cannot satisfy replication factor {config.REPLICATION_FACTOR}; only {len(healthy_workers)} healthy workers."
            )
        sorted_workers = sorted(
            healthy_workers,
            key=lambda w: tentative_loads.get(w["worker_id"], 0),
        )
        return sorted_workers[: config.REPLICATION_FACTOR]

    async def store_chunk_on_worker(self, worker: Dict[str, Any], chunk_id: str, data: bytes) -> bool:
        url = self._get_worker_url(worker, f"/internal/chunks/{chunk_id}")
        try:
            resp = await self.client.put(url, content=data, headers={"Content-Type": "application/octet-stream"})
            return resp.status_code == 200
        except Exception as e:
            logger.error("Failed to store chunk %s on %s: %s", chunk_id, worker["worker_id"], e)
            return False

    async def fetch_chunk_from_worker(self, worker: Dict[str, Any], chunk_id: str) -> Optional[bytes]:
        url = self._get_worker_url(worker, f"/internal/chunks/{chunk_id}")
        try:
            resp = await self.client.get(url)
            if resp.status_code == 200:
                return resp.content
            return None
        except Exception as e:
            logger.error("Failed to fetch chunk %s from %s: %s", chunk_id, worker["worker_id"], e)
            return None

    async def delete_chunk_from_worker(self, worker: Dict[str, Any], chunk_id: str) -> bool:
        url = self._get_worker_url(worker, f"/internal/chunks/{chunk_id}")
        try:
            resp = await self.client.delete(url)
            return resp.status_code in (200, 204)
        except Exception as e:
            logger.error("Failed to delete chunk %s on %s: %s", chunk_id, worker["worker_id"], e)
            return False

    async def delete_object_chunks(self, object_id: str, chunks: List[Dict[str, Any]]):
        workers_map = {w["worker_id"]: w for w in self.db.get_all_workers()}
        delete_tasks = []
        for ch in chunks:
            chunk_id = ch["chunk_id"]
            for worker_id in ch["replicas"]:
                if worker_id in workers_map:
                    worker = workers_map[worker_id]
                    delete_tasks.append(self.delete_chunk_from_worker(worker, chunk_id))
        if delete_tasks:
            await asyncio.gather(*delete_tasks, return_exceptions=True)

    async def retrieve_chunk_with_integrity(
        self,
        chunk_id: str,
        expected_checksum: str,
        replica_worker_ids: List[str],
    ) -> Optional[bytes]:
        workers_map = {w["worker_id"]: w for w in self.db.get_all_workers()}
        valid_data: Optional[bytes] = None
        corrupted_workers = []

        for w_id in replica_worker_ids:
            if w_id not in workers_map:
                continue
            worker = workers_map[w_id]
            data = await self.fetch_chunk_from_worker(worker, chunk_id)
            if data is None:
                continue

            calculated_hash = hashlib.sha256(data).hexdigest()
            if calculated_hash == expected_checksum:
                valid_data = data
                break
            else:
                logger.warning("DATA CORRUPTION DETECTED on worker %s for chunk %s!", w_id, chunk_id)
                corrupted_workers.append(worker)

        if valid_data is not None:
            for corrupt_worker in corrupted_workers:
                asyncio.create_task(
                    self.store_chunk_on_worker(corrupt_worker, chunk_id, valid_data)
                )
            return valid_data
        return None

    async def _watchdog_loop(self):
        while self._running:
            try:
                await self._check_health_and_rebalance()
            except Exception as e:
                logger.error("Error in watchdog loop: %s", e)
            await asyncio.sleep(config.HEAL_CHECK_INTERVAL)

    async def _check_health_and_rebalance(self):
        healthy_workers = await self.get_healthy_workers()
        healthy_map = {w["worker_id"]: w for w in healthy_workers}
        all_chunks = self.db.get_all_chunks()
        worker_counts = self.db.get_worker_chunk_counts()

        for ch in all_chunks:
            chunk_id = ch["chunk_id"]
            expected_checksum = ch["checksum"]
            replicas = ch["replicas"]
            active_replicas = [r for r in replicas if r in healthy_map]

            if len(active_replicas) < config.REPLICATION_FACTOR and len(active_replicas) > 0:
                candidates = [w for w in healthy_workers if w["worker_id"] not in replicas]
                if not candidates:
                    continue
                candidates.sort(key=lambda w: worker_counts.get(w["worker_id"], 0))
                target_worker = candidates[0]
                survivor_worker = healthy_map[active_replicas[0]]
                chunk_data = await self.fetch_chunk_from_worker(survivor_worker, chunk_id)
                if chunk_data is None:
                    continue
                if hashlib.sha256(chunk_data).hexdigest() != expected_checksum:
                    continue

                stored = await self.store_chunk_on_worker(target_worker, chunk_id, chunk_data)
                if stored:
                    self.db.add_replica(chunk_id, target_worker["worker_id"])
                    if self.journal_client:
                        try:
                            await self.journal_client.log_edit("OP_ADD_REPLICA", {
                                "chunk_id": chunk_id,
                                "worker_id": target_worker["worker_id"],
                            })
                        except Exception:
                            pass
                    for off_r in [r for r in replicas if r not in healthy_map]:
                        self.db.remove_replica(chunk_id, off_r)
                        if self.journal_client:
                            try:
                                await self.journal_client.log_edit("OP_REMOVE_REPLICA", {
                                    "chunk_id": chunk_id,
                                    "worker_id": off_r,
                                })
                            except Exception:
                                pass
                    worker_counts[target_worker["worker_id"]] = worker_counts.get(target_worker["worker_id"], 0) + 1

