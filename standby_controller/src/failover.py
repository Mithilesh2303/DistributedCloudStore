import asyncio
import logging
import time
from typing import Callable, Optional
import httpx

try:
    from standby_controller.src import config
    from standby_controller.src.syncer import EditLogSyncer
except (ImportError, ModuleNotFoundError):
    from src import config
    from src.syncer import EditLogSyncer

logger = logging.getLogger("standby.failover")


class FailoverManager:
    """
    Watches the Active Controller node.
    If the Active Controller becomes unreachable for FAILOVER_TIMEOUT seconds,
    automatically promotes this Standby Controller to ACTIVE.
    """

    def __init__(self, syncer: EditLogSyncer, on_promote_callback: Callable):
        self.syncer = syncer
        self.on_promote = on_promote_callback
        self.active_url = config.ACTIVE_CONTROLLER_URL.rstrip("/")
        self.client = httpx.AsyncClient(timeout=2.0)
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None
        self.last_healthy_time = time.time()
        self.is_promoted = False

    async def start(self):
        self._running = True
        self.last_healthy_time = time.time()
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("FailoverManager active monitoring on %s (timeout: %.1fs)",
                    self.active_url, config.FAILOVER_TIMEOUT)

    async def stop(self):
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
        await self.client.aclose()
        logger.info("FailoverManager stopped.")

    async def _check_active_health(self) -> bool:
        try:
            resp = await self.client.get(f"{self.active_url}/health")
            if resp.status_code == 200:
                data = resp.json()
                # Must be active role
                return data.get("role", "ACTIVE") == "ACTIVE"
            return False
        except Exception:
            return False

    async def _monitor_loop(self):
        while self._running and not self.is_promoted:
            healthy = await self._check_active_health()
            now = time.time()

            if healthy:
                self.last_healthy_time = now
            else:
                elapsed = now - self.last_healthy_time
                logger.warning(
                    "Active Controller unreachable (%.1fs elapsed, timeout is %.1fs)...",
                    elapsed, config.FAILOVER_TIMEOUT
                )
                if elapsed >= config.FAILOVER_TIMEOUT:
                    logger.critical(
                        "ACTIVE CONTROLLER FAILURE DETECTED! Initiating automatic failover..."
                    )
                    await self.promote()
                    break

            await asyncio.sleep(1.0)

    async def promote(self):
        if self.is_promoted:
            return

        self.is_promoted = True
        logger.info("Promoting Standby Controller to ACTIVE role...")

        # 1. Drain all pending edits from JournalNodes to achieve zero data loss
        try:
            applied = await self.syncer.sync_once()
            logger.info("Final pre-promotion sync applied %d edits (latest txid: %d)",
                        applied, self.syncer.last_applied_txid)
        except Exception as e:
            logger.error("Error during pre-promotion sync: %s", e)

        # 2. Invoke callback to activate ClusterManager, accept client writes, etc.
        await self.on_promote()
        logger.info("STANDBY CONTROLLER PROMOTION COMPLETE. Now running as ACTIVE Controller.")

