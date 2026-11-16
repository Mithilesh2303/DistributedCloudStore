import asyncio
import logging
from typing import Optional

import httpx
try:
    from worker.src import config
except (ImportError, ModuleNotFoundError):
    from src import config

logger = logging.getLogger("worker.heartbeat")


class HeartbeatClient:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=5.0)
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Heartbeat client started for worker %s", config.WORKER_ID)

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self.client.aclose()
        logger.info("Heartbeat client stopped for worker %s", config.WORKER_ID)

    async def _register(self) -> bool:
        url = f"{config.CONTROLLER_URL}/internal/workers/register"
        payload = {
            "worker_id": config.WORKER_ID,
            "host": config.WORKER_HOST,
            "port": config.WORKER_PORT,
        }
        try:
            resp = await self.client.post(url, json=payload)
            if resp.status_code == 200:
                logger.info("Successfully registered worker %s with controller at %s",
                            config.WORKER_ID, url)
                return True
            logger.warning("Controller returned HTTP %d on registration: %s",
                           resp.status_code, resp.text)
            return False
        except Exception as e:
            logger.warning("Failed to connect to controller at %s for registration: %s",
                           url, e)
            return False

    async def _send_heartbeat(self) -> bool:
        url = f"{config.CONTROLLER_URL}/internal/workers/heartbeat"
        payload = {"worker_id": config.WORKER_ID}
        try:
            resp = await self.client.post(url, json=payload)
            if resp.status_code == 200:
                return True
            if resp.status_code == 404:
                logger.warning("Controller did not recognize worker %s. Re-registering...",
                               config.WORKER_ID)
                await self._register()
                return False
            logger.warning("Controller returned HTTP %d on heartbeat: %s",
                           resp.status_code, resp.text)
            return False
        except Exception as e:
            logger.warning("Failed to send heartbeat to controller at %s: %s", url, e)
            return False

    async def _run_loop(self):
        # Initial registration with retry
        registered = False
        while self._running and not registered:
            registered = await self._register()
            if not registered:
                await asyncio.sleep(1.5)

        # Heartbeat loop
        while self._running:
            await asyncio.sleep(config.HEARTBEAT_INTERVAL)
            if not self._running:
                break
            await self._send_heartbeat()
