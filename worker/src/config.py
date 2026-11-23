import os

WORKER_ID = os.getenv("WORKER_ID", "worker1") # gets env from docker compose or OS
WORKER_HOST = os.getenv("WORKER_HOST", WORKER_ID)
WORKER_PORT = int(os.getenv("WORKER_PORT", 8001))
CONTROLLER_URL = os.getenv("CONTROLLER_URL", "http://controller:8000")
CONTROLLER_URLS = [
    u.strip()
    for u in os.getenv("CONTROLLER_URLS", CONTROLLER_URL).split(",")
    if u.strip()
]
DATA_DIR = os.getenv("DATA_DIR", "/data")
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", 2.0))
