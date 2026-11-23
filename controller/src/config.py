import os

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 1024 * 1024))  # 1 MB default
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", 2))
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", 6.0))  # seconds
HEAL_CHECK_INTERVAL = float(os.getenv("HEAL_CHECK_INTERVAL", 2.0))  # seconds

DATA_DIR = os.getenv("DATA_DIR", "/data")
DATABASE_PATH = os.getenv("DATABASE_PATH", os.path.join(DATA_DIR, "metadata.db"))

HOST = os.getenv("CONTROLLER_HOST", "0.0.0.0")
PORT = int(os.getenv("CONTROLLER_PORT", 8000))

# High Availability (HA) & Journaling configuration
CONTROLLER_ROLE = os.getenv("CONTROLLER_ROLE", "ACTIVE")
JOURNAL_NODES = [
    url.strip()
    for url in os.getenv("JOURNAL_NODES", "").split(",")
    if url.strip()
]
