import os

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 1024 * 1024))  # 1 MB default
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", 2))
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", 6.0))  # seconds
HEAL_CHECK_INTERVAL = float(os.getenv("HEAL_CHECK_INTERVAL", 2.0))  # seconds

DATA_DIR = os.getenv("DATA_DIR", "/data")
DATABASE_PATH = os.getenv("DATABASE_PATH", os.path.join(DATA_DIR, "metadata.db"))
FSIMAGE_DIR = os.getenv("FSIMAGE_DIR", os.path.join(DATA_DIR, "fsimages"))

HOST = os.getenv("STANDBY_HOST", "0.0.0.0")
PORT = int(os.getenv("STANDBY_PORT", 8000))

# High Availability (HA) & Journaling configuration
ACTIVE_CONTROLLER_URL = os.getenv("ACTIVE_CONTROLLER_URL", "http://controller:8000")
JOURNAL_NODES = [
    url.strip()
    for url in os.getenv("JOURNAL_NODES", "http://journalnode1:8010,http://journalnode2:8010,http://journalnode3:8010").split(",")
    if url.strip()
]
SYNC_INTERVAL = float(os.getenv("SYNC_INTERVAL", 1.0))
CHECKPOINT_INTERVAL = float(os.getenv("CHECKPOINT_INTERVAL", 15.0))
FAILOVER_TIMEOUT = float(os.getenv("FAILOVER_TIMEOUT", 5.0))

