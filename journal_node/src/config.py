import os

JOURNAL_NODE_ID = os.getenv("JOURNAL_NODE_ID", "jn1")
HOST = os.getenv("JOURNAL_HOST", "0.0.0.0")
PORT = int(os.getenv("JOURNAL_PORT", 8010))

DATA_DIR = os.getenv("DATA_DIR", "/data")
JOURNAL_DIR = os.getenv("JOURNAL_DIR", os.path.join(DATA_DIR, "journal"))

