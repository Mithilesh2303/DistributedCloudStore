# Mini Distributed Cloud Object Store

A fault-tolerant, chunk-based distributed cloud object store built with Python and Docker Compose. Slices arbitrary binary objects into 1 MB chunks and distributes them across 4 storage worker nodes with a replication factor of 2, providing automated failure detection, self-healing, and data integrity verification.

---

## Architecture Overview

* **1 Controller Node**: Exposes the client REST API gateway on port `8000`, manages metadata (SQLite with WAL mode), worker health (heartbeat watchdog), chunk placement, and automated re-replication.
* **4 Storage Worker Nodes**: Lightweight storage nodes (`worker1`, `worker2`, `worker3`, `worker4`) on port `8001` with isolated persistent Docker volume mounts (`/data`).
* **Replication Factor**: 2 copies per chunk stored across distinct workers.
* **Chunk Size**: 1 MB (1,048,576 bytes; final chunk $\le$ 1 MB).
* **Network**: Docker bridge network (`storage-net`) with internal service DNS.
* **Orchestration**: Docker Compose without Kubernetes.

For complete architectural diagrams, failure handling protocols, and API specs, see [ARCHITECTURE.md](file:///Users/mithileshsaiyechuri/Acads/Sem%207/Cloud/Project/ARCHITECTURE.md).

---

## Project Structure

```
DistributedCloudStore/
├── ARCHITECTURE.md          # Comprehensive architecture design document
├── README.md                # Build, run, and interaction guide
├── docker-compose.yml       # Cluster deployment definition (Controller + 4 Workers)
├── controller/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── main.py          # FastAPI REST endpoints (Client & Internal APIs)
│       ├── database.py      # SQLite metadata engine (objects, chunks, replicas, workers)
│       ├── manager.py       # Cluster manager, health watchdog, & self-healing loop
│       ├── models.py        # Pydantic schemas
│       └── config.py        # Controller settings
├── worker/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── main.py          # Worker API (PUT/GET/DELETE chunks, list chunks)
│       ├── storage.py       # Atomic persistent file storage in /data
│       ├── heartbeat.py     # Background worker registration & heartbeat reporter
│       └── config.py        # Worker settings
├── tests/
│   └── test_unit.py         # Unit tests for Database and Storage Engine
└── scripts/
    └── demo_all.sh          # Automated end-to-end demo script covering all 6 rubric points
```

---

## Getting Started

### Prerequisites
* Docker and Docker Compose v2+
* Python 3.9+ (optional, for local unit testing)
* `curl` and `shasum` (or `sha256sum`)

### 1. Build and Start the Cluster
```bash
docker compose up -d --build
```

Verify that all 5 containers are running:
```bash
docker compose ps
```

### 2. Verify Cluster Status
Check worker health and registered capacity:
```bash
curl http://localhost:8000/internal/cluster/status
```

---

## Automated Demonstration Script

To run the complete automated test verifying all 6 demonstration requirements from the project specification:
```bash
./scripts/demo_all.sh
```

This script automatically executes and verifies:
1. **Object Storage**: Uploads a test document, downloads it, and verifies matching SHA-256 hashes.
2. **Chunking & Distribution**: Uploads a 2.5 MB binary file, verifies 1 MB chunk slicing, and confirms distribution across workers.
3. **Replication**: Verifies every chunk has exactly 2 copies on distinct workers.
4. **Metadata Management**: Uploads custom metadata, updates it via `PATCH`, and verifies changes without re-uploading the file.
5. **Worker Failure & Self-Healing**: Stops `worker2` (`docker stop worker2`), retrieves the object from surviving copies, monitors the watchdog re-replicating under-replicated chunks, and restarts `worker2` (`docker start worker2`).
6. **Data Integrity & Auto-Healing**: Intentionally injects corrupted bytes into a worker's chunk file; verifies that the Controller detects the corruption, serves the correct data from the healthy replica, and automatically repairs the corrupted file on disk.

---

## Manual API Walkthrough

### 1. Upload an Object
```bash
curl -X POST http://localhost:8000/api/v1/objects \
  -F "file=@sample.pdf" \
  -F 'metadata={"project": "Cloud", "department": "Computer Science"}'
```
*Returns `201 Created` with `object_id`, chunk breakdown, and replica locations.*

### 2. Download an Object
```bash
curl -O -J http://localhost:8000/api/v1/objects/<OBJECT_ID>
```

### 3. Retrieve Object Metadata
```bash
curl http://localhost:8000/api/v1/objects/<OBJECT_ID>/metadata
```

### 4. Update Custom Metadata
```bash
curl -X PATCH http://localhost:8000/api/v1/objects/<OBJECT_ID>/metadata \
  -H "Content-Type: application/json" \
  -d '{"status": "published", "reviewer": "professor"}'
```

### 5. List Stored Objects
```bash
curl "http://localhost:8000/api/v1/objects?page=1&limit=20"
```

### 6. Delete an Object
```bash
curl -X DELETE http://localhost:8000/api/v1/objects/<OBJECT_ID>
```
*Returns `204 No Content` and removes all chunks across worker volumes.*

---

## Running Unit Tests

Run the local unit tests (independent of Docker):
```bash
python3 -m unittest discover tests
```

---

## Stopping the System

To stop all containers:
```bash
docker compose down
```

To stop containers and wipe persistent volumes:
```bash
docker compose down -v
```
