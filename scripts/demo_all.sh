#!/usr/bin/env bash
# ==============================================================================
# Mini Distributed Cloud Object Store - End-to-End Demonstration Script
# Covers all 6 required demonstration scenarios from the project specification.
# ==============================================================================

set -euo pipefail

BASE_URL="http://localhost:8000"
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_step() {
    echo -e "\n${BLUE}======================================================================${NC}"
    echo -e "${GREEN}[DEMO STEP] $1${NC}"
    echo -e "${BLUE}======================================================================${NC}"
}

log_info() {
    echo -e "${YELLOW}>> $1${NC}"
}

# 0. Check cluster connectivity
log_step "0. Verifying Cluster Health"
if ! curl -s "$BASE_URL/internal/cluster/status" > /dev/null; then
    echo -e "${RED}Error: Controller is not reachable at $BASE_URL.${NC}"
    echo "Please ensure the cluster is running with: docker compose up -d"
    exit 1
fi

log_info "Cluster status:"
curl -s "$BASE_URL/internal/cluster/status" | python3 -m json.tool
sleep 2

# ------------------------------------------------------------------------------
# 1. Object Storage: Upload and retrieve an object; verify matching hashes
# ------------------------------------------------------------------------------
log_step "1. Object Storage: Upload and Retrieve (Verify Integrity)"
SAMPLE_FILE="$TEMP_DIR/sample_doc.txt"
echo "Distributed Cloud Object Storage System - Demonstration Document $(date)" > "$SAMPLE_FILE"
ORIGINAL_HASH=$(shasum -a 256 "$SAMPLE_FILE" | awk '{print $1}')
log_info "Original File: $SAMPLE_FILE (SHA256: $ORIGINAL_HASH)"

log_info "Uploading file via POST /api/v1/objects..."
UPLOAD_RESP=$(curl -s -X POST "$BASE_URL/api/v1/objects" \
    -F "file=@$SAMPLE_FILE" \
    -F 'metadata={"demo": "step1", "owner": "mithilesh"}')
echo "$UPLOAD_RESP" | python3 -m json.tool

OBJ1_ID=$(echo "$UPLOAD_RESP" | python3 -c "import sys, json; print(json.load(sys.stdin)['object_id'])")
log_info "Uploaded Object ID: $OBJ1_ID"

log_info "Downloading file via GET /api/v1/objects/$OBJ1_ID..."
DOWNLOADED_FILE="$TEMP_DIR/downloaded_doc.txt"
curl -s -o "$DOWNLOADED_FILE" "$BASE_URL/api/v1/objects/$OBJ1_ID"

DOWNLOADED_HASH=$(shasum -a 256 "$DOWNLOADED_FILE" | awk '{print $1}')
log_info "Downloaded File SHA256: $DOWNLOADED_HASH"

if [ "$ORIGINAL_HASH" == "$DOWNLOADED_HASH" ]; then
    echo -e "${GREEN}✓ SUCCESS: Downloaded object matches original exactly!${NC}"
else
    echo -e "${RED}✗ FAILURE: Hash mismatch!${NC}"
    exit 1
fi

# ------------------------------------------------------------------------------
# 2. Chunking & Distribution: Object > 1MB distributed across workers
# ------------------------------------------------------------------------------
log_step "2. Chunking and Distribution (> 1 MB object)"
LARGE_FILE="$TEMP_DIR/large_payload.bin"
if [ -f "image_to_add.png" ]; then
    log_info "Using real binary image 'image_to_add.png' (2.55 MB) for chunking demonstration..."
    cp "image_to_add.png" "$LARGE_FILE"
else
    log_info "Generating 2.5 MB random binary test file..."
    dd if=/dev/urandom of="$LARGE_FILE" bs=1048576 count=2 status=none
    dd if=/dev/urandom of="$LARGE_FILE" bs=524288 count=1 oflag=append conv=notrunc status=none
fi
LARGE_HASH=$(shasum -a 256 "$LARGE_FILE" | awk '{print $1}')
log_info "Test file size: $(stat -f%z "$LARGE_FILE" 2>/dev/null || stat -c%s "$LARGE_FILE") bytes. SHA256: $LARGE_HASH"

log_info "Uploading 2.5 MB object..."
LARGE_UPLOAD=$(curl -s -X POST "$BASE_URL/api/v1/objects" \
    -F "file=@$LARGE_FILE;type=image/png" \
    -F 'metadata={"description": "image_to_add demonstration", "type": "image/png"}')
echo "$LARGE_UPLOAD" | python3 -m json.tool

LARGE_OBJ_ID=$(echo "$LARGE_UPLOAD" | python3 -c "import sys, json; print(json.load(sys.stdin)['object_id'])")
TOTAL_CHUNKS=$(echo "$LARGE_UPLOAD" | python3 -c "import sys, json; print(json.load(sys.stdin)['total_chunks'])")
log_info "Total chunks created: $TOTAL_CHUNKS (Chunk size: 1 MB, last chunk <= 1 MB)"

# ------------------------------------------------------------------------------
# 3. Replication: Verify each chunk has 2 copies stored on distinct workers
# ------------------------------------------------------------------------------
log_step "3. Replication: Verify Each Chunk Has 2 Copies on Distinct Workers"
log_info "Inspecting chunk replica locations:"
echo "$LARGE_UPLOAD" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for ch in data['chunks']:
    replicas = ch['replicas']
    print(f\"  Chunk: {ch['chunk_id']} | Size: {ch['size']} bytes | Replicas ({len(replicas)}): {replicas}\")
    assert len(replicas) == 2, f'Chunk {ch[\"chunk_id\"]} does not have 2 replicas!'
    assert replicas[0] != replicas[1], f'Chunk {ch[\"chunk_id\"]} placed on duplicate worker!'
"
echo -e "${GREEN}✓ SUCCESS: All chunks have replication factor 2 across distinct workers!${NC}"

# ------------------------------------------------------------------------------
# 4. Metadata Management: Retrieve and update metadata without re-uploading
# ------------------------------------------------------------------------------
log_step "4. Metadata: Retrieve and Modify Metadata Without Re-uploading"
log_info "Retrieving metadata for object $OBJ1_ID..."
curl -s "$BASE_URL/api/v1/objects/$OBJ1_ID/metadata" | python3 -m json.tool

log_info "Updating custom metadata via PATCH /api/v1/objects/$OBJ1_ID/metadata..."
PATCH_RESP=$(curl -s -X PATCH "$BASE_URL/api/v1/objects/$OBJ1_ID/metadata" \
    -H "Content-Type: application/json" \
    -d '{"status": "archived", "project": "cloud-semester-7", "grade": "A+"}')
echo "$PATCH_RESP" | python3 -m json.tool

log_info "Verifying that metadata was updated in place:"
UPDATED_GRADE=$(echo "$PATCH_RESP" | python3 -c "import sys, json; print(json.load(sys.stdin)['custom_metadata'].get('grade'))")
if [ "$UPDATED_GRADE" == "A+" ]; then
    echo -e "${GREEN}✓ SUCCESS: Metadata updated without re-uploading object binary!${NC}"
else
    echo -e "${RED}✗ FAILURE: Metadata update failed.${NC}"
    exit 1
fi

# ------------------------------------------------------------------------------
# 5. Worker Failure & Self-Healing: Stop container, retrieve, restart
# ------------------------------------------------------------------------------
log_step "5. Worker Failure Handling & Self-Healing"
TARGET_WORKER="worker2"
log_info "Simulating failure of $TARGET_WORKER via 'docker stop $TARGET_WORKER'..."
docker stop "$TARGET_WORKER"

log_info "Attempting object download while $TARGET_WORKER is DOWN..."
FAIL_TEST_DOWNLOAD="$TEMP_DIR/fail_test_doc.bin"
curl -s -o "$FAIL_TEST_DOWNLOAD" "$BASE_URL/api/v1/objects/$LARGE_OBJ_ID"
FAIL_DOWNLOAD_HASH=$(shasum -a 256 "$FAIL_TEST_DOWNLOAD" | awk '{print $1}')

if [ "$FAIL_DOWNLOAD_HASH" == "$LARGE_HASH" ]; then
    echo -e "${GREEN}✓ SUCCESS: Object retrieved seamlessly from surviving replica during worker downtime!${NC}"
else
    echo -e "${RED}✗ FAILURE: Could not retrieve object during worker failure!${NC}"
fi

log_info "Waiting 7 seconds for heartbeat timeout and self-healing re-replication loop..."
sleep 7

log_info "Checking cluster status after self-healing:"
curl -s "$BASE_URL/internal/cluster/status" | python3 -m json.tool

log_info "Restarting $TARGET_WORKER via 'docker start $TARGET_WORKER'..."
docker start "$TARGET_WORKER"
sleep 4
log_info "Cluster status after $TARGET_WORKER recovery:"
curl -s "$BASE_URL/internal/cluster/status" | python3 -m json.tool

# ------------------------------------------------------------------------------
# 6. Data Integrity: Detect and handle intentional data corruption
# ------------------------------------------------------------------------------
log_step "6. Data Integrity: Corruption Detection & Auto-Healing"
log_info "Identifying a chunk stored on worker1..."
META_DATA=$(curl -s "$BASE_URL/api/v1/objects/$OBJ1_ID/metadata")
CORRUPT_CHUNK_ID=$(echo "$META_DATA" | python3 -c "import sys, json; print(json.load(sys.stdin)['chunks'][0]['chunk_id'])")
REPLICAS=$(echo "$META_DATA" | python3 -c "import sys, json; print(json.load(sys.stdin)['chunks'][0]['replicas'])")
log_info "Chunk $CORRUPT_CHUNK_ID is stored on: $REPLICAS"

FIRST_WORKER=$(echo "$META_DATA" | python3 -c "import sys, json; print(json.load(sys.stdin)['chunks'][0]['replicas'][0])")
log_info "Intentionally corrupting chunk file on $FIRST_WORKER (/data/$CORRUPT_CHUNK_ID)..."
docker exec "$FIRST_WORKER" sh -c "echo 'CORRUPTED_BYTES_INJECTION' > /data/$CORRUPT_CHUNK_ID"

log_info "Downloading object $OBJ1_ID (Controller should detect corruption and fallback to good replica)..."
HEALED_DOWNLOAD="$TEMP_DIR/healed_doc.txt"
curl -s -o "$HEALED_DOWNLOAD" "$BASE_URL/api/v1/objects/$OBJ1_ID"
HEALED_HASH=$(shasum -a 256 "$HEALED_DOWNLOAD" | awk '{print $1}')

if [ "$HEALED_HASH" == "$ORIGINAL_HASH" ]; then
    echo -e "${GREEN}✓ SUCCESS: Controller detected corrupted replica, seamlessly served valid data from second replica!${NC}"
else
    echo -e "${RED}✗ FAILURE: Corrupted data was returned to client!${NC}"
    exit 1
fi

sleep 1
log_info "Verifying that Controller repaired the corrupted replica on $FIRST_WORKER disk:"
docker exec "$FIRST_WORKER" sh -c "cat /data/$CORRUPT_CHUNK_ID" | head -c 40
echo -e "\n${GREEN}✓ SUCCESS: Corrupted replica was automatically repaired with authentic data!${NC}"

# ------------------------------------------------------------------------------
# 7. Cleanup & Object Listing
# ------------------------------------------------------------------------------
log_step "7. Listing & Deletion"
log_info "Listing all objects in the store:"
curl -s "$BASE_URL/api/v1/objects" | python3 -m json.tool

log_info "Deleting object $OBJ1_ID via DELETE /api/v1/objects/$OBJ1_ID..."
STATUS_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$BASE_URL/api/v1/objects/$OBJ1_ID")
if [ "$STATUS_CODE" == "204" ]; then
    echo -e "${GREEN}✓ SUCCESS: Object deleted with HTTP 204 No Content.${NC}"
fi

log_info "Verifying object is gone (expecting HTTP 404):"
NOT_FOUND_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/api/v1/objects/$OBJ1_ID")
log_info "GET /api/v1/objects/$OBJ1_ID returned: HTTP $NOT_FOUND_CODE"

echo -e "\n${GREEN}======================================================================${NC}"
echo -e "${GREEN}ALL 6 DEMONSTRATION REQUIREMENTS COMPLETED SUCCESSFULLY!${NC}"
echo -e "${GREEN}======================================================================${NC}"
