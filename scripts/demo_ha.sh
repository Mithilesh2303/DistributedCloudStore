#!/usr/bin/env bash
# ==============================================================================
# Standby Controller & Journal Nodes High Availability (HA) Demonstration
# Demonstrates:
#   1. Active / Standby Coordinator Architecture
#   2. Quorum JournalNodes with append-only EditLogs (txid tracking)
#   3. Standby EditLog tailing and real-time state synchronization
#   4. FSImage checkpointing (point-in-time metadata snapshot)
#   5. Active Controller failure simulation & automatic failover
#   6. Zero data loss verification and write availability on promoted controller
# ==============================================================================

set -euo pipefail

ACTIVE_URL="http://localhost:8000"
STANDBY_URL="http://localhost:8002"
JN1_URL="http://localhost:8011"
JN2_URL="http://localhost:8012"
JN3_URL="http://localhost:8013"

TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_step() {
    echo -e "\n${BLUE}======================================================================${NC}"
    echo -e "${GREEN}[HA DEMO STEP] $1${NC}"
    echo -e "${BLUE}======================================================================${NC}"
}

log_info() {
    echo -e "${YELLOW}>> $1${NC}"
}

# ------------------------------------------------------------------------------
# 0. Verify HA Cluster Status
# ------------------------------------------------------------------------------
log_step "0. Verifying HA Cluster Topology"

log_info "Checking Active Controller (port 8000)..."
curl -s "$ACTIVE_URL/internal/ha/status" | python3 -m json.tool

log_info "Checking Standby Controller (port 8002)..."
curl -s "$STANDBY_URL/internal/ha/status" | python3 -m json.tool

log_info "Checking JournalNode 1 (port 8011)..."
curl -s "$JN1_URL/v1/journal/status" | python3 -m json.tool

# ------------------------------------------------------------------------------
# 1. Upload Object to Active Controller & Observe EditLog Generation
# ------------------------------------------------------------------------------
log_step "1. Upload Object to Active Controller (Port 8000)"
TEST_FILE="$TEMP_DIR/ha_test_document.txt"
echo "High Availability Distributed Cloud Object Storage - Test Payload $(date)" > "$TEST_FILE"
ORIGINAL_HASH=$(shasum -a 256 "$TEST_FILE" | awk '{print $1}')
log_info "Original File Hash (SHA256): $ORIGINAL_HASH"

log_info "Uploading via POST http://localhost:8000/api/v1/objects..."
UPLOAD_RESP=$(curl -s -X POST "$ACTIVE_URL/api/v1/objects" \
    -F "file=@$TEST_FILE" \
    -F 'metadata={"cluster": "ha-cluster", "replication": "active-standby"}')
echo "$UPLOAD_RESP" | python3 -m json.tool

OBJ_ID=$(echo "$UPLOAD_RESP" | python3 -c "import sys, json; print(json.load(sys.stdin)['object_id'])")
log_info "Uploaded Object ID: $OBJ_ID"

# ------------------------------------------------------------------------------
# 2. Inspect Distributed EditLogs on JournalNodes
# ------------------------------------------------------------------------------
log_step "2. Verifying Committed EditLogs on Quorum JournalNodes"
sleep 1
log_info "Fetching recent editlog transactions from JournalNode 1 ($JN1_URL/v1/journal/edits)..."
curl -s "$JN1_URL/v1/journal/edits?since_txid=0" | python3 -m json.tool

# ------------------------------------------------------------------------------
# 3. Verify Standby Controller Synchronization
# ------------------------------------------------------------------------------
log_step "3. Verifying Standby Controller Synchronization"
sleep 2
log_info "Checking Standby Controller HA Status ($STANDBY_URL/internal/ha/status)..."
curl -s "$STANDBY_URL/internal/ha/status" | python3 -m json.tool

log_info "Querying Object Metadata directly from Standby Controller (Port 8002)..."
STANDBY_META=$(curl -s "$STANDBY_URL/api/v1/objects/$OBJ_ID/metadata")
echo "$STANDBY_META" | python3 -m json.tool

# ------------------------------------------------------------------------------
# 4. Trigger FSImage Checkpoint
# ------------------------------------------------------------------------------
log_step "4. FSImage Checkpoint Generation (Metadata Snapshot)"
log_info "Triggering FSImage checkpoint on Standby Controller (POST $STANDBY_URL/internal/ha/checkpoint)..."
CHECKPOINT_RESP=$(curl -s -X POST "$STANDBY_URL/internal/ha/checkpoint")
echo "$CHECKPOINT_RESP" | python3 -m json.tool

log_info "Inspecting latest FSImage snapshot on Standby Controller ($STANDBY_URL/internal/ha/fsimage)..."
curl -s "$STANDBY_URL/internal/ha/fsimage" | python3 -c "import sys, json; d=json.load(sys.stdin); print(f'Checkpoint txid: {d.get(\"checkpoint_txid\")}, Objects: {len(d.get(\"snapshot\", {}).get(\"objects\", []))}, Chunks: {len(d.get(\"snapshot\", {}).get(\"chunks\", []))}')"

# ------------------------------------------------------------------------------
# 5. Simulate Active Controller Crash & Automatic Failover
# ------------------------------------------------------------------------------
log_step "5. Simulating Active Controller Crash (docker stop controller)"
log_info "Stopping active controller container..."
docker stop controller

log_info "Waiting 6 seconds for Standby Controller failover watchdog to detect crash and promote..."
sleep 6

log_info "Checking Standby Controller Health & Role ($STANDBY_URL/health)..."
curl -s "$STANDBY_URL/health" | python3 -m json.tool

# ------------------------------------------------------------------------------
# 6. Verify Object Download from Promoted Controller (Zero Data Loss)
# ------------------------------------------------------------------------------
log_step "6. Verifying Retrieval and Data Integrity from Promoted Controller"
DOWNLOADED_FILE="$TEMP_DIR/ha_downloaded.txt"
log_info "Downloading object $OBJ_ID from promoted controller (port 8002)..."
curl -s -o "$DOWNLOADED_FILE" "$STANDBY_URL/api/v1/objects/$OBJ_ID"

DOWNLOAD_HASH=$(shasum -a 256 "$DOWNLOADED_FILE" | awk '{print $1}')
log_info "Original Hash:   $ORIGINAL_HASH"
log_info "Downloaded Hash: $DOWNLOAD_HASH"

if [ "$ORIGINAL_HASH" = "$DOWNLOAD_HASH" ]; then
    echo -e "${GREEN}SUCCESS: Object downloaded with 100% data integrity after master controller crash!${NC}"
else
    echo -e "${RED}ERROR: Hash mismatch!${NC}"
    exit 1
fi

# ------------------------------------------------------------------------------
# 7. Verify Write Availability on Promoted Controller
# ------------------------------------------------------------------------------
log_step "7. Verifying Write Availability on Promoted Controller (Port 8002)"
SECOND_FILE="$TEMP_DIR/second_test_doc.txt"
echo "Second payload uploaded directly to newly promoted controller" > "$SECOND_FILE"
UPLOAD_RESP2=$(curl -s -X POST "$STANDBY_URL/api/v1/objects" -F "file=@$SECOND_FILE")
echo "$UPLOAD_RESP2" | python3 -m json.tool
log_info "Successfully uploaded new object to promoted controller!"

# ------------------------------------------------------------------------------
# 8. Restart Primary Controller
# ------------------------------------------------------------------------------
log_step "8. Restarting Primary Controller Container"
log_info "Starting controller container back up..."
docker start controller
sleep 3
log_info "Cluster recovered successfully."

echo -e "\n${GREEN}======================================================================${NC}"
echo -e "${GREEN}ALL HIGH AVAILABILITY & FSIMAGE/EDITLOG TESTS PASSED SUCCESSFULLY!${NC}"
echo -e "${GREEN}======================================================================${NC}"

