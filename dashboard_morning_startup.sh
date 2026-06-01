#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# dashboard_morning_startup.sh
# Runs at 8:00 AM every weekday via cron
# 1. Fix Dev_colo contract file permissions for today
# 2. Kill old worker (stuck on yesterday's log)
# 3. Start fresh worker
# 4. Verify worker is reading today's fills
# ─────────────────────────────────────────────────────────────────────────────

# ── Config ────────────────────────────────────────────────────────────────────
VENV="/home/report/devstudio/Prashant/Live_Dashboard/venv/bin/activate"
PROD="/home/report/devstudio/Prashant/Live_Dashboard/Prod"
WORKER="dashboard_worker_prod.py"
WORKER_LOG="/home/report/devstudio/Prashant/Live_Dashboard/logs/dashboard_worker.log"

COLO_HOST="192.168.71.200"
COLO_USER="Data_colo"
COLO_PASS="Datacolo@2026"
DEV_USER="Dev_colo"
DEV_PASS="Devcolo@2026"

TODAY=$(date +%Y%m%d)
CONTRACT_FILE="/data/pcapdata/${TODAY}/fo_contract_stream_info_${TODAY}.csv"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

log() { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] $1"; }

log "${YELLOW}=== Dashboard Morning Startup — ${TODAY} ===${NC}"

# ── Step 1: Fix contract file permissions ─────────────────────────────────────
log "Step 1: Fixing contract file permissions on colo..."
sshpass -p "${DEV_PASS}" ssh -o StrictHostKeyChecking=no \
    ${DEV_USER}@${COLO_HOST} \
    "chmod 644 ${CONTRACT_FILE} 2>/dev/null && echo OK || echo FAIL"

# Verify Data_colo can read it
CHECK=$(sshpass -p "${COLO_PASS}" ssh -o StrictHostKeyChecking=no \
    ${COLO_USER}@${COLO_HOST} \
    "test -r ${CONTRACT_FILE} && echo READABLE || echo NOT_READABLE")

if [ "$CHECK" = "READABLE" ]; then
    log "${GREEN}[OK]${NC}   Contract file readable: ${CONTRACT_FILE}"
else
    log "${RED}[WARN]${NC} Contract file not readable — worker may fail to load token map"
fi

# ── Step 2: Kill old worker ────────────────────────────────────────────────────
log "Step 2: Killing old worker..."
PIDS=$(pgrep -f "${WORKER}" 2>/dev/null)
if [ -n "$PIDS" ]; then
    kill $PIDS 2>/dev/null
    sleep 3
    STILL=$(pgrep -f "${WORKER}" 2>/dev/null)
    if [ -n "$STILL" ]; then
        kill -9 $STILL 2>/dev/null
        sleep 1
    fi
    log "${GREEN}[OK]${NC}   Old worker killed (PID: $PIDS)"
else
    log "${YELLOW}[INFO]${NC} No existing worker found"
fi

# ── Step 2b: Clear stale Redis keys from previous day ────────────────────────
log "Step 2b: Clearing stale Redis keys..."
/home/report/devstudio/Prashant/Live_Dashboard/venv/bin/python3 -c "
import redis
r = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
for k in r.keys('dashboard:*'):
    r.delete(k)
    print('  Deleted: ' + k)
print('Redis cleared')
"

# ── Step 3: Start fresh worker ────────────────────────────────────────────────
log "Step 3: Starting fresh worker..."
mkdir -p $(dirname ${WORKER_LOG})
source ${VENV}
cd ${PROD}
nohup python3 ${WORKER} >> ${WORKER_LOG} 2>&1 &
NEW_PID=$!
sleep 3

if kill -0 ${NEW_PID} 2>/dev/null; then
    log "${GREEN}[OK]${NC}   Worker started — PID: ${NEW_PID}"
else
    log "${RED}[ERROR]${NC} Worker failed to start — check log: ${WORKER_LOG}"
    tail -20 ${WORKER_LOG}
    exit 1
fi

# ── Step 4: Verify worker reading today's fills ───────────────────────────────
log "Step 4: Verifying worker reads today's fills..."
sleep 15  # give worker time to connect and parse fills

# Check log for today's date and fill count
TODAY_DASH=$(date +%Y-%m-%d)
FILL_LINE=$(grep "fill lines for date ${TODAY_DASH:0:4}-${TODAY_DASH:5:2}-${TODAY_DASH:8:2}\|fill lines for date ${TODAY}" ${WORKER_LOG} 2>/dev/null | tail -1)
EOD_LINE=$(grep "EOD loaded\|EOD CSV not found\|zero overnight" ${WORKER_LOG} 2>/dev/null | tail -1)
POS_LINE=$(grep "Positions built:" ${WORKER_LOG} 2>/dev/null | tail -1)

log "Fill check : ${FILL_LINE:-'No fill line found yet'}"
log "EOD check  : ${EOD_LINE:-'No EOD line found yet'}"
log "Positions  : ${POS_LINE:-'No position line found yet'}"

# Check positions > 0
POS_COUNT=$(echo "$POS_LINE" | grep -oP '\d+ tokens' | grep -oP '\d+' || echo "0")
if [ "${POS_COUNT}" -gt "0" ] 2>/dev/null; then
    log "${GREEN}[OK]${NC}   ${POS_COUNT} positions built successfully"
else
    log "${YELLOW}[WARN]${NC} 0 positions — fills may not be available yet at 8 AM (market opens 9:15)"
fi

log "${GREEN}=== Startup complete ===${NC}"
log "Worker PID : ${NEW_PID}"
log "Worker Log : ${WORKER_LOG}"
log "Tail logs  : tail -f ${WORKER_LOG}"
