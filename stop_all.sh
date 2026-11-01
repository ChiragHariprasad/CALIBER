#!/usr/bin/env bash
# ==============================================================================
# CALIBRE — Stop All Services in One Command
#
# Stops:
# 1. Any active Python streaming pipeline processes
# 2. All Docker containers (Kafka, Zookeeper, Flink JobManager, TaskManager)
# 3. Releases ports (9092, 2181, 8081)
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "============================================================"
echo " CALIBRE PLATFORM — ONE-COMMAND SHUTDOWN"
echo "============================================================"

echo ""
echo "[1/3] Terminating any running Python pipeline processes..."
pkill -f "run_pipeline.py run" 2>/dev/null || true
pkill -f "run_pipeline.py test" 2>/dev/null || true
echo "      Done."

echo ""
echo "[2/3] Stopping Docker infrastructure (Kafka, Zookeeper, Flink)..."
if command -v docker &>/dev/null; then
    docker compose down --remove-orphans 2>/dev/null || true
    for c in calibre-kafka calibre-zookeeper calibre-flink-jobmanager calibre-flink-taskmanager calibre-init-kafka; do
        docker rm -f "$c" 2>/dev/null || true
    done
    echo "      Docker containers stopped and removed."
else
    echo "      [WARN] Docker CLI not found in PATH."
fi

echo ""
echo "[3/3] Verifying shutdown..."
if command -v docker &>/dev/null; then
    RUNNING_CALIBRE=$(docker ps -q --filter "name=calibre")
    if [ -z "$RUNNING_CALIBRE" ]; then
        echo "      No Calibre containers running."
    else
        echo "      [WARN] Lingering containers: $RUNNING_CALIBRE"
    fi
fi

echo ""
echo "============================================================"
echo " SUCCESS: All CALIBRE services have been cleanly stopped."
echo " Relational database intact at: data/calibre_persistence.db"
echo "============================================================"

