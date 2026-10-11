#!/usr/bin/env bash
set -e
KAFKA_DIR="$HOME/kafka"
CLUSTER_ID=$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)
echo "Generated cluster ID: $CLUSTER_ID"
"$KAFKA_DIR/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$KAFKA_DIR/config/server.properties" --standalone --ignore-formatted
echo "Storage formatted successfully."
