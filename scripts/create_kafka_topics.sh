#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Load .env if present (safe)
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

RAW_TOPIC=${RAW_TOPIC:-raw.openaq}
CLEAN_TOPIC=${CLEAN_TOPIC:-clean.openaq}

docker compose -f docker/docker-compose.yml exec -T kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --topic "$RAW_TOPIC" --partitions 3 --replication-factor 1

docker compose -f docker/docker-compose.yml exec -T kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --topic "$CLEAN_TOPIC" --partitions 3 --replication-factor 1

echo "Topics:"
docker compose -f docker/docker-compose.yml exec -T kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
