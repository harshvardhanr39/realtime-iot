#!/usr/bin/env bash
set -euo pipefail
RAW_TOPIC=${RAW_TOPIC:-raw.openaq}
CLEAN_TOPIC=${CLEAN_TOPIC:-clean.openaq}

# create topics inside the Kafka container (bitnami path)
docker compose -f docker/docker-compose.yml exec -T kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --topic "$RAW_TOPIC" --partitions 3 --replication-factor 1

docker compose -f docker/docker-compose.yml exec -T kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --topic "$CLEAN_TOPIC" --partitions 3 --replication-factor 1

echo "Topics:"
docker compose -f docker/docker-compose.yml exec -T kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
