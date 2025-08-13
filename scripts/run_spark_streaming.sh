#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."

# Load .env safely
if [[ -f .env ]]; then
  set -o allexport; source .env; set +o allexport
fi

mkdir -p "${CHECKPOINT_ROOT:-.checkpoints}"/{clean,agg_kafka,agg_ddb,deadletter,debug_console}

# Detect pyspark version to pick matching connector (defaults to 3.5.6)
PYSPARK_VER="$(python - <<'PY'
import pyspark, sys
print(".".join(pyspark.__version__.split(".")[:3]))
PY
)"
SPARK_KAFKA_VERSION="${SPARK_KAFKA_VERSION:-$PYSPARK_VER}"

echo "Starting Spark job for OpenAQ streaming..."
echo "Using Kafka connector version: $SPARK_KAFKA_VERSION"

spark-submit \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.adaptive.enabled=false \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_KAFKA_VERSION},org.apache.spark:spark-token-provider-kafka-0-10_2.12:${SPARK_KAFKA_VERSION}" \
  streaming/spark_job.py
