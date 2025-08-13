# streaming/spark_job.py
import os, json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, expr, struct, to_json, window, avg, count, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType, BooleanType
)

# add at top of function
from decimal import Decimal, InvalidOperation

def to_decimal(x):
    if x is None:
        return None
    # Convert through str to avoid binary float artifacts (e.g., 0.30000000004)
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return None

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "raw.openaq")
CLEAN_TOPIC = os.getenv("CLEAN_TOPIC", "clean.openaq")
AGG_TOPIC = os.getenv("AGG_TOPIC", "agg.openaq.1m")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", ".checkpoints")

DDB_ENDPOINT = os.getenv("DDB_ENDPOINT", "http://localhost:8000")
DDB_TABLE = os.getenv("DDB_TABLE", "openaq_timeseries")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# ----- Schema for raw messages (from our producer) -----
parameter_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("units", StringType(), True),
    StructField("display_name", StringType(), True),
])
coordinates_schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
])
raw_schema = StructType([
    StructField("source", StringType(), True),
    StructField("sensor_id", LongType(), True),
    StructField("location_id", LongType(), True),
    StructField("parameter", parameter_schema, True),
    StructField("value", DoubleType(), True),
    StructField("datetime_utc", StringType(), True),
    StructField("datetime_local", StringType(), True),
    StructField("coordinates", coordinates_schema, True),
    StructField("ingested_at_utc", StringType(), True),
])

def build_spark():
    # Resolve the right Kafka package for your PySpark version automatically
    import pyspark
    ver = ".".join(pyspark.__version__.split(".")[:3])
    pkgs = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{ver}"
    return (
        SparkSession.builder
        .appName("OpenAQStreaming")
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", pkgs)
        .config("spark.sql.session.timeZone", "UTC") 
        .config("spark.sql.adaptive.enabled", "false") 
        .getOrCreate()
    )

def main():
    print("Starting Spark job for OpenAQ streaming...")
    spark = build_spark()
    print(f"Spark version: {spark.version}")
    spark.sparkContext.setLogLevel("WARN")
    print(f"Using Kafka bootstrap servers: {KAFKA_BOOTSTRAP}")
    # --- Read from Kafka ---
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "earliest")  # switch to "earliest" to replay
        .load()
    )
    
    print(f"Raw stream schema: {raw}")
    
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json", "timestamp AS kafka_ts")
        .select(from_json(col("json"), raw_schema).alias("d"), col("kafka_ts"))
        .select("d.*", "kafka_ts")
        .withColumn("event_ts", to_timestamp(col("datetime_utc")))
        .filter(col("event_ts").isNotNull() & col("value").isNotNull())
        .withColumn("parameter_id", col("parameter.id"))          # <-- add
        .withColumn("parameter_name", col("parameter.name"))      # (optional)
        .withColumn("parameter_units", col("parameter.units"))    # (optional)
    )
    print(f"Parsed stream schema: {parsed}")
    
    # --- Dedup with watermark (15 min lateness) ---
    deduped = (
        parsed
        .withWatermark("event_ts", "15 minutes")
        .dropDuplicates(["sensor_id", "event_ts", "parameter_id"])
    )

    print(f"Deduped stream schema: {deduped.schema}")
    
    # --- Write clean events back to Kafka ---
    clean_out = (
        deduped
        .select(
            to_json(
                struct(
                    col("source"),
                    col("sensor_id"),
                    col("location_id"),
                    col("parameter").alias("parameter"),
                    col("parameter_id"),                          # <-- add (optional)
                    col("value"),
                    col("datetime_utc"),
                    col("datetime_local"),
                    col("coordinates"),
                    col("ingested_at_utc"),
                    col("event_ts").cast("string").alias("event_ts"),
                )
            ).alias("value")
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", CLEAN_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/clean")
        .outputMode("append")
        .start()
    )
    
    print(f"Clean output stream started: {clean_out}")

    # --- 1-minute tumbling aggregate ---
    agg = (
        deduped
        .groupBy(
            window(col("event_ts"), "1 minute").alias("w"),
            col("sensor_id"),
            col("parameter.name").alias("parameter_name"),
            col("parameter.units").alias("units"),
            col("location_id"),
        )
        .agg(
            avg(col("value")).alias("avg_value"),
            count(lit(1)).alias("count")
        )
        .select(
            col("sensor_id"),
            col("parameter_name"),
            col("units"),
            col("location_id"),
            col("w.start").alias("window_start"),
            col("w.end").alias("window_end"),
            col("avg_value"),
            col("count"),
        )
    )

    # --- Optional: aggregates to Kafka (JSON) ---
    agg_to_kafka = (
        agg
        .select(
            to_json(
                struct(
                    col("sensor_id"),
                    col("parameter_name"),
                    col("units"),
                    col("location_id"),
                    col("window_start").cast("string").alias("window_start_utc"),
                    col("window_end").cast("string").alias("window_end_utc"),
                    col("avg_value"),
                    col("count")
                )
            ).alias("value")
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", AGG_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/agg_kafka")
        .outputMode("update")
        .start()
    )
    
    

    # --- Aggregates to DynamoDB via foreachBatch ---
    def write_to_ddb(batch_df, batch_id: int):
        import boto3
        ddb = boto3.resource("dynamodb", region_name=AWS_REGION, endpoint_url=DDB_ENDPOINT)
        table = ddb.Table(DDB_TABLE)

        with table.batch_writer(overwrite_by_pkeys=["pk", "sk"]) as bw:
            for row in batch_df.toLocalIterator():
                pk = f"{row['sensor_id']}#{row['parameter_name']}"
                sk = str(row['window_end'])  # ISO-8601 string is fine

                item = {
                    "pk": pk,
                    "sk": sk,
                    "sensor_id": int(row['sensor_id']),
                    "parameter_name": row['parameter_name'],
                    "units": row['units'] or "",
                    "location_id": int(row['location_id']) if row['location_id'] is not None else None,
                    "window_start_utc": str(row['window_start']),
                    "window_end_utc": str(row['window_end']),
                    # >>> use Decimal, not float
                    "avg_value": to_decimal(row['avg_value']),
                    # int is fine; DynamoDB will accept it (internally converted to Decimal)
                    "count": int(row['count']) if row['count'] is not None else 0,
                }
                # DynamoDB does not store None values
                item = {k: v for k, v in item.items() if v is not None}
                bw.put_item(Item=item)


    agg_to_ddb = (
        agg
        .writeStream
        .foreachBatch(write_to_ddb)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/agg_ddb")
        .outputMode("update")
        .start()
    )

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
