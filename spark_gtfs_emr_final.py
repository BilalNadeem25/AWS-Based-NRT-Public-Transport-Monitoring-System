from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max, min, countDistinct,
    from_unixtime, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
import sys

spark = SparkSession.builder.appName("GTFS-EMR-Final").getOrCreate()

# -----------------------------
# READ FILE FROM ARGUMENT
# -----------------------------
if len(sys.argv) < 2:
    raise Exception("Input JSON file not provided")

input_file = sys.argv[1]
print("Reading GTFS file:", input_file)

# -----------------------------
# Read JSON 
# -----------------------------
df = spark.read.option("multiline", "true").json(input_file)

# -----------------------------
# Cast timestamp safely
# -----------------------------
df = df.withColumn(
    "event_time",
    from_unixtime(col("timestamp").cast("long"))
).withColumn(
    "event_time",
    col("event_time").cast(TimestampType())
)

# -----------------------------
# 1. Latest vehicle snapshot
# -----------------------------
vehicle_window = Window.partitionBy("vehicle.id").orderBy(col("event_time").desc())

vehicle_latest = (
    df.withColumn("rn", row_number().over(vehicle_window))
      .filter(col("rn") == 1)
      .select(
        col("trip.route_id").alias("route_id"),
        col("trip.trip_id").alias("trip_id"),
        col("trip.start_time").alias("trip_start_time"),
        col("trip.start_date").alias("trip_start_date"),
        col("vehicle.id").alias("vehicle_id"),
        col("vehicle.license_plate").alias("license_plate"),
        col("position.latitude").alias("latitude"),
        col("position.longitude").alias("longitude"),
        col("position.speed").alias("speed"),
        col("position.bearing").alias("bearing"),
        col("event_time").alias("last_update")   
      )

)

# -----------------------------
# 2. Route-level metrics
# -----------------------------
route_metrics = (
    df.groupBy(col("trip.route_id").alias("route_id"))
      .agg(
          countDistinct("vehicle.id").alias("vehicle_count"),
          countDistinct("trip.trip_id").alias("trip_count"),
          avg("position.speed").alias("avg_speed"),
          max("position.speed").alias("max_speed"),
          min("position.speed").alias("min_speed"),
          max("event_time").alias("last_update")
      )
)

# -----------------------------
# 3. Trip-level metrics
# -----------------------------
trip_metrics = (
    df.groupBy(
        col("trip.trip_id").alias("trip_id"),
        col("trip.route_id").alias("route_id"),
        col("trip.start_time").alias("start_time"),
        col("trip.start_date").alias("start_date")
    )
    .agg(
        countDistinct("vehicle.id").alias("vehicle_count"),
        avg("position.speed").alias("avg_speed"),
        max("position.speed").alias("max_speed"),
        min("position.speed").alias("min_speed"),
        countDistinct("event_time").alias("gps_points"),
        max("event_time").alias("last_update")
    )
)

# ===============================
# Write outputs directly to S3
# ===============================
BUCKET = "gtfs-s3"
ARCHIVE_PREFIX = "archive/"
OUTPUT_PREFIX = "output/"

def write_outputs(df, name):
    parquet_path = f"s3://{BUCKET}/{OUTPUT_PREFIX}parquet/{name}"
    csv_path = f"s3://{BUCKET}/{OUTPUT_PREFIX}csv/{name}"

    df.write.mode("overwrite").parquet(parquet_path)

    df.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(csv_path)

write_outputs(vehicle_latest, "vehicle_latest")
write_outputs(route_metrics, "route_metrics")
write_outputs(trip_metrics, "trip_metrics")

print("Outputs written to S3 successfully")

spark.stop()