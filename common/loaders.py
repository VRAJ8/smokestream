"""Spark loaders for historical PurpleAir downloads (used by the batch job and the sweep)."""

from pyspark.sql import functions as F

from common.history import SENSOR_COLUMNS, resolve_columns


def load_history(spark, history_path, sensors_path):
    raw = spark.read.option("header", True).csv(history_path)
    cols = resolve_columns(raw.columns)
    readings = raw.select(
        F.col(f"`{cols['sensor_index']}`").cast("long").alias("sensor_index"),
        F.col(f"`{cols['event_time']}`").cast("double").alias("event_time"),
        F.col(f"`{cols['pm25_a']}`").cast("double").alias("pm25_a"),
        F.col(f"`{cols['pm25_b']}`").cast("double").alias("pm25_b"),
        F.col(f"`{cols['pm1']}`").cast("double").alias("pm1"),
        F.col(f"`{cols['rh']}`").cast("double").alias("rh"),
    ).withColumn("arrival_time", F.col("event_time"))
    sensors = spark.read.option("header", True).csv(sensors_path).select(
        F.col(SENSOR_COLUMNS["sensor_index"]).cast("long").alias("sensor_index"),
        F.col(SENSOR_COLUMNS["lat"]).cast("double").alias("lat"),
        F.col(SENSOR_COLUMNS["lon"]).cast("double").alias("lon"),
    )
    return readings.join(F.broadcast(sensors), "sensor_index")
