"""SparkSession setup shared by the jobs (local mode, one laptop)."""

import os
import sys
import time

from pyspark.sql import SparkSession

# Spark 4.x is built with Scala 2.13; the Kafka connector version must match Spark's.
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:{version}"


def session(app_name, kafka=False):
    # Make Spark's Python workers use this same interpreter (the project .venv).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    # PySpark converts timestamps to Python datetimes in the process's local time
    # zone. Pin it to UTC so collected times (e.g. those written to MongoDB) are UTC.
    os.environ["TZ"] = "UTC"
    time.tzset()
    builder = (SparkSession.builder.appName(app_name)
               .master(os.environ.get("SPARK_MASTER", "local[*]"))
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
               .config("spark.ui.showConsoleProgress", "false")
               # Local mode runs everything in the driver JVM, whose default 1 GB heap is
               # too small for the enrichment joins over a full season.
               .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g")))
    if kafka:
        import pyspark
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE.format(version=pyspark.__version__))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
