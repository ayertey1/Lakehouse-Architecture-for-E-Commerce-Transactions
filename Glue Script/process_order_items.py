"""
process_order_items.py
Glue ETL job to process Order Items into Delta Lake
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp,
    to_date,
    lit
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType

# Initialize SparkSession with Delta
spark = SparkSession.builder \
    .appName("ProcessOrderItemsETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Input and output locations
RAW_INPUT_PATH = "s3://lakehouse-datastore/raw/order_items/"
PROCESSED_OUTPUT_PATH = "s3://lakehouse-datastore/processed/order_items/"
REJECTED_PATH = "s3://lakehouse-datastore/rejected/order_items/"

# Define schema
order_items_schema = StructType([
    StructField("id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("days_since_prior_order", IntegerType(), True),
    StructField("product_id", StringType(), False),
    StructField("add_to_cart_order", IntegerType(), True),
    StructField("reordered", IntegerType(), True),
    StructField("order_timestamp", StringType(), False),  # parse manually
    StructField("date", StringType(), False)              # parse manually
])

# Load CSV
df_raw = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(order_items_schema) \
    .load(RAW_INPUT_PATH)

# Trim strings
df_trimmed = df_raw \
    .withColumn("id", trim(col("id"))) \
    .withColumn("order_id", trim(col("order_id"))) \
    .withColumn("user_id", trim(col("user_id"))) \
    .withColumn("product_id", trim(col("product_id")))

# Parse timestamps and dates
df_parsed = df_trimmed \
    .withColumn("order_timestamp", to_timestamp("order_timestamp")) \
    .withColumn("date", to_date("date"))

# Validation
df_valid = df_parsed.filter(
    (col("id").isNotNull()) &
    (col("id") != "") &
    (col("order_id").isNotNull()) &
    (col("order_id") != "") &
    (col("user_id").isNotNull()) &
    (col("user_id") != "") &
    (col("product_id").isNotNull()) &
    (col("product_id") != "") &
    (col("order_timestamp").isNotNull()) &
    (col("date").isNotNull())
)

# Rejected records
df_invalid = df_parsed.subtract(df_valid).withColumn(
    "rejection_reason", lit("Failed validation rules")
)

# Deduplicate
df_deduped = df_valid.dropDuplicates(["id"])

# Write rejected records
df_invalid.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(REJECTED_PATH)

# Write Delta Lake partitioned by date
df_deduped.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("date") \
    .option("overwriteSchema", "true") \
    .save(PROCESSED_OUTPUT_PATH)

spark.stop()
