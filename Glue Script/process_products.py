"""
process_products.py
Glue ETL job to process product data into Delta Lake
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType

# Create SparkSession with Delta Lake
spark = SparkSession.builder \
    .appName("ProcessProductsETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Input and output locations (replace with your buckets)
RAW_INPUT_PATH = "s3://your-bucket/raw/products/"
PROCESSED_OUTPUT_PATH = "s3://your-bucket/processed/products/"
REJECTED_PATH = "s3://your-bucket/rejected/products/"

# Define schema
product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("department_id", StringType(), True),
    StructField("department", StringType(), True),
    StructField("product_name", StringType(), True)
])

# Load CSV with schema enforcement
df_raw = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(product_schema) \
    .load(RAW_INPUT_PATH)

# Trim whitespace
df_trimmed = df_raw.select([trim(col(c)).alias(c) for c in df_raw.columns])

# Validate rows: product_id must not be null or empty
df_valid = df_trimmed.filter(col("product_id").isNotNull() & (col("product_id") != ""))

df_invalid = df_trimmed.subtract(df_valid).withColumn("rejection_reason", lit("Invalid or missing product_id"))

# Deduplicate
df_deduped = df_valid.dropDuplicates(["product_id"])

# Write rejected records to rejected path
df_invalid.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(REJECTED_PATH)

# Write clean data to Delta Lake
df_deduped.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("department_id") \
    .option("overwriteSchema", "true") \
    .save(PROCESSED_OUTPUT_PATH)

spark.stop()
