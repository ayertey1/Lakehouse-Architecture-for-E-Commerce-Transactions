"""
process_products.py
Glue ETL job to process product data into Delta Lake
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize SparkSession with Delta support
spark = SparkSession.builder \
    .appName("ProcessProductsETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Input and output locations 
# RAW_INPUT_PATH = "s3://lakehouse-datastore/raw/products/"
# PROCESSED_OUTPUT_PATH = "s3://lakehouse-datastore/processed/products/"
# REJECTED_PATH = "s3://lakehouse-datastore/rejected/products/"
RAW_INPUT_PATH = "data/products.csv"
PROCESSED_OUTPUT_PATH = "data/processed/products/"
REJECTED_PATH = "data/rejected/products/"
# Define schema reflecting your data sample
product_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("department_id", IntegerType(), True),
    StructField("department", StringType(), True),
    StructField("product_name", StringType(), True)
])

# Load CSV with schema enforcement
df_raw = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(product_schema) \
    .load(RAW_INPUT_PATH)

# Trim whitespace from string fields
df_trimmed = df_raw \
    .withColumn("department", trim(col("department"))) \
    .withColumn("product_name", trim(col("product_name")))

# Apply validation rules:
# 1. product_id not null and >0
# 2. department_id not null and >0
# 3. department not null or empty
df_valid = df_trimmed.filter(
    (col("product_id").isNotNull()) &
    (col("product_id") > 0) &
    (col("department_id").isNotNull()) &
    (col("department_id") > 0) &
    (col("department").isNotNull()) &
    (col("department") != "")
)

# Capture invalid records for logging
df_invalid = df_trimmed.subtract(df_valid) \
    .withColumn("rejection_reason", lit("Failed validation rules"))

# Deduplicate
df_deduped = df_valid.dropDuplicates(["product_id"])

# Write rejected records to rejected path in Parquet
df_invalid.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(REJECTED_PATH)

# Write clean deduplicated data to Delta Lake partitioned by department_id
df_deduped.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("department_id") \
    .option("overwriteSchema", "true") \
    .save(PROCESSED_OUTPUT_PATH)

spark.stop()
