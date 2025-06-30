import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp,
    to_date,
    lit
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, DateType

# Glue boilerplate
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Input and output locations
RAW_INPUT_PATH = "s3://lakehouse-datastore/raw/orders/"
PROCESSED_OUTPUT_PATH = "s3://lakehouse-datastore/processed/orders/"
REJECTED_PATH = "s3://lakehouse-datastore/rejected/orders/"

# Define schema
orders_schema = StructType([
    StructField("order_num", StringType(), True),
    StructField("order_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("order_timestamp", StringType(), False),  # parse manually
    StructField("total_amount", DoubleType(), True),
    StructField("date", StringType(), False)              # parse manually
])

# Load CSV
df_raw = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(orders_schema) \
    .load(RAW_INPUT_PATH)

# Trim strings
df_trimmed = df_raw \
    .withColumn("order_id", trim(col("order_id"))) \
    .withColumn("user_id", trim(col("user_id")))

# Parse timestamps
df_parsed = df_trimmed \
    .withColumn("order_timestamp", to_timestamp("order_timestamp")) \
    .withColumn("date", to_date("date"))

# Validation rules
df_valid = df_parsed.filter(
    (col("order_id").isNotNull()) & (col("order_id") != "") &
    (col("user_id").isNotNull()) & (col("user_id") != "") &
    (col("order_timestamp").isNotNull()) &
    (col("date").isNotNull()) &
    ((col("total_amount").isNull()) | (col("total_amount") >= 0))
)

# Capture invalid rows
df_invalid = df_parsed.subtract(df_valid).withColumn(
    "rejection_reason", lit("Failed validation rules")
)

# Deduplicate
df_deduped = df_valid.dropDuplicates(["order_id"])

# Write rejected records
df_invalid.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(REJECTED_PATH)

# Write clean Delta Lake partitioned by date
df_deduped.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("date") \
    .option("overwriteSchema", "true") \
    .save(PROCESSED_OUTPUT_PATH)

job.commit()
