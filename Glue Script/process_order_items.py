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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Glue job boilerplate
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

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

# Load CSV with schema
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
    (col("id").isNotNull()) & (col("id") != "") &
    (col("order_id").isNotNull()) & (col("order_id") != "") &
    (col("user_id").isNotNull()) & (col("user_id") != "") &
    (col("product_id").isNotNull()) & (col("product_id") != "") &
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

# Write clean Delta table partitioned by date
df_deduped.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("date") \
    .option("overwriteSchema", "true") \
    .save(PROCESSED_OUTPUT_PATH)

job.commit()