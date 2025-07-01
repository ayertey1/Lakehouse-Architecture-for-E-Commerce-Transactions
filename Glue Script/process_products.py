import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

RAW_INPUT_PATH = "s3://lakehouse-datastore/raw/products/"
PROCESSED_OUTPUT_PATH = "s3://lakehouse-datastore/processed/products/"
REJECTED_PATH = "s3://lakehouse-datastore/rejected/products/"

product_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("department_id", IntegerType(), True),
    StructField("department", StringType(), True),
    StructField("product_name", StringType(), True)
])

df_raw = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(product_schema) \
    .load(RAW_INPUT_PATH)

df_trimmed = df_raw \
    .withColumn("department", trim(col("department"))) \
    .withColumn("product_name", trim(col("product_name")))

df_valid = df_trimmed.filter(
    (col("product_id").isNotNull()) &
    (col("product_id") > 0) &
    (col("department_id").isNotNull()) &
    (col("department_id") > 0) &
    (col("department").isNotNull()) &
    (col("department") != "")
)

df_invalid = df_trimmed.subtract(df_valid) \
    .withColumn("rejection_reason", lit("Failed validation rules"))

df_deduped = df_valid.dropDuplicates(["product_id"]) \
    .withColumn("ingestion_time", current_timestamp()) \
    .filter(col("department_id").isNotNull())

df_invalid.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(REJECTED_PATH)

df_deduped.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("department_id") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .save(PROCESSED_OUTPUT_PATH)

job.commit()
