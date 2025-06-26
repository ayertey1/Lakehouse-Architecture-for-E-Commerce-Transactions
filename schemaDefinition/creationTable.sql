-- Product Data
product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("department_id", StringType(), True),
    StructField("department", StringType(), True),
    StructField("product_name", StringType(), True)
])

--Orders
orders_schema = StructType([
    StructField("order_num", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("order_timestamp", TimestampType(), False),
    StructField("total_amount", DoubleType(), True),
    StructField("date", DateType(), False)
])
