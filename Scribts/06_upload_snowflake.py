# task 6 (load data to snowflake)
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.700,net.snowflake:snowflake-jdbc:3.13.33,net.snowflake:spark-snowflake_2.13:2.14.0-spark_3.4 pyspark-shell'

from pyspark.sql import SparkSession

# -----------------------------------------------
# 1. إنشاء Spark Session
spark = SparkSession.builder \
    .appName("Gold to Snowflake") \
    .master("local[2]") \
    .config("spark.hadoop.fs.s3a.access.key", "@$@$$@$") \
    .config("spark.hadoop.fs.s3a.secret.key", "@$@$$@$") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.path.style.access", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------
# 2. Snowflake Connection Options
snowflake_options = {
    "sfURL":       "vk55508.eu-central-2.aws.snowflakecomputing.com",
    "sfUser":      "@$@$$@$",
    "sfPassword":  "@$@$$@$",
    "sfDatabase":  "NYC_FLIGHTS_DW",
    "sfSchema":    "PUBLIC",
    "sfWarehouse": "COMPUTE_WH" ,

}

# ----------------------------------------
# 3. تحميل البيانات من Gold (S3)
dim_date              = spark.read.parquet("s3a://nyc-flights-gold/dim_date/")
dim_time              = spark.read.parquet("s3a://nyc-flights-gold/dim_time/")
dim_airport           = spark.read.parquet("s3a://nyc-flights-gold/dim_airport/")
dim_carrier           = spark.read.parquet("s3a://nyc-flights-gold/dim_carrier/")
dim_weather_condition = spark.read.parquet("s3a://nyc-flights-gold/dim_weather_condition/")
fact_flight_delay     = spark.read.parquet("s3a://nyc-flights-gold/fact_flight_delay/")
fact_weather_obs      = spark.read.parquet("s3a://nyc-flights-gold/fact_weather_observation/")

print(" تم تحميل البيانات من Gold S3")

# ------------------------------------
# 4. helper function للرفع على Snowflake
def write_to_snowflake(df, table_name):
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()
    print(f" تم رفع {table_name} على Snowflake")

# ---------------------------------------------------------
# 5. رفع الـ Dimensions أولاً عشان الـ FK يشتغل
write_to_snowflake(dim_date,              "DIM_DATE")
write_to_snowflake(dim_time,              "DIM_TIME")
write_to_snowflake(dim_airport,           "DIM_AIRPORT")
write_to_snowflake(dim_carrier,           "DIM_CARRIER")
write_to_snowflake(dim_weather_condition, "DIM_WEATHER_CONDITION")

# ----------------------------------------------
# 6. رفع الـ Facts بعد الـ Dimensions
write_to_snowflake(fact_flight_delay, "FACT_FLIGHT_DELAY")
write_to_snowflake(fact_weather_obs,  "FACT_WEATHER_OBSERVATION")

print(" تم رفع كل البيانات على Snowflake بنجاح")