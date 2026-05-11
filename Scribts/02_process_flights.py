#task 2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -----------------------------------------------------------
# 1. إنشاء Spark Session
spark = SparkSession.builder \
    .appName("Flight Data Warehouse - Silver") \
    .master("local[2]") \
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.hadoop:hadoop-aws:3.4.2",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",  
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1",
            "org.apache.iceberg:iceberg-aws-bundle:1.10.1"
        ])
    ) \
    .config("spark.hadoop.fs.s3a.access.key", "@$@$$@$") \
    .config("spark.hadoop.fs.s3a.secret.key", "@$@$$@$") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.path.style.access", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(" Spark Started:", spark.version)

# ----------------------------------------------------
# 2. تحميل البيانات الخام (Bronze) من Local
flights_df = spark.read.csv(
    "/home/ahmed-refat/Desktop/NYC_delays.csv",
    header=True,
    inferSchema=True
)

# ------------------------------------------------
# 3. تنظيف البيانات (Silver)

flights_clean = flights_df \
    .withColumn("FL_DATE", F.to_date(F.col("FL_DATE"), "M/d/yyyy hh:mm:ss a")) \
    .drop(
        "ORIGIN_AIRPORT_SEQ_ID", "ORIGIN_CITY_MARKET_ID",
        "DEST_AIRPORT_SEQ_ID", "DEST_CITY_MARKET_ID",
        "ARR_DELAY_GROUP", "TOTAL_ADD_GTIME", "LONGEST_ADD_GTIME",
        "DISTANCE_GROUP", "ARR_TIME_BLK", "DEP_DELAY_NEW",
        "ARR_DELAY_NEW", "FLIGHTS"
    )

# تحويل أنواع البيانات
flights_clean = flights_clean \
    .withColumn("CANCELLED", F.col("CANCELLED").cast("integer")) \
    .withColumn("DIVERTED", F.col("DIVERTED").cast("integer"))

# ملء القيم الفارغة
flights_clean = flights_clean \
    .fillna(0, subset=[
        "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
        "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY",
        "DEP_DELAY", "ARR_DELAY"
    ])

# أعمدة جديدة
flights_clean = flights_clean \
    .withColumn("IS_DEP_DELAYED", F.when(F.col("DEP_DELAY") > 0, 1).otherwise(0)) \
    .withColumn("IS_ARR_DELAYED", F.when(F.col("ARR_DELAY") > 0, 1).otherwise(0))

# حذف التكرار
flights_clean = flights_clean.dropDuplicates()

# تعديل الوقت
flights_clean = flights_clean \
    .withColumn("CRS_DEP_TIME", F.lpad(F.col("CRS_DEP_TIME").cast("string"), 4, "0")) \
    .withColumn("CRS_DEP_TIME", F.to_timestamp(F.col("CRS_DEP_TIME"), "HHmm")) \
    .withColumn("CRS_DEP_TIME", F.col("CRS_DEP_TIME").cast("string").substr(12, 5)) \
    .withColumn("CRS_ARR_TIME", F.lpad(F.col("CRS_ARR_TIME").cast("string"), 4, "0")) \
    .withColumn("CRS_ARR_TIME", F.to_timestamp(F.col("CRS_ARR_TIME"), "HHmm")) \
    .withColumn("CRS_ARR_TIME", F.col("CRS_ARR_TIME").cast("string").substr(12, 5))

# ------------------------------------------
# 4. عرض النتيجة
flights_clean.printSchema()
flights_clean.show(3)

# -----------------------------------------------
# 5. حفظ البيانات على S3 (Silver)

flights_clean.write \
    .mode("overwrite") \
    .parquet("s3a://nyc-flights-silver/flights/")

print(" تم حفظ flights على S3 Silver بنجاح")