#task 3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -----------------------------------------------------------
#إنشاء Spark Session
spark = SparkSession.builder \
    .appName("weather Data Warehouse - Silver") \
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

#------------------------------------------------------
# لود  الداتا 
weather_df = spark.read.csv("/opt/airflow/data/weather.csv", header=True, inferSchema=True)


#----------------------------------------------
#data clean 
weather_clean = weather_df \
    .drop("feelslike", "dew", "solarradiation", "solarenergy", 
          "uvindex", "icon", "stations", "name") \
    .fillna(0.0, subset=["precip", "snow", "snowdepth", "windgust", "precipprob"]) \
    .fillna("None", subset=["preciptype", "severerisk"]) \
    .withColumn("datetime", F.col("datetime").cast("timestamp")) \
    .withColumn("date", F.to_date(F.col("datetime"))) \
    .withColumn("hour", F.hour(F.col("datetime"))) \
    .dropDuplicates()

weather_clean.show(3)

# -----------------------------------------------
# . حفظ البيانات على S3 (Silver)

weather_clean.write \
    .mode("overwrite") \
    .parquet("s3a://nyc-flights-silver/weather/")

print(" تم حفظ flights على S3 Silver بنجاح")