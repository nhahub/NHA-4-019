#task 4 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -----------------------------------------------------------
#إنشاء Spark Session
spark = SparkSession.builder \
    .appName("airports Data Warehouse - Silver") \
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


#---------------------------------------------------------------------------------------
# لود أول داتا ست
airports_df = spark.read.csv("/home/ahmed-refat/Desktop/flights & airports/Raw Data(batch)/World_Airports.csv", header=True, inferSchema=True)

airports_clean = airports_df \
    .select(
        "iata_code", "name", "type", "municipality",
        "iso_country", "iso_region", "latitude_deg",
        "longitude_deg", "elevation_ft", "scheduled_service"
    ) \
    .filter(F.col("iata_code").isNotNull()) \
    .dropDuplicates()

# -----------------------------------------------
# . حفظ البيانات على S3 (Silver)

airports_clean.write \
    .mode("overwrite") \
    .parquet("s3a://nyc-flights-silver/airports/")

print(" تم حفظ airports على S3 Silver بنجاح")