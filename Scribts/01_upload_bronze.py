from pyspark.sql import SparkSession
import boto3

# ------------------------------------
# 1. إنشاء Spark Session
spark = (
    SparkSession.builder
    .appName("Bronze Layer - Upload Check")
    .master("local[2]")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0",
            "org.apache.hadoop:hadoop-aws:3.4.2",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1",
            "org.apache.iceberg:iceberg-aws-bundle:1.10.1"
        ])
    )
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.access.key", "@@$@$$@$")
    .config("spark.hadoop.fs.s3a.secret.key", "@$@$$@$")
    .config("spark.hadoop.fs.s3a.path.style.access", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

# ----------------------------------------------------------
# 2. تعريف الـ files وأماكنها
AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
BRONZE_BUCKET  = "nyc-flights-bronze"
REGION         = "us-east-1" 



# الملفات المحلية واسم كل واحدة على اس3 
files = [
    {"local_path": "/home/ahmed-refat/Desktop/NYC_delays.csv",  "s3_key": "NYC_delays.csv"},
    {"local_path": "/home/ahmed-refat/Desktop/flights & airports/Raw Data(batch)/weather.csv",     "s3_key": "weather.csv"},
    {"local_path": "/home/ahmed-refat/Desktop/flights & airports/Raw Data(batch)/World_Airports.csv",    "s3_key": "airports.csv"},
]

# ----------------------------------------------
# 3. التحقق من وجود الملف على اس 3 ورفعه لو مش موجود

# الكونكشن بتاع اس 3 
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION
)

def file_exists_on_s3(bucket, key):
    """بتتحقق لو الملف موجود على اس 3 ولا لأ"""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False

def upload_if_not_exists(local_path, bucket, key):
    """بترفع الملف بس لو مش موجود على اس3"""
    if file_exists_on_s3(bucket, key):
        print(f" موجود بالفعل على S3: {key} - تم التخطي")
    else:
        print(f" جاري الرفع: {key}")
        s3_client.upload_file(local_path, bucket, key)
        print(f"تم الرفع بنجاح: {key}")

# ---------------------------------------------------
# 4. تنفيذ الرفع على الـ 3 ملفات
for file in files:
    upload_if_not_exists(file["local_path"], BRONZE_BUCKET, file["s3_key"])

print("\n Bronze Layer جاهز على S3")