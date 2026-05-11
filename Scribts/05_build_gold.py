# task 5 (create dimintions and facts then load it to s3 (gold layer))
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.700 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ------------------------------------------------------
# 1. إنشاء Spark Session
spark = SparkSession.builder \
    .appName("Gold Layer - NYC Flights Analysis") \
    .master("local[2]") \
    .config("spark.hadoop.fs.s3a.access.key", "@$@$$@$") \
    .config("spark.hadoop.fs.s3a.secret.key", "@$@$$@$") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.path.style.access", "false") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------
# 2. تحميل البيانات من Silver (S3)
flights_df  = spark.read.parquet("s3a://nyc-flights-silver/flights/")
weather_df  = spark.read.parquet("s3a://nyc-flights-silver/weather/")
airports_df = spark.read.parquet("s3a://nyc-flights-silver/airports/")
print(" تم تحميل البيانات من Silver بنجاح")

# --------------------------------------------
# 3. بناء DIM_DATE
# بنسيب الـ key كـ BIGINT من غير cast لـ INT عشان نتجنب الـ overflow
dim_date = flights_df \
    .select(F.col("FL_DATE").alias("full_date")) \
    .distinct() \
    .withColumn("date_key", F.monotonically_increasing_id()) \
    .withColumn("day", F.dayofmonth("full_date")) \
    .withColumn("day_name", F.date_format("full_date", "EEEE")) \
    .withColumn("week_of_year", F.weekofyear("full_date")) \
    .withColumn("month", F.month("full_date")) \
    .withColumn("month_name", F.date_format("full_date", "MMMM")) \
    .withColumn("quarter", F.quarter("full_date")) \
    .withColumn("year", F.year("full_date")) \
    .withColumn("is_weekend", F.when(F.dayofweek("full_date").isin([1, 7]), 1).otherwise(0)) \
    .withColumn("season", F.when(F.month("full_date").isin([12, 1, 2]), "Winter")
                           .when(F.month("full_date").isin([3, 4, 5]), "Spring")
                           .when(F.month("full_date").isin([6, 7, 8]), "Summer")
                           .otherwise("Fall")) \
    .withColumn("holiday_flag", F.lit(0))

# ----------------------------------------------------
# 4. بناء DIM_TIME
dim_time = flights_df \
    .select(F.col("CRS_DEP_TIME").alias("time_label")) \
    .distinct() \
    .withColumn("time_key", F.monotonically_increasing_id()) \
    .withColumn("hour", F.col("time_label").substr(1, 2).cast("integer")) \
    .withColumn("minute", F.col("time_label").substr(4, 2).cast("integer")) \
    .withColumn("part_of_day", F.when(F.col("hour").between(6, 11), "Morning")
                                 .when(F.col("hour").between(12, 17), "Afternoon")
                                 .when(F.col("hour").between(18, 21), "Evening")
                                 .otherwise("Night")) \
    .withColumn("time_slot_1h", F.concat(F.col("hour").cast("string"), F.lit(":00"))) \
    .withColumn("time_slot_3h", F.concat((F.col("hour") - (F.col("hour") % 3)).cast("string"), F.lit(":00"))) \
    .withColumn("peak_hour_flag", F.when(
        F.col("hour").between(7, 9) | F.col("hour").between(17, 19), 1).otherwise(0))

# ------------------------------------------------
# 5. بناء DIM_AIRPORT
dim_airport = airports_df \
    .withColumn("airport_key", F.monotonically_increasing_id()) \
    .withColumnRenamed("name", "airport_name") \
    .withColumnRenamed("municipality", "city") \
    .withColumnRenamed("iso_country", "country") \
    .withColumnRenamed("iso_region", "state") \
    .withColumnRenamed("type", "airport_type") \
    .select(
        "airport_key", "iata_code", "airport_name", "city",
        "state", "country", "latitude_deg", "longitude_deg",
        "elevation_ft", "airport_type"
    )

# -------------------------------------------------
# 6. بناء DIM_CARRIER
dim_carrier = flights_df \
    .select(F.col("OP_UNIQUE_CARRIER").alias("carrier_code")) \
    .distinct() \
    .withColumn("carrier_key", F.monotonically_increasing_id()) \
    .withColumn("carrier_name", F.lit(None).cast("string"))

# ---------------------------------------------------------
# 7. بناء DIM_WEATHER_CONDITION
dim_weather_condition = weather_df \
    .select("conditions", "preciptype", "severerisk") \
    .distinct() \
    .withColumn("weather_condition_key", F.monotonically_increasing_id()) \
    .withColumnRenamed("conditions", "weather_main") \
    .withColumn("weather_description", F.col("weather_main")) \
    .withColumnRenamed("preciptype", "precipitation_type") \
    .withColumnRenamed("severerisk", "severity_level")

# -------------------------------------------
# 8. بناء FACT_FLIGHT_DELAY
date_dim    = dim_date.select("date_key", "full_date")
time_dim    = dim_time.select("time_key", "time_label")
origin_dim  = dim_airport.select(
    F.col("airport_key").alias("origin_airport_key"),
    F.col("iata_code").alias("origin_iata")
)
dest_dim    = dim_airport.select(
    F.col("airport_key").alias("dest_airport_key"),
    F.col("iata_code").alias("dest_iata")
)
carrier_dim = dim_carrier.select("carrier_key", "carrier_code")

fact_flight_delay = flights_df \
    .join(date_dim,    flights_df["FL_DATE"]          == date_dim["full_date"],       "left") \
    .join(time_dim,    flights_df["CRS_DEP_TIME"]      == time_dim["time_label"],      "left") \
    .join(origin_dim,  flights_df["ORIGIN"]            == origin_dim["origin_iata"],   "left") \
    .join(dest_dim,    flights_df["DEST"]              == dest_dim["dest_iata"],       "left") \
    .join(carrier_dim, flights_df["OP_UNIQUE_CARRIER"] == carrier_dim["carrier_code"], "left") \
    .withColumn("flight_delay_key", F.monotonically_increasing_id()) \
    .withColumn("flight_number", F.col("OP_CARRIER_FL_NUM").cast("string")) \
    .withColumn("delay_bucket",
                F.when(F.col("ARR_DELAY") <= 0, "No Delay")
                 .when(F.col("ARR_DELAY").between(1, 15),   "Minor (1-15 min)")
                 .when(F.col("ARR_DELAY").between(16, 45),  "Moderate (16-45 min)")
                 .when(F.col("ARR_DELAY").between(46, 120), "Severe (46-120 min)")
                 .otherwise("Critical (>120 min)")) \
    .withColumn("is_weather_related_flag", F.when(F.col("WEATHER_DELAY") > 0, 1).otherwise(0)) \
    .select(
        "flight_delay_key", "date_key", "time_key",
        "origin_airport_key", "dest_airport_key", "carrier_key",
        "flight_number", F.col("FL_DATE").alias("fl_date"),
        F.col("DEP_DELAY").alias("dep_delay_minutes"),
        F.col("ARR_DELAY").alias("arr_delay_minutes"),
        F.col("CARRIER_DELAY").alias("carrier_delay_minutes"),
        F.col("WEATHER_DELAY").alias("weather_delay_minutes"),
        F.col("NAS_DELAY").alias("nas_delay_minutes"),
        F.col("SECURITY_DELAY").alias("security_delay_minutes"),
        F.col("LATE_AIRCRAFT_DELAY").alias("late_aircraft_delay_minutes"),
        F.col("CRS_ELAPSED_TIME").alias("scheduled_elapsed_time"),
        F.col("ACTUAL_ELAPSED_TIME").alias("actual_elapsed_time"),
        F.col("AIR_TIME").alias("air_time"),
        F.col("DISTANCE").alias("distance"),
        F.col("CANCELLED").alias("cancelled_flag"),
        F.col("DIVERTED").alias("diverted_flag"),
        F.col("IS_DEP_DELAYED").alias("is_dep_delayed_flag"),
        F.col("IS_ARR_DELAYED").alias("is_arr_delayed_flag"),
        "is_weather_related_flag", "delay_bucket"
    )

# ---------------------------------------------------------------
# 9. بناء FACT_WEATHER_OBSERVATION
weather_date_dim = dim_date.select("date_key", "full_date")
weather_time_dim = dim_time.select("time_key", "time_label")
weather_cond_dim = dim_weather_condition.select("weather_condition_key", "weather_main")

fact_weather_observation = weather_df \
    .join(weather_date_dim, weather_df["date"]      == weather_date_dim["full_date"],     "left") \
    .join(weather_time_dim, weather_df["hour"]       == F.col("time_label").substr(1, 2).cast("integer"), "left") \
    .join(weather_cond_dim, weather_df["conditions"] == weather_cond_dim["weather_main"], "left") \
    .withColumn("weather_observation_key", F.monotonically_increasing_id()) \
    .withColumn("airport_key", F.lit(None).cast("long")) \
    .withColumn("weather_severity_score",
                F.when(F.col("severerisk") == "None",     0)
                 .when(F.col("severerisk") == "Low",      1)
                 .when(F.col("severerisk") == "Moderate", 2)
                 .when(F.col("severerisk") == "High",     3)
                 .otherwise(0)) \
    .select(
        "weather_observation_key", "date_key", "time_key",
        "airport_key", "weather_condition_key",
        F.col("temp").alias("temperature"), "humidity",
        F.col("precip").alias("precipitation"), "snow",
        F.col("windspeed").alias("wind_speed"),
        F.col("winddir").alias("wind_direction"),
        "visibility", F.col("sealevelpressure").alias("pressure"),
        "cloudcover", "weather_severity_score"
    )

# ------------------------------------------------
# 10. حفظ الـ Gold Layer على S3
dim_date.write.mode("overwrite").parquet("s3a://nyc-flights-gold/dim_date/")
dim_time.write.mode("overwrite").parquet("s3a://nyc-flights-gold/dim_time/")
dim_airport.write.mode("overwrite").parquet("s3a://nyc-flights-gold/dim_airport/")
dim_carrier.write.mode("overwrite").parquet("s3a://nyc-flights-gold/dim_carrier/")
dim_weather_condition.write.mode("overwrite").parquet("s3a://nyc-flights-gold/dim_weather_condition/")
fact_flight_delay.write.mode("overwrite").parquet("s3a://nyc-flights-gold/fact_flight_delay/")
fact_weather_observation.write.mode("overwrite").parquet("s3a://nyc-flights-gold/fact_weather_observation/")

print(" Gold Layer تم حفظه بنجاح على S3")