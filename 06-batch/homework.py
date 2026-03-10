from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Question 1: Install Spark and PySpark
# Create a local Spark session and print the version.
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('homework') \
    .getOrCreate()

print(f"Spark version: {spark.version}")

# ── Question 2: Yellow November 2025
# Read the November 2025 Yellow Taxi parquet file, repartition to 4 partitions,
# and write to parquet. The average size of the output files answers Q2.
year = 2025
month = 11

input_path = f'data/raw/yellow/{year}/yellow_tripdata_{year}-{month:02d}.parquet'
output_path = f'data/pq/yellow/{year}/'

df_yellow = spark.read.parquet(input_path)

df_yellow \
    .repartition(4) \
    .write.mode('overwrite').parquet(output_path)

# ── Question 3: Count records 
# How many taxi trips started on November 15, 2025?
df_yellow = spark.read.parquet('data/pq/yellow/*/*') \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime') \
    .withColumn('service_type', F.lit('yellow'))

df_yellow.createOrReplaceTempView('yellow_trips_data')

spark.sql("""
SELECT
    COUNT(1) AS num_trips
FROM yellow_trips_data
WHERE pickup_datetime >= '2025-11-15'
  AND pickup_datetime <  '2025-11-16'
""").show()

# ── Question 4: Longest trip 
# What is the duration of the longest trip in the dataset (in hours)?
spark.sql("""
SELECT
    ROUND((unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 3600, 2) AS duration_hours
FROM yellow_trips_data
ORDER BY duration_hours DESC
LIMIT 5
""").show()

# ── Question 5: User Interface
# Spark's UI (application dashboard) is available at http://localhost:4040
# while the Spark session is running.
print("Spark UI available at: http://localhost:4040")

# ── Question 6: Least frequent pickup location zone
# Load the zone lookup CSV into a temp view, then join with the yellow trips
# data to find the pickup zone with the fewest trips.
df_zones = spark.read \
    .option("header", "true") \
    .csv('data/raw/taxi_zone_lookup.csv')
df_zones.write.mode('overwrite').parquet('data/pq/zones')

df_zones = spark.read.parquet('data/pq/zones')
df_zones.createOrReplaceTempView('zones')

spark.sql("""
SELECT
    pul.Zone  AS pickup_zone,
    COUNT(1)  AS num_trips
FROM yellow_trips_data  trips
LEFT JOIN zones pul ON trips.PULocationID = pul.LocationID
GROUP BY pul.Zone
ORDER BY num_trips ASC
LIMIT 10
""").show(truncate=False)

spark.stop()
