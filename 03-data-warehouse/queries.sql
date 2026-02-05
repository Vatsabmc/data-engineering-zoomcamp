-- Create an external table using the Yellow Taxi Trip Records
CREATE OR REPLACE EXTERNAL TABLE `steel-shine.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_2026/yellow_tripdata_2024-*.parquet']
);

-- Count of records for the 2024 Yellow Taxi Data External
SELECT count(*) FROM `steel-shine.nytaxi.external_yellow_tripdata`;

-- Create a materialized table using the Yellow Taxi Trip Records
CREATE OR REPLACE TABLE steel-shine.nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM steel-shine.nytaxi.external_yellow_tripdata;

-- Count the distinct number of PULocationIDs for the entire dataset on both the tables.
SELECT COUNT(DISTINCT(PULocationID)) FROM `steel-shine.nytaxi.external_yellow_tripdata`;
SELECT COUNT(DISTINCT(PULocationID)) FROM `steel-shine.nytaxi.yellow_tripdata_non_partitioned`;

-- Retrieve the PULocationID from the table
SELECT PULocationID FROM `steel-shine.nytaxi.yellow_tripdata_non_partitioned`;
-- Retrieve the PULocationID and DOLocationID from the table
SELECT PULocationID, DOLocationID FROM `steel-shine.nytaxi.yellow_tripdata_non_partitioned`;

-- Count records that have a fare_amount of 0?
SELECT count(*) FROM `steel-shine.nytaxi.external_yellow_tripdata` WHERE fare_amount=0;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE steel-shine.nytaxi.yellow_tripdata_partitioned
PARTITION BY
  DATE(tpep_dropoff_datetime) AS
SELECT * FROM steel-shine.nytaxi.external_yellow_tripdata;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE steel-shine.nytaxi.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM steel-shine.nytaxi.external_yellow_tripdata;

-- Retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
SELECT DISTINCT(VendorID) FROM  `steel-shine.nytaxi.yellow_tripdata_non_partitioned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT(VendorID) FROM  `steel-shine.nytaxi.yellow_tripdata_partitioned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT(VendorID) FROM  `steel-shine.nytaxi.yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Count of records for the 2024 Yellow Taxi Data Materialized
SELECT count(*) FROM `steel-shine.nytaxi.yellow_tripdata_non_partitioned`;