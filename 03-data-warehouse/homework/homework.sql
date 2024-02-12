-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `my-project-id.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://my-bucket-name/green_tripdata_2022.csv']
);

-- Check external green trip data
SELECT * FROM my-project-id.ny_taxi.external_green_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE my-project-id.ny_taxi.green_tripdata AS
SELECT * FROM my-project-id.ny_taxi.external_green_tripdata;

-- Check green trip data
SELECT * FROM my-project-id.ny_taxi.green_tripdata limit 10;


-- Question 2:
-- Distinct number of PULocationIDs
SELECT COUNT(DISTINCT(pulocation_id))
FROM my-project-id.ny_taxi.green_tripdata;

SELECT COUNT(DISTINCT(pulocation_id))
FROM my-project-id.ny_taxi.external_green_tripdata;


-- Question 3:
-- How many records have a fare_amount of 0?
SELECT COUNT(fare_amount)
FROM my-project-id.ny_taxi.green_tripdata
WHERE fare_amount=0;


-- Question 4:
-- Partition lpep_pickup_datetime column and clustering pulocation_id
CREATE OR REPLACE TABLE my-project-id.ny_taxi.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY pulocation_id AS
SELECT * FROM my-project-id.ny_taxi.external_green_tripdata;


-- Question 5:
SELECT DISTINCT pulocation_id
FROM my-project-id.ny_taxi.green_tripdata
WHERE lpep_pickup_datetime BETWEEN TIMESTAMP("2022-06-01") AND TIMESTAMP("2022-06-30");

SELECT DISTINCT pulocation_id
FROM my-project-id.ny_taxi.green_tripdata_partitoned_clustered
WHERE lpep_pickup_datetime BETWEEN TIMESTAMP("2022-06-01") AND TIMESTAMP("2022-06-30");


-- Question 8:
SELECT COUNT(*)
FROM my-project-id.ny_taxi.green_tripdata;
