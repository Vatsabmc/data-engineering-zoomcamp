/*
Question 3. Counting short trips
*/
SELECT COUNT(1)
FROM public.ny_taxi_trips t
WHERE 1 = 1
	AND lpep_pickup_datetime >= '2025-11-01'
    AND lpep_pickup_datetime < '2025-12-01'
	AND trip_distance <= 1;
/*
Question 4. Longest trip for each day
*/
SELECT
    CAST(lpep_pickup_datetime AS DATE) AS "pickup_day",
    MAX(trip_distance) AS "max_trip_distance"
FROM public.ny_taxi_trips t
WHERE 1 = 1
	AND trip_distance <= 100
GROUP BY
    "pickup_day"
ORDER BY
    "max_trip_distance" DESC
LIMIT 5;
/*
Question 5. Biggest pickup zone
*/

SELECT
    CAST(lpep_pickup_datetime AS DATE) AS "pickup_day",
    CONCAT(zpu."Borough", ' | ', zpu."Zone") AS "pickup_loc",
    SUM(total_amount) AS "total_amount_sum"
FROM
    public.ny_taxi_trips t,
    zones zpu
WHERE 1 = 1
    AND t."PULocationID" = zpu."LocationID"
	AND CAST(lpep_pickup_datetime AS DATE) = '2025-11-18'
GROUP BY
    "pickup_day", "pickup_loc"
ORDER BY
    "total_amount_sum" DESC
LIMIT 5;
/*
Question 6. Largest tip
*/
SELECT
    CONCAT(zpu."Borough", ' | ', zpu."Zone") AS "pickup_loc",
    CONCAT(zdo."Borough", ' | ', zdo."Zone") AS "dropoff_loc",
	SUM(tip_amount) AS "max_tip_amount"
FROM
    public.ny_taxi_trips t,
    zones zpu,
    zones zdo
WHERE 1 = 1
    AND t."PULocationID" = zpu."LocationID"
    AND t."DOLocationID" = zdo."LocationID"
	AND zpu."Zone" = 'East Harlem North'
GROUP BY
    "pickup_loc", "dropoff_loc"
ORDER BY
    "max_tip_amount" DESC
LIMIT 5;

