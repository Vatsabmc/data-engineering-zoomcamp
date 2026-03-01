/* @bruin

name: reports.trips_report

type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table

columns:
  - name: pickup_date
    type: date
    description: Date of the trip pickup
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: The payment method used
    primary_key: true
  - name: pickup_location_id
    type: integer
    description: TLC taxi zone of pickup
    primary_key: true
  - name: trip_count
    type: bigint
    description: Number of trips in this grouping
    checks:
      - name: positive
  - name: avg_fare_amount
    type: float
    description: Average fare amount
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: float
    description: Average trip distance in miles
    checks:
      - name: non_negative
  - name: avg_tip_amount
    type: float
    description: Average tip amount
  - name: total_revenue
    type: float
    description: Total revenue (sum of fares + tips + tolls)
    checks:
      - name: non_negative
  - name: avg_passenger_count
    type: float
    description: Average number of passengers

custom_checks:
  - name: positive_row_count
    description: Report table should have data for requested period
    query: SELECT COUNT(*) > 0 FROM reports.trips_report
    value: 1


@bruin */

SELECT
  CAST(pickup_datetime AS DATE) as pickup_date,
  payment_type_name,
  pickup_location_id,

  -- Count metrics
  COUNT(*) as trip_count,
  SUM(COALESCE(passenger_count, 0)) as total_passengers,

  -- Distance metrics
  ROUND(SUM(COALESCE(trip_distance, 0)), 2) as total_distance,

  -- Revenue metrics
  ROUND(SUM(COALESCE(fare_amount, 0)), 2) as total_fare,
  ROUND(SUM(COALESCE(tip_amount, 0)), 2) as total_tips,
  ROUND(SUM(COALESCE(total_amount, 0)), 2) as total_revenue,

  -- Average metrics
  ROUND(AVG(COALESCE(fare_amount, 0)), 2) as avg_fare_amount,
  ROUND(AVG(COALESCE(trip_distance, 0)), 2) as avg_trip_distance,
  ROUND(AVG(COALESCE(passenger_count, 0)), 2) as avg_passenger_count
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  CAST(pickup_datetime AS DATE),
  payment_type_name,
  pickup_location_id
ORDER BY
  pickup_date DESC,
  trip_count DESC
