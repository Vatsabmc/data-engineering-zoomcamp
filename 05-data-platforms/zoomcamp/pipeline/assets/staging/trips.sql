/* @bruin

name: staging.trips

type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table

columns:
  - name: pickup_datetime
    type: timestamp
    description: The date and time when the meter was engaged
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: The date and time when the meter was disengaged
    primary_key: true
    checks:
      - name: not_null
  - name: trip_distance
    type: float
    description: The elapsed trip distance in miles
    checks:
      - name: non_negative
  - name: RatecodeID
    type: integer
    description: The final rate code in effect at the end of the trip
  - name: store_and_fwd_flag
    type: string
    description: Whether the trip record was held in vehicle memory
  - name: pickup_location_id
    type: integer
    description: TLC taxi zone in which the pickup occurred
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: TLC taxi zone in which the dropoff occurred
    checks:
      - name: not_null
  - name: payment_type
    type: integer
    description: A numeric code signifying how the passenger paid
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: The time-and-distance fare calculated by the meter
    primary_key: true
    checks:
      - name: non_negative
  - name: extra
    type: float
    description: Miscellaneous extras and surcharges
  - name: mta_tax
    type: float
    description: $0.50 MTA tax
  - name: tip_amount
    type: float
    description: Tip amount for credit card tipping
  - name: tolls_amount
    type: float
    description: Total amount of all tolls paid in trip
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: The total amount charged to passengers
    checks:
      - name: non_negative
  - name: payment_type_name
    type: string
    description: Human-readable payment type (from lookup table)
  - name: extracted_at
    type: timestamp
    description: Timestamp when the record was extracted from the source

custom_checks:
  - name: no_duplicate_trips
    description: Ensures trips are deduplicated within the time window
    query: |
      SELECT COUNT(*) - COUNT(DISTINCT CONCAT(pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount)) as duplicates
      FROM staging.trips
    value: 0

@bruin */

WITH source_data AS(
  SELECT
  -- Pickup/dropoff datetime
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,

  -- Location IDs
    pu_location_id AS pickup_location_id,
    do_location_id AS dropoff_location_id,
  
  -- Trip details
    passenger_count,
    trip_distance,

  -- Payment info
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,

  -- Metadata
    extracted_at
  FROM ingestion.trips
  WHERE 1=1
    -- Filter out ivalid records
    AND tpep_pickup_datetime IS NOT NULL
    AND fare_amount >= 0
    AND total_amount >= 0

),

deduped AS (
  -- Deduplicate raw ingestion data using ROW_NUMBER over composite key
  -- We keep only the first occurrence (RN=1) for each unique trip
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY 
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        fare_amount
      ORDER BY extracted_at DESC
    ) as rn
  FROM source_data
)

SELECT 
  d.pickup_datetime,
  d.dropoff_datetime,
  d.pickup_location_id,
  d.dropoff_location_id,
  d.passenger_count,
  d.trip_distance,
  d.payment_type,
  COALESCE(p.payment_type_name, 'UNKNOWN') as payment_type_name,
  d.fare_amount,
  d.extra,
  d.mta_tax,
  d.tip_amount,
  d.tolls_amount,
  d.improvement_surcharge,
  d.total_amount,
  d.extracted_at
FROM deduped d
LEFT JOIN ingestion.payment_lookup p
  ON d.payment_type = p.payment_type_id
WHERE d.rn = 1

