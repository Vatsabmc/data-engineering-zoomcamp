"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: VendorID
    type: integer
    description: A code indicating the TPEP provider (1=Creative Mobile Technologies, 2=VeriFone)
  - name: tpep_pickup_datetime
    type: timestamp
    description: The date and time when the meter was engaged
  - name: tpep_dropoff_datetime
    type: timestamp
    description: The date and time when the meter was disengaged
  - name: passenger_count
    type: integer
    description: The number of passengers in the vehicle
  - name: trip_distance
    type: float
    description: The elapsed trip distance in miles reported by the taximeter
  - name: RatecodeID
    type: integer
    description: The final rate code in effect at the end of the trip
  - name: store_and_fwd_flag
    type: string
    description: This flag indicates whether the trip record was held in vehicle memory before sending (Y=yes, N=no)
  - name: PULocationID
    type: integer
    description: TLC taxi zone in which the pickup occurred
  - name: DOLocationID
    type: integer
    description: TLC taxi zone in which the dropoff occurred
  - name: payment_type
    type: integer
    description: A numeric code signifying how the passenger paid for the trip (1=Credit card, 2=Cash, etc.)
  - name: fare_amount
    type: float
    description: The time-and-distance fare calculated by the meter
  - name: extra
    type: float
    description: Miscellaneous extras and surcharges (rush hour, overnight, etc.)
  - name: mta_tax
    type: float
    description: $0.50 MTA tax that is automatically triggered based on the metered rate in use
  - name: tip_amount
    type: float
    description: Tip amount for credit card tipping (cash tips are not included)
  - name: tolls_amount
    type: float
    description: Total amount of all tolls paid in trip
  - name: total_amount
    type: float
    description: The total amount charged to passengers (includes fare, extras, MTA tax, tip, tolls)
  - name: extracted_at
    type: timestamp
    description: Timestamp when the record was extracted from the source

@bruin"""

import os
import pandas as pd
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta


def materialize():
    """
    Fetch NYC Taxi trip data from TLC public endpoint.

    This function:
    - Uses BRUIN_START_DATE/BRUIN_END_DATE to determine date range
    - Reads taxi_types from BRUIN_VARS (e.g., ["yellow", "green"])
    - Fetches parquet files from TLC endpoint: https://d37ci6vzurychx.cloudfront.net/trip-data/
    - Combines all fetched data into a single DataFrame
    - Adds extracted_at timestamp for lineage tracking
    - Returns the concatenated DataFrame for Bruin to load into DuckDB

    Environment variables:
    - BRUIN_START_DATE: Start date (YYYY-MM-DD)
    - BRUIN_END_DATE: End date (YYYY-MM-DD)
    - BRUIN_VARS: JSON with pipeline variables including taxi_types list
    """

    # Get environment variables
    start_date_str = os.getenv("BRUIN_START_DATE")
    end_date_str = os.getenv("BRUIN_END_DATE")
    bruin_vars_str = os.getenv("BRUIN_VARS", "{}")

    # Parse dates (format: YYYY-MM-DD)
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Parse pipeline variables to get taxi_types
    bruin_vars = json.loads(bruin_vars_str) if bruin_vars_str else {}
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    print(f"Ingesting taxi data from {start_date.date()} to {end_date.date()}")
    print(f"Taxi types: {taxi_types}")

    # Base URL for TLC trip data
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    dataframes = []
    extraction_time = datetime.utcnow()

    # Iterate through each month in the date range
    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month

        # Fetch data for each taxi type in this month
        for taxi_type in taxi_types:
            # File naming pattern: {taxi_type}_tripdata_{YYYY}-{MM}.parquet
            filename = f"{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
            url = base_url + filename

            try:
                print(f"Fetching {filename}...")
                df = pd.read_parquet(url)

                # Add extraction timestamp for lineage and debugging
                df["extracted_at"] = extraction_time

                dataframes.append(df)
                print(f"  Loaded {filename}: {len(df)} rows")

            except Exception as e:
                print(f"  Warning: Could not fetch {filename}: {str(e)}")
                continue

        # Move to next month
        current_date += relativedelta(months=1)

    if not dataframes:
        raise ValueError(
            f"No data files found for taxi types {taxi_types} in range {start_date.date()} to {end_date.date()}"
        )

    # Combine all dataframes
    result_df = pd.concat(dataframes, ignore_index=True)

    print(f"\nTotal rows ingested: {len(result_df)}")
    print(f"Columns: {list(result_df.columns)}")

    return result_df
