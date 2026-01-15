#!/usr/bin/env python
# coding: utf-8
import io
import click
import requests
import pandas as pd
from tqdm.auto import tqdm
import pyarrow.parquet as pq
from sqlalchemy import create_engine

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "lpep_pickup_datetime", "lpep_dropoff_datetime"]


@click.command()
@click.option("--pg-user", default="root", help="PostgreSQL user")
@click.option("--pg-pass", default="root", help="PostgreSQL password")
@click.option("--pg-host", default="localhost", help="PostgreSQL host")
@click.option("--pg-port", default=5432, type=int, help="PostgreSQL port")
@click.option("--pg-db", default="ny_taxi", help="PostgreSQL database name")
@click.option("--data-format", default="parquet", help="Data format (csv or parquet)")
@click.option("--year", default=2021, type=int, help="Year of the data")
@click.option("--month", default=1, type=int, help="Month of the data")
@click.option("--target-table", default="yellow_taxi_data", help="Target table name")
@click.option(
    "--chunksize", default=100000, type=int, help="Chunk size for reading CSV"
)
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, data_format, year, month, target_table, chunksize):
    """Ingest NYC taxi data into PostgreSQL database."""
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )
    print("Connected to the database.")

    load_zones(engine)
    print("Zones data loaded.")

    if data_format == "csv":
        load_csv(engine, year, month, target_table, chunksize)
    elif data_format == "parquet":
        load_parquet(engine, year, month, target_table, chunksize)
    else:
        raise ValueError("Unsupported data format. Use 'csv' or 'parquet'.")
    print("Taxi trips data loaded.")


def load_csv(engine, year: int, month: int, target_table: str, chunksize: int) -> None:
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
            first = False

        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")


def load_parquet(
    engine, year: int, month: int, target_table: str, chunksize: int
) -> None:
    prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    url = f"{prefix}/green_tripdata_{year}-{month:02d}.parquet"
    response = requests.get(url)
    response.raise_for_status()

    # Create ParquetFile from in-memory buffer
    parquet_file = pq.ParquetFile(io.BytesIO(response.content))

    first = True

    # --- Iterate over batches ---
    for batch in tqdm(parquet_file.iter_batches(batch_size=chunksize), desc="Loading Parquet in chunks"):
        df_chunk = batch.to_pandas()

        if first:
            # Create table schema
            df_chunk.head(0).to_sql(
                name=target_table, con=engine, if_exists="replace", index=False
            )
            first = False

        # Append data
        df_chunk.to_sql(name=target_table, con=engine, if_exists="append", index=False)


def load_zones(engine) -> None:
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
    df_zones = pd.read_csv(url)
    df_zones.to_sql(name="zones", con=engine, if_exists="replace", index=False)


if __name__ == "__main__":
    run()
