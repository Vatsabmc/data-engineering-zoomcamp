import json
from dataclasses import dataclass


@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    tpep_pickup_datetime: int  # epoch milliseconds


@dataclass
class GreenRide:
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float


def ride_from_row(row):
    return Ride(
        PULocationID=int(row["PULocationID"]),
        DOLocationID=int(row["DOLocationID"]),
        trip_distance=float(row["trip_distance"]),
        total_amount=float(row["total_amount"]),
        tpep_pickup_datetime=int(row["tpep_pickup_datetime"].timestamp() * 1000),
    )


def green_ride_from_row(row):
    import pandas as pd

    def safe_int(val, default=0):
        return default if pd.isna(val) else int(val)

    def safe_float(val, default=0.0):
        return default if pd.isna(val) else float(val)

    def safe_datetime_str(val):
        if pd.isna(val):
            return "1970-01-01 00:00:00"
        return val.strftime("%Y-%m-%d %H:%M:%S")

    return GreenRide(
        lpep_pickup_datetime=safe_datetime_str(row["lpep_pickup_datetime"]),
        lpep_dropoff_datetime=safe_datetime_str(row["lpep_dropoff_datetime"]),
        PULocationID=safe_int(row["PULocationID"]),
        DOLocationID=safe_int(row["DOLocationID"]),
        passenger_count=safe_int(row["passenger_count"]),
        trip_distance=safe_float(row["trip_distance"]),
        tip_amount=safe_float(row["tip_amount"]),
        total_amount=safe_float(row["total_amount"]),
    )


def ride_deserializer(ride_class=Ride, data=None):
    json_str = data.decode("utf-8")
    ride_dict = json.loads(json_str)
    return ride_class(**ride_dict)
