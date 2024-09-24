import requests
import duckdb
import pandas as pd
import os
from dagster_duckdb import DuckDBResource
from dagster_university.assets import constants
from dagster import asset, AssetExecutionContext
from ..partitions import monthly_partition, weekly_partition


@asset(
    partitions_def=monthly_partition
)
def taxi_trips_file(context: AssetExecutionContext) -> None:
    """
    The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(
        constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb"
    ) as output_file:
        output_file.write(raw_trips.content)


@asset
def taxi_zones_file() -> None:
    """
    The raw csv file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """

    taxi_zones = requests.get(
        "https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD"
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(taxi_zones.content)


@asset(
    deps=["taxi_trips_file"],
    partitions_def=monthly_partition,    
)
def taxi_trips(context: AssetExecutionContext, database: DuckDBResource) -> None:
    """
    The raw taxi trips dataset, loaded into a DuckDB database
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    query = f"""
        create table if not exists trips (
            vendor_id integer
            , pickup_zone_id integer
            , dropoff_zone_id integer
            , rate_code_id double
            , payment_type integer
            , dropoff_datetime timestamp
            , pickup_datetime timestamp
            , trip_distance double
            , passenger_count double
            , total_amount double
            , partition_date varchar
        );

        delete from trips where partition_date = '{month_to_fetch}';

        insert into trips
        select
            VendorID
            , PULocationID
            , DOLocationID
            , RatecodeID
            , payment_type
            , tpep_dropoff_datetime
            , tpep_pickup_datetime
            , trip_distance
            , passenger_count
            , total_amount
            , '{month_to_fetch}' as partition_date
        from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
    """

    with database.get_connection() as conn:
        conn.execute(query)



@asset(
    deps=["taxi_zones_file"],
)
def taxi_zones(database: DuckDBResource):
    """
        The raw taxi zones dataset, loaded into a DuckDB database.
    """

    query = f"""
        create or replace table zones as (
            select
                LocationID as zone_id,
                zone,
                borough,
                the_geom as geometry
            from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)


@asset(
    deps=["taxi_trips"],
    partitions_def=weekly_partition
)
def trips_by_week(context: AssetExecutionContext, database: DuckDBResource) -> None:
    period_to_fetch = context.partition_key

    query = f"""
        select
            weekofyear(pickup_datetime) as period
            , count(vendor_id) as num_trips
            , sum(total_amount) as total_amount
            , sum(trip_distance) as trip_distance
            , sum(passenger_count) as passenger_count
        from trips
        where pickup_datetime >= '{period_to_fetch}'
            and pickup_datetime < '{period_to_fetch}'::date + interval '1 week'
        group by period
    """

    with database.get_connection() as conn:
        trips_by_week_df = conn.execute(query).fetch_df()

    trips_by_week_df["period"] = period_to_fetch
    trips_by_week_df = trips_by_week_df.sort_values("period")

    trips_by_week_df.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)

    try:
        # If the file already exists, append to it, but replace the existing month's data
        existing = pd.read_csv(constants.TRIPS_BY_WEEK_FILE_PATH)
        existing = existing[existing["period"] != period_to_fetch]
        existing = pd.concat([existing, trips_by_week_df]).sort_values(by="period")
        existing.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
    except FileNotFoundError:
        trips_by_week_df.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)
