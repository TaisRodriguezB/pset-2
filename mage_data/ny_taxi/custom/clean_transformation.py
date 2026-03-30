import os
import pandas as pd
from sqlalchemy import create_engine, text


def get_db_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom


@custom
def transform_custom(*args, **kwargs):
    engine = get_db_engine()

    zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    zones = pd.read_csv(zone_url)

    zones.columns = [c.lower().strip() for c in zones.columns]
    zones = zones.rename(columns={"locationid": "location_id"})

    payment_type_map = pd.DataFrame({
        "payment_type_code": [1, 2, 3, 4, 5, 6],
        "payment_type_desc": [
            "Credit card",
            "Cash",
            "No charge",
            "Dispute",
            "Unknown",
            "Voided trip",
        ],
    })

    vendor_map = pd.DataFrame({
        "vendor_code": [1, 2],
        "vendor_name": ["Creative Mobile Technologies", "VeriFone Inc."]
    })

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS clean;"))

        conn.execute(text("DROP TABLE IF EXISTS clean.fact_trips;"))
        conn.execute(text("DROP TABLE IF EXISTS clean.dim_pickup_location;"))
        conn.execute(text("DROP TABLE IF EXISTS clean.dim_dropoff_location;"))
        conn.execute(text("DROP TABLE IF EXISTS clean.dim_payment_type;"))
        conn.execute(text("DROP TABLE IF EXISTS clean.dim_vendor;"))

    vendor_map.to_sql(
        "dim_vendor",
        engine,
        schema="clean",
        if_exists="replace",
        index=False
    )

    payment_type_map.to_sql(
        "dim_payment_type",
        engine,
        schema="clean",
        if_exists="replace",
        index=False
    )

    zones.to_sql(
        "dim_pickup_location",
        engine,
        schema="clean",
        if_exists="replace",
        index=False
    )

    zones.to_sql(
        "dim_dropoff_location",
        engine,
        schema="clean",
        if_exists="replace",
        index=False
    )

    fact_sql = """
    CREATE TABLE clean.fact_trips AS
    WITH base AS (
        SELECT
            vendorid::INT AS vendor_code,
            pickup_datetime,
            dropoff_datetime,
            pickup_location_id::INT AS pickup_location_id,
            dropoff_location_id::INT AS dropoff_location_id,
            passenger_count::NUMERIC AS passenger_count,
            trip_distance::NUMERIC AS trip_distance,
            payment_type::INT AS payment_type_code,
            fare_amount::NUMERIC AS fare_amount,
            tip_amount::NUMERIC AS tip_amount,
            tolls_amount::NUMERIC AS tolls_amount,
            total_amount::NUMERIC AS total_amount,
            taxi_type,
            source_period,
            EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60.0 AS duration_minutes
        FROM raw.taxi_trips_raw
        WHERE pickup_datetime IS NOT NULL
          AND dropoff_datetime IS NOT NULL
          AND dropoff_datetime >= pickup_datetime
          AND pickup_location_id IS NOT NULL
          AND dropoff_location_id IS NOT NULL
          AND pickup_location_id <> dropoff_location_id
          AND COALESCE(trip_distance, 0) > 0
          AND COALESCE(total_amount, 0) >= 0
          AND COALESCE(fare_amount, 0) >= 0
    )
    SELECT
        ROW_NUMBER() OVER () AS trip_key,
        vendor_code,
        payment_type_code,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        duration_minutes,
        taxi_type,
        source_period
    FROM base;
    """

    with engine.begin() as conn:
        conn.execute(text(fact_sql))

        conn.execute(text("ALTER TABLE clean.dim_vendor ADD PRIMARY KEY (vendor_code);"))
        conn.execute(text("ALTER TABLE clean.dim_payment_type ADD PRIMARY KEY (payment_type_code);"))
        conn.execute(text("ALTER TABLE clean.dim_pickup_location ADD PRIMARY KEY (location_id);"))
        conn.execute(text("ALTER TABLE clean.dim_dropoff_location ADD PRIMARY KEY (location_id);"))
        conn.execute(text("ALTER TABLE clean.fact_trips ADD PRIMARY KEY (trip_key);"))

    return {
        "status": "success",
        "message": "Clean schema and star model created"
    }
