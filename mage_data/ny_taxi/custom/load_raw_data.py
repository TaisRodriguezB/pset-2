import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text


def get_db_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def month_list(start_year: int, end_year: int):
    months = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            months.append(f"{year}-{month:02d}")
    return months


def build_url(taxi_type: str, period: str):
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{period}.parquet"


def standardize_columns(df: pd.DataFrame, taxi_type: str):
    df.columns = [c.lower().strip() for c in df.columns]

    if taxi_type == "yellow":
        rename_map = {
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
            "pulocationid": "pickup_location_id",
            "dolocationid": "dropoff_location_id",
        }
    else:
        rename_map = {
            "lpep_pickup_datetime": "pickup_datetime",
            "lpep_dropoff_datetime": "dropoff_datetime",
            "pulocationid": "pickup_location_id",
            "dolocationid": "dropoff_location_id",
        }

    df = df.rename(columns=rename_map)

    keep_cols = [
        "vendorid",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "passenger_count",
        "trip_distance",
        "ratecodeid",
        "store_and_fwd_flag",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
    ]

    for col in keep_cols:
        if col not in df.columns:
            df[col] = None

    df = df[keep_cols].copy()
    df["taxi_type"] = taxi_type
    return df


def table_exists_already(engine, dataset_name: str, period: str):
    sql = """
        SELECT 1
        FROM raw.load_log
        WHERE dataset_name = :dataset_name
          AND file_period = :file_period
          AND status = 'success'
        LIMIT 1
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(sql),
            {"dataset_name": dataset_name, "file_period": period}
        ).fetchone()
    return result is not None


def write_log(engine, dataset_name: str, period: str, source_url: str, status: str):
    sql = """
        INSERT INTO raw.load_log (dataset_name, file_period, source_url, status)
        VALUES (:dataset_name, :file_period, :source_url, :status)
        ON CONFLICT (dataset_name, file_period)
        DO UPDATE SET
            source_url = EXCLUDED.source_url,
            status = EXCLUDED.status,
            loaded_at = CURRENT_TIMESTAMP
    """
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "dataset_name": dataset_name,
                "file_period": period,
                "source_url": source_url,
                "status": status,
            },
        )


if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom


@custom
def transform_custom(*args, **kwargs):
    taxi_type = os.getenv("TAXI_TYPE", "green").lower()
    start_year = int(os.getenv("START_YEAR", "2024"))
    end_year = int(os.getenv("END_YEAR", "2025"))
    dataset_name = f"{taxi_type}_tripdata"

    engine = get_db_engine()

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE SCHEMA IF NOT EXISTS raw;
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.load_log (
                id SERIAL PRIMARY KEY,
                dataset_name TEXT NOT NULL,
                file_period TEXT NOT NULL,
                source_url TEXT NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL,
                UNIQUE (dataset_name, file_period)
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.taxi_trips_raw (
                vendorid NUMERIC NULL,
                pickup_datetime TIMESTAMP NULL,
                dropoff_datetime TIMESTAMP NULL,
                pickup_location_id NUMERIC NULL,
                dropoff_location_id NUMERIC NULL,
                passenger_count NUMERIC NULL,
                trip_distance NUMERIC NULL,
                ratecodeid NUMERIC NULL,
                store_and_fwd_flag TEXT NULL,
                payment_type NUMERIC NULL,
                fare_amount NUMERIC NULL,
                extra NUMERIC NULL,
                mta_tax NUMERIC NULL,
                tip_amount NUMERIC NULL,
                tolls_amount NUMERIC NULL,
                total_amount NUMERIC NULL,
                congestion_surcharge NUMERIC NULL,
                airport_fee NUMERIC NULL,
                cbd_congestion_fee NUMERIC NULL,
                taxi_type TEXT NULL,
                source_period TEXT NULL,
                source_url TEXT NULL,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))

    results = []

    for period in month_list(start_year, end_year):
        url = build_url(taxi_type, period)

        if table_exists_already(engine, dataset_name, period):
            results.append({"period": period, "status": "skipped"})
            continue

        try:
            df = pd.read_parquet(url)
            df = standardize_columns(df, taxi_type)

            df["source_period"] = period
            df["source_url"] = url
            df["ingested_at"] = datetime.utcnow()

            df.to_sql(
                "taxi_trips_raw",
                engine,
                schema="raw",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=50000,
            )

            write_log(engine, dataset_name, period, url, "success")

            results.append({
                "period": period,
                "status": "loaded",
                "rows": len(df)
            })

        except Exception as e:
            write_log(engine, dataset_name, period, url, "failed")
            results.append({
                "period": period,
                "status": "failed",
                "error": str(e)
            })

    return results