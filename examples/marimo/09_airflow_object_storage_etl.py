# /// script
# requires-python = ">=3.12,<3.15"
# dependencies = [
#     "apache-airflow-task-sdk==1.3.1",
#     "boto3>=1.37.3",
#     "duckdb>=1.3",
#     "fsspec>=2025.5.1",
#     "marimo>=0.17.0",
#     "opendalfs",
#     "pandas>=2.2",
#     "pyarrow>=22.0.0",
# ]
# [tool.uv.sources]
# opendalfs = { path = "../..", editable = true }
# ///

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import json
    import urllib.parse
    import urllib.request

    import duckdb
    import pandas as pd
    from _shared import MinioConfig, clear_prefix, ensure_minio, opendal_filesystem
    from airflow.sdk import ObjectStoragePath
    from airflow.sdk.io import store as airflow_store

    import marimo as mo

    return (
        MinioConfig,
        ObjectStoragePath,
        airflow_store,
        clear_prefix,
        duckdb,
        ensure_minio,
        json,
        mo,
        opendal_filesystem,
        pd,
        urllib,
    )


@app.cell
def _(mo):
    mo.md("""
    # Airflow's cloud-native object-storage ETL, backed by OpenDAL

    This is a notebook-sized migration of Airflow's official
    [Object Storage tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/objectstorage.html):
    fetch public Open-Meteo air-quality data, retain the raw response, write
    Parquet, and query the result with DuckDB. The notebook exercises the public
    Task SDK `ObjectStoragePath`; it does **not** pretend to launch a full Airflow
    scheduler.

    Open-Meteo publishes its API specification under CC BY 4.0 and requires
    attribution to Open-Meteo and the underlying CAMS ENSEMBLE providers.
    """)
    return


@app.cell
def _(
    MinioConfig,
    airflow_store,
    clear_prefix,
    ensure_minio,
    opendal_filesystem,
):
    airflow_minio = MinioConfig.from_env()
    ensure_minio(airflow_minio)
    airflow_fs = opendal_filesystem(airflow_minio)
    airflow_prefix = "09_airflow"
    clear_prefix(airflow_fs, airflow_prefix)
    airflow_protocol = "opendal+s3"
    airflow_conn_id = "opendal-marimo-09"
    airflow_store.attach(
        airflow_protocol,
        conn_id=airflow_conn_id,
        fs=airflow_fs,
    )
    return airflow_conn_id, airflow_minio, airflow_prefix, airflow_protocol


@app.cell
def _(json, urllib):
    air_parameters = {
        "latitude": 52.52,
        "longitude": 13.41,
        "hourly": "pm10,pm2_5",
        "forecast_days": 1,
        "timezone": "UTC",
    }
    air_api_url = (
        "https://air-quality-api.open-meteo.com/v1/air-quality?"
        + urllib.parse.urlencode(air_parameters)
    )
    air_request = urllib.request.Request(
        air_api_url,
        headers={"User-Agent": "opendalfs-marimo-examples/0.3"},
    )
    with urllib.request.urlopen(air_request, timeout=30) as air_response:
        air_payload_bytes = air_response.read()
    air_payload = json.loads(air_payload_bytes)

    assert len(air_payload["hourly"]["time"]) == 24
    assert len(air_payload["hourly"]["pm10"]) == 24
    assert len(air_payload["hourly"]["pm2_5"]) == 24
    return air_api_url, air_payload, air_payload_bytes


@app.cell
def _(
    ObjectStoragePath,
    air_payload,
    air_payload_bytes,
    airflow_conn_id,
    airflow_prefix,
    airflow_protocol,
    pd,
):
    air_raw_path = ObjectStoragePath(
        f"{airflow_prefix}/raw/air-quality.json",
        protocol=airflow_protocol,
        conn_id=airflow_conn_id,
    )
    air_parquet_path = ObjectStoragePath(
        f"{airflow_prefix}/curated/air-quality.parquet",
        protocol=airflow_protocol,
        conn_id=airflow_conn_id,
    )
    air_raw_path.parent.mkdir(parents=True, exist_ok=True)
    with air_raw_path.open("wb") as air_raw_stream:
        air_raw_stream.write(air_payload_bytes)

    air_frame = pd.DataFrame(air_payload["hourly"])
    air_frame["time"] = pd.to_datetime(air_frame["time"], utc=True)
    air_parquet_path.parent.mkdir(parents=True, exist_ok=True)
    with air_parquet_path.open("wb") as air_parquet_stream:
        air_frame.to_parquet(air_parquet_stream, index=False)

    assert air_raw_path.read_bytes() == air_payload_bytes
    assert air_raw_path.exists() and air_parquet_path.exists()
    return air_frame, air_parquet_path, air_raw_path


@app.cell
def _(air_frame, air_parquet_path, airflow_minio, duckdb):
    air_connection = duckdb.connect(database=":memory:")
    air_connection.register_filesystem(air_parquet_path.fs)
    air_duckdb_url = airflow_minio.url(air_parquet_path.path, protocol="opendal+s3")
    air_summary = air_connection.execute(
        """
        SELECT
            count(*) AS observations,
            avg(pm10) AS mean_pm10,
            avg(pm2_5) AS mean_pm2_5
        FROM read_parquet(?)
        """,
        [air_duckdb_url],
    ).fetchone()
    air_connection.close()

    assert air_summary is not None
    assert air_summary[0] == len(air_frame) == 24
    assert air_summary[1] is not None and air_summary[1] >= 0
    assert air_summary[2] is not None and air_summary[2] >= 0
    return (air_summary,)


@app.cell
def _(air_api_url, air_parquet_path, air_raw_path, air_summary, mo):
    mo.md(
        f"""
    ## Verified ETL

    - API request: [{air_api_url}]({air_api_url})
    - Raw object: `{air_raw_path}`
    - Curated object: `{air_parquet_path}`
    - Observations: {air_summary[0]}
    - Mean PM10: {air_summary[1]:.2f} µg/m³
    - Mean PM2.5: {air_summary[2]:.2f} µg/m³

    The source values change as forecasts update, so the executable contract checks
    schema, hourly cardinality, non-negative aggregates, and round-trip bytes—not
    a hard-coded pollution reading.
    """
    )
    return


if __name__ == "__main__":
    app.run()
