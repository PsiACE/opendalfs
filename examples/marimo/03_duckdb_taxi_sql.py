# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
#     "duckdb>=1.4.0",
#     "fsspec>=2025.5.1",
#     "marimo>=0.17.0",
#     "opendalfs",
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
    import duckdb
    from _shared import (
        MinioConfig,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        opendal_filesystem,
    )

    import marimo as mo

    return (
        MinioConfig,
        dataset_spec,
        duckdb,
        ensure_minio,
        fetch_to_minio,
        mo,
        opendal_filesystem,
    )


@app.cell
def _(mo):
    mo.md("""
    # Query Parquet in place with DuckDB and opendalfs

    An operations analyst needs revenue and trip counts by borough without first
    loading millions of taxi rows into pandas. DuckDB's Python client officially
    accepts an fsspec filesystem through `register_filesystem`, so the SQL remains
    ordinary `read_parquet`/`read_csv_auto` SQL:
    [DuckDB fsspec guide](https://duckdb.org/docs/current/guides/python/filesystems) ·
    [DuckDB marimo guide](https://duckdb.org/docs/stable/guides/python/marimo).

    Both files are published by the
    [NYC Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
    under the NYC Open Data Terms of Use.
    """)
    return


@app.cell
def _(MinioConfig, ensure_minio, opendal_filesystem):
    minio = MinioConfig.from_env()
    ensure_minio(minio)
    duckdb_fs = opendal_filesystem(minio)
    assert type(duckdb_fs).__module__.startswith("opendalfs")
    return duckdb_fs, minio


@app.cell
def _(dataset_spec, duckdb_fs, fetch_to_minio):
    taxi_spec = dataset_spec("nyc_taxi")
    zones_spec = dataset_spec("nyc_taxi_zones")
    taxi_key = "raw/duckdb-taxi/yellow_tripdata_2025-01.parquet"
    zones_key = "raw/duckdb-taxi/taxi_zone_lookup.csv"
    taxi_download = fetch_to_minio(duckdb_fs, taxi_spec, taxi_key, timeout=120)
    zones_download = fetch_to_minio(duckdb_fs, zones_spec, zones_key)
    assert taxi_download.size == 59_158_238
    assert zones_download.size == 12_331
    return (
        taxi_download,
        taxi_key,
        taxi_spec,
        zones_download,
        zones_key,
        zones_spec,
    )


@app.cell
def _(duckdb, duckdb_fs):
    connection = duckdb.connect()
    connection.register_filesystem(duckdb_fs)
    return (connection,)


@app.cell
def _(connection, minio, taxi_key, zones_key):
    taxi_url = minio.url(taxi_key, protocol="opendal+s3")
    zones_url = minio.url(zones_key, protocol="opendal+s3")
    borough_summary = connection.execute(
        """
        SELECT
            zones.Borough AS borough,
            count(*) AS trips,
            round(sum(taxi.total_amount), 2) AS gross_revenue
        FROM read_parquet(?) AS taxi
        INNER JOIN read_csv_auto(?) AS zones
            ON taxi.PULocationID = zones.LocationID
        WHERE
            taxi.tpep_pickup_datetime >= TIMESTAMP '2025-01-01'
            AND taxi.tpep_pickup_datetime < TIMESTAMP '2025-02-01'
            AND taxi.total_amount >= 0
        GROUP BY zones.Borough
        ORDER BY trips DESC
        """,
        [taxi_url, zones_url],
    ).to_arrow_table()

    total_trips = sum(borough_summary["trips"].to_pylist())
    assert borough_summary.num_rows >= 5
    assert total_trips > 3_000_000
    assert "Manhattan" in borough_summary["borough"].to_pylist()
    return borough_summary, taxi_url, total_trips


@app.cell
def _(connection, taxi_url, total_trips):
    direct_count = connection.execute(
        """
        SELECT count(*)
        FROM read_parquet(?)
        WHERE
            tpep_pickup_datetime >= TIMESTAMP '2025-01-01'
            AND tpep_pickup_datetime < TIMESTAMP '2025-02-01'
            AND total_amount >= 0
        """,
        [taxi_url],
    ).fetchone()[0]
    assert direct_count == total_trips
    return


@app.cell
def _(
    borough_summary,
    minio,
    mo,
    taxi_download,
    taxi_spec,
    total_trips,
    zones_download,
    zones_spec,
):
    mo.md(
        f"""
        ## SQL result verified

        - Fact source: [{taxi_spec.name}]({taxi_spec.source}), {taxi_spec.license}
        - Dimension source: [{zones_spec.name}]({zones_spec.source}), {zones_spec.license}
        - Fact object: `{minio.url(taxi_download.key, protocol="opendal+s3")}`
        - Dimension object: `{minio.url(zones_download.key, protocol="opendal+s3")}`
        - Joined non-negative January trips: {total_trips:,}

        The sum of grouped trip counts is checked against an independent direct count.

    ```
    {borough_summary}
    ```
                """
    )
    return


@app.cell
def _(connection):
    connection.close()
    return


if __name__ == "__main__":
    app.run()
