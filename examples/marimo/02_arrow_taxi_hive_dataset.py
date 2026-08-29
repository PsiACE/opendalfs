# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
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
    import datetime as dt

    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
    from _shared import (
        MinioConfig,
        clear_prefix,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        opendal_filesystem,
    )

    import marimo as mo

    return (
        MinioConfig,
        clear_prefix,
        dataset_spec,
        ds,
        dt,
        ensure_minio,
        fetch_to_minio,
        mo,
        opendal_filesystem,
        pa,
        pc,
        pq,
    )


@app.cell
def _(mo):
    mo.md("""
    # Build a Hive-partitioned taxi dataset with PyArrow

    An urban-mobility team receives one monthly Parquet object. We turn it into a
    date-partitioned dataset, then demonstrate projection and partition pruning
    without writing a custom Arrow adapter.

    PyArrow officially accepts fsspec filesystems through `FSSpecHandler`:
    [filesystem integration](https://arrow.apache.org/docs/python/filesystems.html) ·
    [`write_dataset`](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html) ·
    [Hive partitioning](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.partitioning.html).
    The trip records are published by the
    [NYC Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
    under the NYC Open Data Terms of Use.
    """)
    return


@app.cell
def _(MinioConfig, ensure_minio, opendal_filesystem):
    minio = MinioConfig.from_env()
    ensure_minio(minio)
    arrow_fs = opendal_filesystem(minio)
    assert type(arrow_fs).__module__.startswith("opendalfs")
    return arrow_fs, minio


@app.cell
def _(arrow_fs, dataset_spec, fetch_to_minio):
    taxi_spec = dataset_spec("nyc_taxi")
    taxi_raw_key = "raw/arrow-taxi/yellow_tripdata_2025-01.parquet"
    taxi_download = fetch_to_minio(arrow_fs, taxi_spec, taxi_raw_key, timeout=120)
    assert taxi_download.size == 59_158_238
    return taxi_download, taxi_raw_key, taxi_spec


@app.cell
def _(arrow_fs, dt, pc, pq, taxi_raw_key):
    with arrow_fs.open(taxi_raw_key, "rb") as taxi_source:
        taxi_table = pq.read_table(taxi_source)

    pickup = taxi_table["tpep_pickup_datetime"]
    in_january = pc.and_(
        pc.greater_equal(pickup, dt.datetime(2025, 1, 1)),
        pc.less(pickup, dt.datetime(2025, 2, 1)),
    )
    january_taxi = taxi_table.filter(in_january)
    january_taxi = january_taxi.append_column(
        "pickup_date",
        pc.strftime(january_taxi["tpep_pickup_datetime"], format="%Y-%m-%d"),
    )

    assert january_taxi.num_rows > 3_000_000
    assert pc.count_distinct(january_taxi["pickup_date"]).as_py() == 31
    return (january_taxi,)


@app.cell
def _(arrow_fs, clear_prefix, ds, january_taxi, pa):
    taxi_dataset_key = "curated/arrow-taxi-hive"
    clear_prefix(arrow_fs, taxi_dataset_key)
    pickup_partitioning = ds.partitioning(
        pa.schema([("pickup_date", pa.string())]),
        flavor="hive",
    )
    ds.write_dataset(
        january_taxi,
        base_dir=taxi_dataset_key,
        filesystem=arrow_fs,
        format="parquet",
        partitioning=pickup_partitioning,
        basename_template="part-{i}.parquet",
        max_rows_per_file=250_000,
        max_rows_per_group=100_000,
    )
    return (taxi_dataset_key,)


@app.cell
def _(arrow_fs, ds, january_taxi, minio, pc, taxi_dataset_key):
    published_dataset = ds.dataset(
        f"{minio.bucket}/{taxi_dataset_key}",
        filesystem=arrow_fs,
        format="parquet",
        partitioning="hive",
    )
    selected_day = "2025-01-15"
    selected_columns = ["VendorID", "trip_distance", "total_amount", "pickup_date"]
    selected_table = published_dataset.to_table(
        columns=selected_columns,
        filter=ds.field("pickup_date") == selected_day,
    )

    expected_rows = pc.sum(pc.equal(january_taxi["pickup_date"], selected_day)).as_py()
    selected_fragments = list(
        published_dataset.get_fragments(
            filter=ds.field("pickup_date") == selected_day,
        )
    )
    assert selected_table.num_rows == expected_rows
    assert selected_table.num_rows > 50_000
    assert len(selected_fragments) <= 2
    assert set(selected_table.column_names) == set(selected_columns)
    return selected_day, selected_fragments, selected_table


@app.cell
def _(
    minio,
    mo,
    selected_day,
    selected_fragments,
    selected_table,
    taxi_dataset_key,
    taxi_download,
    taxi_spec,
):
    mo.md(
        f"""
    ## Partition pruning verified

    - Source: [{taxi_spec.name}]({taxi_spec.source}), {taxi_spec.license}
    - Raw object: `{minio.url(taxi_download.key, protocol="opendal+s3")}`
    - Published dataset: `{minio.url(taxi_dataset_key, protocol="opendal+s3")}`
    - Query: `{selected_day}`, four projected columns
    - Result: {selected_table.num_rows:,} rows from {len(selected_fragments)} fragment(s)

    The row count is checked against the original in-memory table, so a successful
    run verifies both the write and the pruned read.
            """
    )
    return


if __name__ == "__main__":
    app.run()
