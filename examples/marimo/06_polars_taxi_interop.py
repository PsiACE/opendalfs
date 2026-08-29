# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
#     "fsspec>=2025.5.1",
#     "marimo>=0.17.0",
#     "opendalfs",
#     "polars>=1.30.0",
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
    import polars as pl
    import pyarrow.dataset as ds
    from _shared import (
        MinioConfig,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        opendal_filesystem,
    )
    from polars.testing import assert_frame_equal

    import marimo as mo

    return (
        MinioConfig,
        assert_frame_equal,
        dataset_spec,
        ds,
        ensure_minio,
        fetch_to_minio,
        mo,
        opendal_filesystem,
        pl,
    )


@app.cell
def _(mo):
    mo.md("""
    # Polars interoperability: file-like and Arrow Dataset paths

    A Polars team wants one storage configuration without giving up lazy query
    planning. We compare two supported ecosystem extension points over the same
    OpenDAL-backed object: Polars' file-like reader and its official
    `scan_pyarrow_dataset` bridge.

    [Polars cloud I/O](https://docs.pola.rs/user-guide/io/cloud-storage/) documents
    both Python file objects and PyArrow Dataset scans. The taxi records come from
    the [NYC TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
    under the NYC Open Data Terms of Use.
    """)
    return


@app.cell
def _(MinioConfig, ensure_minio, opendal_filesystem):
    minio = MinioConfig.from_env()
    ensure_minio(minio)
    polars_fs = opendal_filesystem(minio)
    assert type(polars_fs).__module__.startswith("opendalfs")
    return minio, polars_fs


@app.cell
def _(dataset_spec, fetch_to_minio, polars_fs):
    taxi_spec = dataset_spec("nyc_taxi")
    taxi_key = "raw/polars-taxi/yellow_tripdata_2025-01.parquet"
    taxi_download = fetch_to_minio(polars_fs, taxi_spec, taxi_key, timeout=120)
    assert taxi_download.size == 59_158_238
    return taxi_download, taxi_key, taxi_spec


@app.cell
def _(pl, polars_fs, taxi_key):
    business_columns = [
        "VendorID",
        "passenger_count",
        "trip_distance",
        "total_amount",
    ]
    with polars_fs.open(taxi_key, "rb") as taxi_source:
        eager_taxi = pl.read_parquet(taxi_source, columns=business_columns)
    eager_summary = (
        eager_taxi
        .filter((pl.col("passenger_count") >= 1) & (pl.col("total_amount") >= 0))
        .group_by("VendorID")
        .agg(
            pl.len().alias("trips"),
            pl.col("trip_distance").sum().alias("trip_distance"),
            pl.col("total_amount").sum().alias("gross_revenue"),
        )
        .sort("VendorID")
    )
    assert eager_taxi.height > 3_000_000
    return business_columns, eager_summary


@app.cell
def _(business_columns, ds, minio, pl, polars_fs, taxi_key):
    arrow_taxi = ds.dataset(
        f"{minio.bucket}/{taxi_key}",
        filesystem=polars_fs,
        format="parquet",
    )
    lazy_query = (
        pl
        .scan_pyarrow_dataset(arrow_taxi)
        .select(business_columns)
        .filter((pl.col("passenger_count") >= 1) & (pl.col("total_amount") >= 0))
        .group_by("VendorID")
        .agg(
            pl.len().alias("trips"),
            pl.col("trip_distance").sum().alias("trip_distance"),
            pl.col("total_amount").sum().alias("gross_revenue"),
        )
        .sort("VendorID")
    )
    lazy_plan = lazy_query.explain()
    lazy_summary = lazy_query.collect()
    assert lazy_summary.height > 0
    return lazy_plan, lazy_summary


@app.cell
def _(assert_frame_equal, eager_summary, lazy_summary):
    assert_frame_equal(
        lazy_summary,
        eager_summary,
        check_exact=False,
        rel_tol=1e-12,
    )
    return


@app.cell
def _(assert_frame_equal, lazy_summary, pl, polars_fs):
    polars_curated_key = "curated/polars-taxi/vendor-summary.parquet"
    with polars_fs.open(polars_curated_key, "wb") as destination:
        lazy_summary.write_parquet(destination)
    with polars_fs.open(polars_curated_key, "rb") as source:
        published_summary = pl.read_parquet(source)
    assert_frame_equal(published_summary, lazy_summary)
    return polars_curated_key, published_summary


@app.cell
def _(
    lazy_plan,
    minio,
    mo,
    polars_curated_key,
    published_summary,
    taxi_download,
    taxi_spec,
):
    mo.md(
        f"""
    ## Both official paths agree

    - Source: [{taxi_spec.name}]({taxi_spec.source}), {taxi_spec.license}
    - Raw object: `{minio.url(taxi_download.key, protocol="opendal+s3")}`
    - Curated object: `{minio.url(polars_curated_key, protocol="opendal+s3")}`
    - Summary rows: {published_summary.height}

    The eager file-like result and lazy PyArrow Dataset result are equal. A native
    `pl.scan_parquet("s3://...")` uses Polars' own Rust cloud implementation rather
    than Python's fsspec registry, so it is intentionally reserved for a separate,
    explicit benchmark instead of being presented as opendalfs.

    ```
    {lazy_plan}
    ```
            """
    )
    return


if __name__ == "__main__":
    app.run()
