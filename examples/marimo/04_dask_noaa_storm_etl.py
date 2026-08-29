# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
#     "dask[dataframe]>=2026.8.0",
#     "fsspec>=2025.5.1",
#     "marimo>=0.17.0",
#     "opendalfs",
#     "pandas>=3.0.0",
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
    import dask.dataframe as dd
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
        dd,
        ensure_minio,
        fetch_to_minio,
        mo,
        opendal_filesystem,
    )


@app.cell
def _(mo):
    mo.md("""
    # Parallel storm-event ETL with Dask

    An insurance-risk team needs one queryable table from three annual compressed
    NOAA Storm Events exports. Dask uses fsspec for remote I/O, so the same ETL can
    read and write MinIO through opendalfs:
    [remote data](https://docs.dask.org/en/stable/how-to/connect-to-remote-data.html) ·
    [Dask Parquet](https://docs.dask.org/en/stable/dataframe-parquet.html).

    NOAA/NCEI publishes the files as United States public-domain data:
    [bulk downloads](https://www.ncei.noaa.gov/stormevents/ftp.jsp) ·
    [NCEI open-data policy](https://www.ncei.noaa.gov/archive).
    """)
    return


@app.cell
def _(MinioConfig, ensure_minio, opendal_filesystem):
    minio = MinioConfig.from_env()
    ensure_minio(minio)
    storm_fs = opendal_filesystem(minio)
    assert type(storm_fs).__module__.startswith("opendalfs")
    return minio, storm_fs


@app.cell
def _(dataset_spec, fetch_to_minio, storm_fs):
    storm_inputs = []
    for storm_year in (2021, 2022, 2023):
        storm_spec = dataset_spec(f"noaa_storm_{storm_year}")
        storm_key = f"raw/dask-noaa-storm/year={storm_year}/details.csv.gz"
        storm_download = fetch_to_minio(
            storm_fs,
            storm_spec,
            storm_key,
            timeout=120,
        )
        assert storm_download.size > 10_000_000
        storm_inputs.append((storm_year, storm_spec, storm_download))
    return (storm_inputs,)


@app.cell
def _(dd, minio, storm_inputs):
    storm_urls = [
        minio.url(storm_download.key, protocol="opendal+s3")
        for _, _, storm_download in storm_inputs
    ]
    storm_columns = [
        "BEGIN_YEARMONTH",
        "STATE",
        "EVENT_TYPE",
        "INJURIES_DIRECT",
        "DEATHS_DIRECT",
    ]
    storms = dd.read_csv(
        storm_urls,
        compression="gzip",
        blocksize=None,
        storage_options=minio.opendal_options(),
        usecols=storm_columns,
        assume_missing=True,
    )
    storms = storms.assign(
        YEAR=(storms["BEGIN_YEARMONTH"] // 100).astype("int16"),
        STATE=storms["STATE"].astype("string"),
        EVENT_TYPE=storms["EVENT_TYPE"].astype("string"),
    ).drop(columns="BEGIN_YEARMONTH")
    assert storms.npartitions == 3
    return (storms,)


@app.cell
def _(storms):
    annual_risk = (
        storms
        .groupby("YEAR")
        .agg({
            "EVENT_TYPE": "count",
            "INJURIES_DIRECT": "sum",
            "DEATHS_DIRECT": "sum",
        })
        .rename(columns={"EVENT_TYPE": "EVENTS"})
        .compute()
        .sort_index()
    )
    source_rows = int(annual_risk["EVENTS"].sum())
    assert list(annual_risk.index) == [2021, 2022, 2023]
    assert source_rows > 150_000
    assert annual_risk["DEATHS_DIRECT"].sum() > 0
    return annual_risk, source_rows


@app.cell
def _(clear_prefix, minio, storm_fs, storms):
    storm_dataset_key = "curated/dask-noaa-storm"
    clear_prefix(storm_fs, storm_dataset_key)
    storm_dataset_url = minio.url(storm_dataset_key, protocol="opendal+s3")
    storms.to_parquet(
        storm_dataset_url,
        engine="pyarrow",
        write_index=False,
        partition_on=["YEAR"],
        storage_options=minio.opendal_options(),
    )
    return storm_dataset_key, storm_dataset_url


@app.cell
def _(dd, minio, source_rows, storm_dataset_url):
    published_storms = dd.read_parquet(
        storm_dataset_url,
        engine="pyarrow",
        columns=["STATE", "EVENT_TYPE", "INJURIES_DIRECT", "DEATHS_DIRECT"],
        filters=[[("YEAR", ">=", 2021), ("YEAR", "<=", 2023)]],
        storage_options=minio.opendal_options(),
    )
    published_rows = int(published_storms.map_partitions(len).sum().compute())
    assert published_rows == source_rows
    return published_rows, published_storms


@app.cell
def _(
    annual_risk,
    minio,
    mo,
    published_rows,
    published_storms,
    storm_dataset_key,
    storm_inputs,
):
    source_lines = "\n".join(
        f"- [{year}]({spec.source}): {download.size:,} compressed bytes, {spec.license}"
        for year, spec, download in storm_inputs
    )
    mo.md(
        f"""
    ## ETL verified

    {source_lines}

    - Published dataset: `{minio.url(storm_dataset_key, protocol="opendal+s3")}`
    - Published rows: {published_rows:,}
    - Read-back partitions: {published_storms.npartitions}

    Because gzip is not splittable, each annual source begins as one Dask
    partition. The Parquet publication creates independently readable partitions.

    ```
    {annual_risk.to_string()}
    ```
            """
    )
    return


if __name__ == "__main__":
    app.run()
