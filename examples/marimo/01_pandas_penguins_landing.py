# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
#     "fsspec>=2025.5.1",
#     "marimo>=0.17.0",
#     "opendalfs",
#     "pandas>=3.0.0",
#     "pyarrow>=22.0.0",
#     "tabulate>=0.9.0",
# ]
# [tool.uv.sources]
# opendalfs = { path = "../..", editable = true }
# ///

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():

    import pandas as pd
    from _shared import (
        MinioConfig,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        opendal_filesystem,
        sha256_file,
    )

    import marimo as mo

    return (
        MinioConfig,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        mo,
        opendal_filesystem,
        pd,
        sha256_file,
    )


@app.cell
def _(mo):
    mo.md("""
    # From a public CSV to a governed Parquet object

    A field-science team receives a small public CSV and needs a repeatable landing
    pipeline: retain the source bytes, validate the table, publish typed Parquet,
    and prove that the published object can be read back.

    This follows pandas' official file-like I/O pattern. The Palmer Penguins data
    is published under CC0 by Allison Horst, Alison Hill, and Kristen Gorman:
    [dataset](https://github.com/allisonhorst/palmerpenguins) ·
    [license](https://allisonhorst.github.io/palmerpenguins/LICENSE.html) ·
    [pandas `read_csv`](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html).
    """)
    return


@app.cell
def _(MinioConfig, ensure_minio, opendal_filesystem):
    minio = MinioConfig.from_env()
    ensure_minio(minio)
    penguins_fs = opendal_filesystem(minio)
    assert type(penguins_fs).__module__.startswith("opendalfs")
    return minio, penguins_fs


@app.cell
def _(dataset_spec, fetch_to_minio, penguins_fs):
    penguins_spec = dataset_spec("penguins")
    penguins_raw_key = "raw/pandas-penguins/penguins.csv"
    penguins_download = fetch_to_minio(
        penguins_fs,
        penguins_spec,
        penguins_raw_key,
    )
    assert penguins_download.size == 15_241
    assert len(penguins_download.sha256) == 64
    return penguins_download, penguins_raw_key, penguins_spec


@app.cell
def _(pd, penguins_fs, penguins_raw_key):
    with penguins_fs.open(penguins_raw_key, "rb") as penguins_source:
        penguins = pd.read_csv(
            penguins_source,
            dtype={"species": "string", "island": "string", "sex": "string"},
        )

    assert len(penguins) == 344
    assert set(penguins["species"].dropna()) == {"Adelie", "Chinstrap", "Gentoo"}
    assert penguins["body_mass_g"].notna().sum() == 342

    species_summary = (
        penguins
        .groupby("species", observed=True)
        .agg(
            observations=("species", "size"),
            mean_body_mass_g=("body_mass_g", "mean"),
        )
        .reset_index()
    )
    return penguins, species_summary


@app.cell
def _(penguins, penguins_fs, sha256_file):
    penguins_parquet_key = "curated/pandas-penguins/penguins.parquet"
    with penguins_fs.open(penguins_parquet_key, "wb") as parquet_destination:
        penguins.to_parquet(parquet_destination, index=False)

    parquet_sha256, parquet_size = sha256_file(
        penguins_fs,
        penguins_parquet_key,
    )
    assert parquet_size > 1_000
    return parquet_sha256, parquet_size, penguins_parquet_key


@app.cell
def _(pd, penguins, penguins_fs, penguins_parquet_key):
    with penguins_fs.open(penguins_parquet_key, "rb") as parquet_source:
        published_penguins = pd.read_parquet(parquet_source)

    pd.testing.assert_frame_equal(published_penguins, penguins)
    return


@app.cell
def _(
    minio,
    mo,
    parquet_sha256,
    parquet_size,
    penguins_download,
    penguins_parquet_key,
    penguins_spec,
    species_summary,
):
    mo.md(
        f"""
    ## Published and verified

    - Source: [{penguins_spec.name}]({penguins_spec.source}), {penguins_spec.license}
    - Raw object: `{minio.url(penguins_download.key, protocol="opendal+s3")}`
    - Curated object: `{minio.url(penguins_parquet_key, protocol="opendal+s3")}`
    - Curated size: {parquet_size:,} bytes
    - Curated SHA-256: `{parquet_sha256}`

    The read-back dataframe is exactly equal to the validated 344-row input.
    The business summary below is computed before publication and remains available
    for downstream checks.

    {species_summary.to_markdown(index=False)}
            """
    )
    return


if __name__ == "__main__":
    app.run()
