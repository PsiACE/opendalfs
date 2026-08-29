# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
#     "fsspec>=2025.5.1",
#     "intake==2.0.9",
#     "jinja2>=3.1",
#     "marimo>=0.17.0",
#     "opendalfs",
#     "pandas>=2.2",
# ]
# [tool.uv.sources]
# opendalfs = { path = "../..", editable = true }
# ///

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import io

    import fsspec
    import intake
    import pandas as pd
    import pandas.testing as pdt
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
        ensure_minio,
        fetch_to_minio,
        fsspec,
        intake,
        io,
        mo,
        opendal_filesystem,
        pd,
        pdt,
    )


@app.cell
def _(mo):
    mo.md("""
    # Publish a small feature catalog with Intake

    [Intake catalogs](https://intake.readthedocs.io/en/latest/catalog.html) separate
    a stable, human-readable dataset name from its physical object URL. This
    notebook adapts Intake's official YAML catalog and CSV driver pattern: publish
    a catalog beside the data in MinIO, load it through `opendal+s3://`, and compare
    its result with a direct fsspec read.

    The example uses the MIT-licensed `us-states` dataset. Its abbreviation and
    capital columns stand in for a compact reference-feature table that many jobs
    can discover through one catalog entry.
    """)
    return


@app.cell
def _(MinioConfig, ensure_minio, fsspec, opendal_filesystem):
    intake_minio = MinioConfig.from_env()
    ensure_minio(intake_minio)
    intake_fs = opendal_filesystem(intake_minio)
    fsspec.config.conf["opendal+s3"] = dict(intake_fs.storage_options)
    return intake_fs, intake_minio


@app.cell
def _(dataset_spec, fetch_to_minio, intake_fs):
    states_source = dataset_spec("us_states")
    states_key = "11_intake/catalog/states.csv"
    states_download = fetch_to_minio(intake_fs, states_source, states_key)
    catalog_key = "11_intake/catalog/catalog.yml"
    catalog_text = f"""\
    metadata:
      source: "{states_source.source}"
      license: "{states_source.license}"
      sha256: "{states_download.sha256}"
    sources:
      states:
        driver: csv
        description: US state reference features
        args:
          urlpath: '{{{{CATALOG_DIR}}}}/states.csv'
    """
    intake_fs.pipe_file(catalog_key, catalog_text.encode())
    assert intake_fs.exists(catalog_key)
    return catalog_key, states_download, states_key, states_source


@app.cell
def _(catalog_key, intake, intake_minio):
    catalog_url = intake_minio.url(catalog_key, protocol="opendal+s3")
    states_catalog = intake.open_catalog(catalog_url)
    catalog_frame = states_catalog.states.read()

    assert list(states_catalog) == ["states"]
    return catalog_frame, catalog_url


@app.cell
def _(catalog_frame, intake_fs, io, pd, pdt, states_key):
    direct_frame = pd.read_csv(io.BytesIO(intake_fs.cat_file(states_key)))
    pdt.assert_frame_equal(catalog_frame, direct_frame)

    normalized_columns = {column.lower() for column in catalog_frame.columns}
    assert len(catalog_frame) >= 50
    assert any("state" in column for column in normalized_columns)
    assert any("abbrev" in column or "code" in column for column in normalized_columns)
    return (normalized_columns,)


@app.cell
def _(
    catalog_frame,
    catalog_url,
    mo,
    normalized_columns,
    states_download,
    states_source,
):
    mo.md(
        f"""
    ## Verified catalog contract

    - Dataset source: [{states_source.source}]({states_source.source})
    - License: **{states_source.license}**
    - Catalog: `{catalog_url}`
    - CSV SHA-256: `{states_download.sha256}`
    - Rows: {len(catalog_frame)}
    - Columns: `{sorted(normalized_columns)}`

    The catalog read and a direct `fs.cat_file()` + pandas read are exactly equal.
    Consumers therefore gain discovery metadata and a stable logical name without
    a custom storage adapter or a copied parsing implementation.
    """
    )
    return


if __name__ == "__main__":
    app.run()
