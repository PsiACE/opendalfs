# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
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
    import csv
    import io

    import fsspec
    import pandas as pd
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
        csv,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        fsspec,
        io,
        mo,
        opendal_filesystem,
        pd,
    )


@app.cell
def _(mo):
    mo.md("""
    # Treat a GDELT ZIP archive as a filesystem

    A risk-intelligence pipeline receives compressed event exports. Instead of
    writing archive extraction and temporary-file plumbing, we compose fsspec's
    official ZIP filesystem with an opendalfs file object:
    [fsspec chained/archive features](https://filesystem-spec.readthedocs.io/en/latest/features.html) ·
    [ZIP implementation](https://filesystem-spec.readthedocs.io/en/latest/api.html).

    The [GDELT Project](https://www.gdeltproject.org/) permits unrestricted use and
    redistribution with GDELT attribution. Column meanings come from the
    [GDELT Event Codebook](https://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf).
    """)
    return


@app.cell
def _(MinioConfig, ensure_minio, opendal_filesystem):
    minio = MinioConfig.from_env()
    ensure_minio(minio)
    gdelt_fs = opendal_filesystem(minio)
    assert type(gdelt_fs).__module__.startswith("opendalfs")
    return gdelt_fs, minio


@app.cell
def _(dataset_spec, fetch_to_minio, gdelt_fs):
    gdelt_spec = dataset_spec("gdelt_events")
    gdelt_archive_key = "raw/fsspec-gdelt/2024-01-01.export.CSV.zip"
    gdelt_download = fetch_to_minio(
        gdelt_fs,
        gdelt_spec,
        gdelt_archive_key,
        timeout=120,
    )
    assert gdelt_download.size == 4_109_762
    return gdelt_archive_key, gdelt_download, gdelt_spec


@app.cell
def _(csv, fsspec, gdelt_archive_key, gdelt_fs, io, pd):
    gdelt_columns = {
        0: "GlobalEventID",
        1: "SQLDATE",
        5: "Actor1Code",
        6: "Actor1Name",
        26: "EventCode",
        29: "QuadClass",
        30: "GoldsteinScale",
        34: "AvgTone",
        51: "ActionGeo_CountryCode",
        57: "SourceURL",
    }
    with gdelt_fs.open(gdelt_archive_key, "rb") as archive_stream:
        archive_fs = fsspec.filesystem("zip", fo=archive_stream)
        try:
            archive_members = archive_fs.find("")
            assert len(archive_members) == 1
            gdelt_member = archive_members[0]
            with archive_fs.open(gdelt_member, "rb") as event_stream:
                first_row = next(
                    csv.reader(
                        io.TextIOWrapper(event_stream, encoding="utf-8"),
                        delimiter="\t",
                    )
                )
            assert len(first_row) == 58
            with archive_fs.open(gdelt_member, "rb") as event_stream:
                gdelt_events = pd.read_csv(
                    event_stream,
                    sep="\t",
                    header=None,
                    usecols=list(gdelt_columns),
                    names=list(gdelt_columns.values()),
                )
        finally:
            archive_fs.close()

    assert len(gdelt_events) > 50_000
    assert gdelt_events["GlobalEventID"].is_unique
    return archive_members, gdelt_events


@app.cell
def _(gdelt_events):
    us_events = gdelt_events.loc[
        gdelt_events["ActionGeo_CountryCode"].eq("US")
        & gdelt_events["QuadClass"].isin([3, 4])
    ].copy()
    assert 0 < len(us_events) < len(gdelt_events)
    assert set(us_events["QuadClass"].unique()).issubset({3, 4})
    return (us_events,)


@app.cell
def _(gdelt_fs, pd, us_events):
    gdelt_curated_key = "curated/fsspec-gdelt/us-conflict-events-2024-01-01.parquet"
    with gdelt_fs.open(gdelt_curated_key, "wb") as destination:
        us_events.to_parquet(destination, index=False)
    with gdelt_fs.open(gdelt_curated_key, "rb") as source:
        published_events = pd.read_parquet(source)
    pd.testing.assert_frame_equal(published_events, us_events.reset_index(drop=True))
    return gdelt_curated_key, published_events


@app.cell
def _(
    archive_members,
    gdelt_curated_key,
    gdelt_download,
    gdelt_spec,
    minio,
    mo,
    published_events,
):
    mo.md(
        f"""
    ## Archive workflow verified

    - Source: [{gdelt_spec.name}]({gdelt_spec.source}), {gdelt_spec.license}
    - Archive object: `{minio.url(gdelt_download.key, protocol="opendal+s3")}`
    - Archive members: `{archive_members}`
    - Curated object: `{minio.url(gdelt_curated_key, protocol="opendal+s3")}`
    - Published US conflict/cooperation rows: {len(published_events):,}

    ZIP requires seeking to its central directory, so this example claims
    composable archive access—not zero-buffer extraction. The published Parquet
    table is read back and compared exactly before success is reported.
            """
    )
    return


if __name__ == "__main__":
    app.run()
