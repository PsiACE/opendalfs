# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
#     "dask[array]>=2025.5.1",
#     "fsspec>=2025.5.1",
#     "marimo>=0.17.0",
#     "opendalfs",
#     "numpy>=2.2.0",
#     "rechunker==0.5.4",
#     "scipy>=1.14.0",
#     "xarray>=2025.1.0",
#     "zarr==2.18.7",
# ]
# [tool.uv.sources]
# opendalfs = { path = "../..", editable = true }
# ///

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import numpy as np
    import xarray as xr
    import zarr
    from _shared import (
        MinioConfig,
        benchmark,
        clear_prefix,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        opendal_filesystem,
    )
    from rechunker.api import rechunk

    import marimo as mo

    return (
        MinioConfig,
        benchmark,
        clear_prefix,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        mo,
        np,
        opendal_filesystem,
        rechunk,
        xr,
        zarr,
    )


@app.cell
def _(mo):
    mo.md("""
    # Rechunk a climate array for a different query pattern

    Chunk layout is a workload decision. This notebook takes a public NOAA air
    temperature subset, writes a time-sliced source array, and uses the official
    [`rechunk(...).execute()`](https://rechunker.readthedocs.io/en/latest/api.html)
    workflow to build a second layout in MinIO.

    Rechunker 0.5.4 is intentionally isolated with Zarr 2.18.7. No compatibility
    adapter is used, and the source, temporary, and target stores are ordinary
    fsspec mappings returned by opendalfs.
    """)
    return


@app.cell
def _(MinioConfig, ensure_minio, opendal_filesystem, zarr):
    rechunk_minio = MinioConfig.from_env()
    ensure_minio(rechunk_minio)
    rechunk_fs = opendal_filesystem(rechunk_minio)

    assert type(rechunk_fs).__module__.startswith("opendalfs")
    assert zarr.__version__.startswith("2.18.")
    return (rechunk_fs,)


@app.cell
def _(dataset_spec, fetch_to_minio, rechunk_fs):
    rechunk_spec = dataset_spec("air_temperature")
    rechunk_prefix = "solutions/16-rechunker-layout"
    rechunk_raw_key = f"{rechunk_prefix}/raw/air_temperature.nc"
    rechunk_download = fetch_to_minio(
        rechunk_fs,
        rechunk_spec,
        rechunk_raw_key,
        timeout=120,
    )

    assert rechunk_download.size > 7_000_000
    return rechunk_download, rechunk_prefix, rechunk_raw_key, rechunk_spec


@app.cell
def _(
    clear_prefix,
    np,
    rechunk,
    rechunk_fs,
    rechunk_prefix,
    rechunk_raw_key,
    xr,
    zarr,
):
    rechunk_work_key = f"{rechunk_prefix}/work"
    clear_prefix(rechunk_fs, rechunk_work_key)

    with (
        rechunk_fs.open(rechunk_raw_key, "rb") as rechunk_stream,
        xr.open_dataset(rechunk_stream, engine="scipy") as rechunk_opened,
    ):
        rechunk_values = (
            rechunk_opened["air"]
            .isel(time=slice(0, 730))
            .load()
            .values.astype("float32")
        )

    rechunk_source_store = rechunk_fs.get_mapper(f"{rechunk_work_key}/source.zarr")
    rechunk_target_store = rechunk_fs.get_mapper(f"{rechunk_work_key}/target.zarr")
    rechunk_temp_store = rechunk_fs.get_mapper(f"{rechunk_work_key}/temp.zarr")

    rechunk_source = zarr.array(
        rechunk_values,
        chunks=(30, 25, 53),
        dtype="float32",
        store=rechunk_source_store,
        overwrite=True,
    )
    rechunk_source.attrs.update({
        "dimensions": ["time", "lat", "lon"],
        "source": "NOAA NCEP/NCAR Reanalysis subset",
    })
    rechunk_plan = rechunk(
        rechunk_source,
        target_chunks=(365, 5, 10),
        max_mem="16MB",
        target_store=rechunk_target_store,
        temp_store=rechunk_temp_store,
    )
    rechunk_plan.execute()
    rechunk_target = zarr.open_array(rechunk_target_store, mode="r")

    assert rechunk_source.chunks == (30, 25, 53)
    assert rechunk_target.chunks == (365, 5, 10)
    assert dict(rechunk_target.attrs) == dict(rechunk_source.attrs)
    np.testing.assert_allclose(rechunk_target[:], rechunk_values)
    return rechunk_source, rechunk_target, rechunk_values, rechunk_work_key


@app.cell
def _(benchmark, rechunk_source, rechunk_target):
    def source_point_series():
        return rechunk_source[:, 12, 26]

    def target_point_series():
        return rechunk_target[:, 12, 26]

    def source_spatial_window():
        return rechunk_source[0, 5:15, 10:30]

    def target_spatial_window():
        return rechunk_target[0, 5:15, 10:30]

    rechunk_timings = benchmark(
        {
            "source point series": source_point_series,
            "target point series": target_point_series,
            "source spatial window": source_spatial_window,
            "target spatial window": target_spatial_window,
        },
        repeat=3,
        seed=16,
    )
    return (rechunk_timings,)


@app.cell
def _(
    mo,
    rechunk_download,
    rechunk_source,
    rechunk_spec,
    rechunk_target,
    rechunk_timings,
    rechunk_values,
    rechunk_work_key,
):
    rechunk_rows = "\n".join(
        f"- {item.label}: median {item.median_s:.4f}s, p95 {item.p95_s:.4f}s"
        for item in rechunk_timings
    )
    mo.md(
        f"""
    ## Verified result

    - Source: [{rechunk_spec.name}]({rechunk_spec.source})
    - License: {rechunk_spec.license}
    - Download: {rechunk_download.size:,} bytes
    - Array: `{rechunk_values.shape}`, `{rechunk_values.dtype}`
    - Source chunks: `{rechunk_source.chunks}`
    - Target chunks: `{rechunk_target.chunks}`
    - Work prefix: `{rechunk_work_key}`
    - Values and attributes survived the transformation.

    These local timings demonstrate workload sensitivity rather than a universal
    winner:

    {rechunk_rows}
    """
    )
    return


if __name__ == "__main__":
    app.run()
