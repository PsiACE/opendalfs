# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
#     "dask[array]>=2025.5.1",
#     "fsspec>=2025.5.1",
#     "marimo>=0.17.0",
#     "opendalfs",
#     "numpy>=2.2.0",
#     "scipy>=1.14.0",
#     "xarray==2026.7.0",
#     "zarr==3.3.0",
# ]
# [tool.uv.sources]
# opendalfs = { path = "../..", editable = true }
# ///

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
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

    import marimo as mo

    return (
        MinioConfig,
        benchmark,
        clear_prefix,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        mo,
        opendal_filesystem,
        xr,
        zarr,
    )


@app.cell
def _(mo):
    mo.md("""
    # Climate data: NetCDF to cloud-native Zarr

    This solution migrates an NCEP/NCAR air-temperature subset from one NetCDF
    file to a chunked Zarr hierarchy in the repository's MinIO service. It follows
    the official [Xarray Zarr I/O guide](https://docs.xarray.dev/en/stable/user-guide/io.html)
    and Zarr's [`FsspecStore`](https://zarr.readthedocs.io/en/stable/api/zarr/storage/)
    extension point instead of adding a storage adapter.

    The source is the Xarray tutorial copy of NOAA NCEP/NCAR Reanalysis data. NOAA
    data are in the United States public domain; scientific use should cite NOAA
    PSL. The workflow keeps source provenance beside the object in MinIO.
    """)
    return


@app.cell
def _(MinioConfig, ensure_minio, opendal_filesystem):
    climate_minio = MinioConfig.from_env()
    ensure_minio(climate_minio)
    climate_fs = opendal_filesystem(climate_minio)

    assert type(climate_fs).__module__.startswith("opendalfs")
    return climate_fs, climate_minio


@app.cell
def _(climate_fs, dataset_spec, fetch_to_minio):
    climate_spec = dataset_spec("air_temperature")
    climate_prefix = "solutions/14-xarray-zarr-climate"
    climate_raw_key = f"{climate_prefix}/raw/air_temperature.nc"
    climate_download = fetch_to_minio(
        climate_fs,
        climate_spec,
        climate_raw_key,
        timeout=120,
    )

    assert climate_download.size > 7_000_000
    assert len(climate_download.sha256) == 64
    return climate_download, climate_prefix, climate_raw_key, climate_spec


@app.cell
def _(
    clear_prefix,
    climate_fs,
    climate_minio,
    climate_prefix,
    climate_raw_key,
    xr,
    zarr,
):
    climate_zarr_key = f"{climate_prefix}/curated/air_temperature.zarr"
    clear_prefix(climate_fs, climate_zarr_key)

    with (
        climate_fs.open(climate_raw_key, "rb") as climate_stream,
        xr.open_dataset(climate_stream, engine="scipy") as climate_opened,
    ):
        # NetCDF packing metadata (int16 + scale_factor) describes the source
        # encoding, not the decoded floating-point array written to Zarr.
        climate_source = climate_opened.load().drop_encoding()

    assert climate_source.sizes == {"lat": 25, "time": 2920, "lon": 53}
    assert climate_source["air"].attrs["units"] == "degK"

    climate_store = zarr.storage.FsspecStore.from_url(
        climate_minio.url(climate_zarr_key, protocol="opendal+s3"),
        storage_options=climate_minio.opendal_options(),
    )
    climate_chunked = climate_source.chunk({"time": 365, "lat": 25, "lon": 53})
    climate_chunked.to_zarr(
        climate_store,
        mode="w",
        consolidated=False,
    )

    climate_reopened = xr.open_zarr(climate_store, consolidated=False)
    climate_expected = climate_source["air"].isel(time=slice(0, 30)).mean("time")
    climate_actual = climate_reopened["air"].isel(time=slice(0, 30)).mean("time").load()
    xr.testing.assert_allclose(climate_actual, climate_expected)

    assert climate_reopened["air"].chunks is not None
    assert climate_reopened["air"].chunks[0][0] == 365
    return climate_reopened, climate_store, climate_zarr_key


@app.cell
def _(benchmark, climate_fs, climate_raw_key, climate_store, xr):
    def read_netcdf_region():
        with (
            climate_fs.open(climate_raw_key, "rb") as benchmark_stream,
            xr.open_dataset(benchmark_stream, engine="scipy") as benchmark_ds,
        ):
            return benchmark_ds["air"].isel(time=slice(0, 30)).mean("time").load()

    def read_zarr_region():
        benchmark_ds = xr.open_zarr(climate_store, consolidated=False)
        try:
            return benchmark_ds["air"].isel(time=slice(0, 30)).mean("time").load()
        finally:
            benchmark_ds.close()

    climate_timings = benchmark(
        {
            "NetCDF region": read_netcdf_region,
            "Zarr region": read_zarr_region,
        },
        repeat=3,
        seed=14,
    )
    return (climate_timings,)


@app.cell
def _(
    climate_download,
    climate_reopened,
    climate_spec,
    climate_timings,
    climate_zarr_key,
    mo,
):
    climate_rows = "\n".join(
        f"- {item.label}: median {item.median_s:.4f}s, p95 {item.p95_s:.4f}s"
        for item in climate_timings
    )
    mo.md(
        f"""
    ## Verified result

    - Source: [{climate_spec.name}]({climate_spec.source})
    - License: {climate_spec.license}
    - NetCDF bytes: {climate_download.size:,}
    - Zarr prefix: `{climate_zarr_key}`
    - Zarr chunks: `{climate_reopened["air"].chunks}`
    - The first 30 time steps produce the same regional mean in both formats.

    The timings below are observations from one local MinIO instance, not general
    backend performance claims:

    {climate_rows}
    """
    )
    return


if __name__ == "__main__":
    app.run()
