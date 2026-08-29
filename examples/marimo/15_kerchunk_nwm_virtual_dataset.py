# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
#     "fsspec>=2025.5.1",
#     "h5netcdf>=1.6.4",
#     "kerchunk[hdf]==0.2.10",
#     "marimo>=0.17.0",
#     "opendalfs",
#     "numpy>=2.2.0",
#     "xarray>=2025.1.0",
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
    from _shared import (
        MinioConfig,
        benchmark,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        opendal_filesystem,
        write_json,
    )
    from kerchunk.hdf import SingleHdf5ToZarr
    from kerchunk.utils import refs_as_store

    import marimo as mo

    return (
        MinioConfig,
        SingleHdf5ToZarr,
        benchmark,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        mo,
        np,
        opendal_filesystem,
        refs_as_store,
        write_json,
        xr,
    )


@app.cell
def _(mo):
    mo.md("""
    # Virtualize a NOAA water-model file with Kerchunk

    Kerchunk records the byte ranges and codecs of an existing HDF5/NetCDF file so
    that Xarray can read it as a virtual Zarr dataset without rewriting its data.
    This notebook adapts the exact NOAA National Water Model path used by the
    [Kerchunk quick start](https://fsspec.github.io/kerchunk/test_example.html)
    and uses the documented
    [`SingleHdf5ToZarr`](https://fsspec.github.io/kerchunk/reference.html) API.

    The NOAA NWM retrospective archive is public-domain open data with no use
    restrictions. Only the compact reference JSON is newly produced.
    """)
    return


@app.cell
def _(MinioConfig, ensure_minio, opendal_filesystem):
    nwm_minio = MinioConfig.from_env()
    ensure_minio(nwm_minio)
    nwm_fs = opendal_filesystem(nwm_minio)

    assert type(nwm_fs).__module__.startswith("opendalfs")
    return (nwm_fs,)


@app.cell
def _(dataset_spec, fetch_to_minio, nwm_fs):
    nwm_spec = dataset_spec("nwm")
    nwm_prefix = "solutions/15-kerchunk-nwm"
    nwm_raw_key = f"{nwm_prefix}/raw/201704010000.CHRTOUT_DOMAIN1.comp"
    nwm_download = fetch_to_minio(
        nwm_fs,
        nwm_spec,
        nwm_raw_key,
        timeout=180,
    )

    assert nwm_download.size > 40_000_000
    return nwm_download, nwm_prefix, nwm_raw_key, nwm_spec


@app.cell
def _(SingleHdf5ToZarr, nwm_fs, nwm_prefix, nwm_raw_key, write_json):
    nwm_url = nwm_fs.unstrip_protocol(nwm_raw_key)
    nwm_source_stream = nwm_fs.open(nwm_raw_key, "rb")
    nwm_translator = SingleHdf5ToZarr(
        nwm_source_stream,
        nwm_url,
        inline_threshold=300,
    )
    try:
        nwm_references = nwm_translator.translate()
    finally:
        nwm_translator.close()

    nwm_reference_key = f"{nwm_prefix}/references/nwm.json"
    write_json(nwm_fs, nwm_reference_key, nwm_references)

    assert nwm_references["version"] == 1
    assert len(nwm_references["refs"]) > 50
    return nwm_reference_key, nwm_references


@app.cell
def _(np, nwm_fs, nwm_raw_key, nwm_references, refs_as_store, xr):
    nwm_reference_store = refs_as_store(nwm_references, fs=nwm_fs)
    nwm_virtual = xr.open_dataset(
        nwm_reference_store,
        engine="zarr",
        zarr_format=2,
        decode_cf=False,
        backend_kwargs={"consolidated": False},
    )
    nwm_numeric_variables = [
        name
        for name, variable in nwm_virtual.data_vars.items()
        if variable.size > 0 and np.issubdtype(variable.dtype, np.number)
    ]
    assert nwm_numeric_variables
    nwm_variable = max(
        nwm_numeric_variables,
        key=lambda name: nwm_virtual[name].size,
    )
    nwm_indexers = dict.fromkeys(nwm_virtual[nwm_variable].dims, 0)
    nwm_virtual_value = nwm_virtual[nwm_variable].isel(nwm_indexers).load()

    with (
        nwm_fs.open(nwm_raw_key, "rb") as nwm_direct_stream,
        xr.open_dataset(
            nwm_direct_stream,
            engine="h5netcdf",
            decode_cf=False,
        ) as nwm_direct,
    ):
        nwm_direct_value = nwm_direct[nwm_variable].isel(nwm_indexers).load()

    xr.testing.assert_allclose(nwm_virtual_value, nwm_direct_value)
    return nwm_indexers, nwm_reference_store, nwm_variable


@app.cell
def _(
    benchmark,
    nwm_fs,
    nwm_indexers,
    nwm_raw_key,
    nwm_reference_store,
    nwm_variable,
    xr,
):
    def read_nwm_direct():
        with (
            nwm_fs.open(nwm_raw_key, "rb") as direct_stream,
            xr.open_dataset(
                direct_stream,
                engine="h5netcdf",
                decode_cf=False,
            ) as direct_ds,
        ):
            return direct_ds[nwm_variable].isel(nwm_indexers).load()

    def read_nwm_virtual():
        virtual_ds = xr.open_dataset(
            nwm_reference_store,
            engine="zarr",
            zarr_format=2,
            decode_cf=False,
            backend_kwargs={"consolidated": False},
        )
        try:
            return virtual_ds[nwm_variable].isel(nwm_indexers).load()
        finally:
            virtual_ds.close()

    nwm_timings = benchmark(
        {
            "Direct HDF5 scalar": read_nwm_direct,
            "Kerchunk scalar": read_nwm_virtual,
        },
        repeat=2,
        seed=15,
    )
    return (nwm_timings,)


@app.cell
def _(
    mo,
    nwm_download,
    nwm_fs,
    nwm_reference_key,
    nwm_references,
    nwm_spec,
    nwm_timings,
    nwm_variable,
):
    nwm_reference_size = nwm_fs.info(nwm_reference_key)["size"]
    nwm_rows = "\n".join(
        f"- {item.label}: median {item.median_s:.4f}s, p95 {item.p95_s:.4f}s"
        for item in nwm_timings
    )
    mo.md(
        f"""
    ## Verified result

    - Source: [{nwm_spec.name}]({nwm_spec.source})
    - License: {nwm_spec.license}
    - Physical object: {nwm_download.size:,} bytes
    - Reference object: {nwm_reference_size:,} bytes
    - Reference entries: {len(nwm_references["refs"]):,}
    - Verified variable: `{nwm_variable}`
    - Direct HDF5 and virtual-Zarr scalar values are equal.

    Local observations:

    {nwm_rows}
    """
    )
    return


if __name__ == "__main__":
    app.run()
