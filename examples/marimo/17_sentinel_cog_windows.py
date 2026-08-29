# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
#     "fsspec>=2025.5.1",
#     "marimo>=0.17.0",
#     "opendalfs",
#     "numpy>=2.2.0",
#     "rasterio>=1.4.0,<1.6",
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
    import rasterio
    from _shared import (
        MinioConfig,
        benchmark,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        opendal_filesystem,
    )
    from rasterio.io import MemoryFile
    from rasterio.windows import Window

    import marimo as mo

    return (
        MemoryFile,
        MinioConfig,
        Window,
        benchmark,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        mo,
        np,
        opendal_filesystem,
        rasterio,
    )


@app.cell
def _(mo):
    mo.md("""
    # Windowed reads from a Sentinel-2 COG

    A Cloud Optimized GeoTIFF arranges imagery in blocks and overviews so readers
    can request only the required ranges. Rasterio 1.4 added an official
    [fsspec-like `opener`](https://rasterio.readthedocs.io/en/stable/topics/vsi.html),
    which lets GDAL use opendalfs without copying the object into a `MemoryFile`.
    The window follows Rasterio's
    [windowed-read guidance](https://rasterio.readthedocs.io/en/stable/topics/windowed-rw.html).

    The red band is from the open Sentinel-2 COG archive. Copernicus Sentinel data
    are available on a free, full, and open basis; redistributed or modified data
    must retain the applicable Copernicus notice.
    """)
    return


@app.cell
def _(MinioConfig, ensure_minio, opendal_filesystem):
    cog_minio = MinioConfig.from_env()
    ensure_minio(cog_minio)
    cog_fs = opendal_filesystem(cog_minio)

    assert type(cog_fs).__module__.startswith("opendalfs")
    return (cog_fs,)


@app.cell
def _(cog_fs, dataset_spec, fetch_to_minio):
    cog_spec = dataset_spec("sentinel_red")
    cog_prefix = "solutions/17-sentinel-cog"
    cog_key = f"{cog_prefix}/imagery/B04.tif"
    cog_download = fetch_to_minio(cog_fs, cog_spec, cog_key, timeout=120)

    assert cog_download.size > 3_000_000
    return cog_download, cog_key, cog_spec


@app.cell
def _(Window, cog_fs, cog_key, rasterio):
    with rasterio.open(cog_key, opener=cog_fs) as cog_dataset:
        cog_block_height, cog_block_width = cog_dataset.block_shapes[0]
        cog_window = Window(
            col_off=cog_dataset.width // 2,
            row_off=cog_dataset.height // 2,
            width=min(cog_block_width, cog_dataset.width // 2),
            height=min(cog_block_height, cog_dataset.height // 2),
        )
        cog_window_pixels = cog_dataset.read(1, window=cog_window)
        cog_profile = {
            "shape": cog_dataset.shape,
            "dtype": cog_dataset.dtypes[0],
            "crs": str(cog_dataset.crs),
            "block_shape": cog_dataset.block_shapes[0],
            "overviews": cog_dataset.overviews(1),
            "is_tiled": cog_dataset.is_tiled,
        }

    assert cog_profile["shape"][0] > 1_000
    assert cog_profile["shape"][1] > 1_000
    assert cog_profile["crs"] != "None"
    assert cog_profile["is_tiled"]
    assert cog_window_pixels.size > 0
    return cog_profile, cog_window, cog_window_pixels


@app.cell
def _(MemoryFile, cog_fs, cog_key, cog_window, cog_window_pixels, np):
    cog_payload = cog_fs.cat_file(cog_key)
    with MemoryFile(cog_payload) as cog_memory, cog_memory.open() as cog_materialized:
        cog_materialized_pixels = cog_materialized.read(1, window=cog_window)

    np.testing.assert_array_equal(cog_window_pixels, cog_materialized_pixels)
    assert len(cog_payload) == cog_fs.info(cog_key)["size"]
    return (cog_payload,)


@app.cell
def _(MemoryFile, benchmark, cog_fs, cog_key, cog_window, rasterio):
    def read_cog_window():
        with rasterio.open(cog_key, opener=cog_fs) as windowed_dataset:
            return windowed_dataset.read(1, window=cog_window)

    def materialize_then_read():
        payload = cog_fs.cat_file(cog_key)
        with (
            MemoryFile(payload) as memory_file,
            memory_file.open() as materialized_dataset,
        ):
            return materialized_dataset.read(1, window=cog_window)

    cog_timings = benchmark(
        {
            "opendalfs opener window": read_cog_window,
            "full materialization": materialize_then_read,
        },
        repeat=3,
        seed=17,
    )
    return (cog_timings,)


@app.cell
def _(
    cog_download,
    cog_payload,
    cog_profile,
    cog_spec,
    cog_timings,
    cog_window_pixels,
    mo,
):
    cog_rows = "\n".join(
        f"- {item.label}: median {item.median_s:.4f}s, p95 {item.p95_s:.4f}s"
        for item in cog_timings
    )
    mo.md(
        f"""
    ## Verified result

    - Source: [{cog_spec.name}]({cog_spec.source})
    - License: {cog_spec.license}
    - Object size: {cog_download.size:,} bytes
    - Raster shape: `{cog_profile["shape"]}` in `{cog_profile["crs"]}`
    - Internal block: `{cog_profile["block_shape"]}`
    - Overviews: `{cog_profile["overviews"]}`
    - Window result: `{cog_window_pixels.shape}`, `{cog_window_pixels.dtype}`
    - The opener and full `{len(cog_payload):,}`-byte materialization returned
      identical pixels.

    Local observations:

    {cog_rows}
    """
    )
    return


if __name__ == "__main__":
    app.run()
