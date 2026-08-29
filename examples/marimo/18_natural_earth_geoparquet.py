# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
#     "fsspec>=2025.5.1",
#     "geopandas>=1.1.0,<1.2",
#     "marimo>=0.17.0",
#     "opendalfs",
#     "pyarrow>=22.0.0",
#     "pyogrio>=0.11.0",
# ]
# [tool.uv.sources]
# opendalfs = { path = "../..", editable = true }
# ///

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import geopandas as gpd
    import pyarrow.fs as pafs
    from _shared import (
        MinioConfig,
        benchmark,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        opendal_filesystem,
        opened_local_copy,
    )

    import marimo as mo

    return (
        MinioConfig,
        benchmark,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        gpd,
        mo,
        opendal_filesystem,
        opened_local_copy,
        pafs,
    )


@app.cell
def _(mo):
    mo.md("""
    # Natural Earth to GeoParquet with bounding-box filtering

    This solution converts a traditional zipped Shapefile into GeoParquet 1.1 in
    MinIO. It uses GeoPandas'
    [`write_covering_bbox`](https://geopandas.org/en/v1.1.0/docs/reference/api/geopandas.GeoDataFrame.to_parquet.html)
    and [`read_parquet(..., bbox=...)`](https://geopandas.org/en/latest/docs/reference/api/geopandas.read_parquet.html)
    APIs. Apache Arrow receives opendalfs through its official `FSSpecHandler`.

    Natural Earth releases all raster and vector map data into the public domain.
    The source archive and its provenance manifest remain beside the curated file.
    """)
    return


@app.cell
def _(MinioConfig, ensure_minio, opendal_filesystem, pafs):
    geo_minio = MinioConfig.from_env()
    ensure_minio(geo_minio)
    geo_fs = opendal_filesystem(geo_minio)
    geo_arrow_fs = pafs.PyFileSystem(pafs.FSSpecHandler(geo_fs))

    assert type(geo_fs).__module__.startswith("opendalfs")
    return geo_arrow_fs, geo_fs


@app.cell
def _(dataset_spec, fetch_to_minio, geo_fs):
    geo_spec = dataset_spec("natural_earth_countries")
    geo_prefix = "solutions/18-natural-earth-geoparquet"
    geo_raw_key = f"{geo_prefix}/raw/ne_110m_admin_0_countries.zip"
    geo_download = fetch_to_minio(
        geo_fs,
        geo_spec,
        geo_raw_key,
        timeout=120,
    )

    assert geo_download.size > 200_000
    return geo_download, geo_prefix, geo_raw_key, geo_spec


@app.cell
def _(geo_fs, geo_raw_key, gpd, opened_local_copy):
    with opened_local_copy(geo_fs, geo_raw_key) as geo_local_zip:
        geo_source = gpd.read_file(f"zip://{geo_local_zip}", engine="pyogrio")

    geo_countries = geo_source[["ADMIN", "CONTINENT", "POP_EST", "geometry"]].rename(
        columns={
            "ADMIN": "name",
            "CONTINENT": "continent",
            "POP_EST": "population",
        }
    )

    assert len(geo_countries) > 150
    assert geo_countries.crs is not None
    assert geo_countries.geometry.notna().all()
    return (geo_countries,)


@app.cell
def _(geo_arrow_fs, geo_countries, geo_prefix, gpd):
    geo_parquet_key = f"{geo_prefix}/curated/countries.parquet"
    geo_countries.to_parquet(
        geo_parquet_key,
        filesystem=geo_arrow_fs,
        index=False,
        schema_version="1.1.0",
        write_covering_bbox=True,
    )

    geo_full = gpd.read_parquet(
        geo_parquet_key,
        filesystem=geo_arrow_fs,
    )
    geo_europe_bbox = (-10.0, 35.0, 30.0, 60.0)
    geo_europe = gpd.read_parquet(
        geo_parquet_key,
        columns=["name", "continent", "population", "geometry"],
        bbox=geo_europe_bbox,
        filesystem=geo_arrow_fs,
    )

    assert len(geo_full) == len(geo_countries)
    assert set(geo_full["name"]) == set(geo_countries["name"])
    assert 0 < len(geo_europe) < len(geo_full)
    assert geo_europe.crs == geo_full.crs

    geo_bounds = geo_europe.geometry.bounds
    geo_minx, geo_miny, geo_maxx, geo_maxy = geo_europe_bbox
    assert (
        (geo_bounds.maxx >= geo_minx)
        & (geo_bounds.minx <= geo_maxx)
        & (geo_bounds.maxy >= geo_miny)
        & (geo_bounds.miny <= geo_maxy)
    ).all()
    return geo_europe, geo_europe_bbox, geo_full, geo_parquet_key


@app.cell
def _(benchmark, geo_arrow_fs, geo_europe_bbox, geo_parquet_key, gpd):
    def read_all_countries():
        return gpd.read_parquet(geo_parquet_key, filesystem=geo_arrow_fs)

    def read_europe_bbox():
        return gpd.read_parquet(
            geo_parquet_key,
            columns=["name", "continent", "population", "geometry"],
            bbox=geo_europe_bbox,
            filesystem=geo_arrow_fs,
        )

    geo_timings = benchmark(
        {
            "full GeoParquet": read_all_countries,
            "Europe bbox": read_europe_bbox,
        },
        repeat=3,
        seed=18,
    )
    return (geo_timings,)


@app.cell
def _(
    geo_download,
    geo_europe,
    geo_fs,
    geo_full,
    geo_parquet_key,
    geo_spec,
    geo_timings,
    mo,
):
    geo_parquet_size = geo_fs.info(geo_parquet_key)["size"]
    geo_rows = "\n".join(
        f"- {item.label}: median {item.median_s:.4f}s, p95 {item.p95_s:.4f}s"
        for item in geo_timings
    )
    mo.md(
        f"""
    ## Verified result

    - Source: [{geo_spec.name}]({geo_spec.source})
    - License: {geo_spec.license}
    - Source ZIP: {geo_download.size:,} bytes
    - GeoParquet: {geo_parquet_size:,} bytes
    - Full countries: {len(geo_full)}
    - Europe bbox candidates: {len(geo_europe)}
    - CRS, row identity, geometry presence, and bbox overlap were verified.

    Local observations:

    {geo_rows}
    """
    )
    return


if __name__ == "__main__":
    app.run()
