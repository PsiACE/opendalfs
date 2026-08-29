# Tutorials

These tutorials teach complete data workflows with fsspec and opendalfs. Each
page is generated from an executable marimo notebook, so the code shown here
and the code tested against the repository's MinIO service have one source.

## Before you begin

Clone the repository, start MinIO, and run any tutorial from the repository
root:

```console
podman compose up -d --wait
uv run --script examples/marimo/01_pandas_penguins_landing.py
```

To work through the same lesson interactively, open its notebook in marimo:

```console
uvx marimo edit --sandbox examples/marimo/01_pandas_penguins_landing.py
```

Every tutorial records its public data source and license, writes immutable
inputs below `raw/`, verifies its result, and supports safe repeated execution.
The performance tutorials verify equal results before measuring warmed,
randomized runs. Their loopback MinIO timings are observations, not universal
storage rankings.

## Foundations

```{toctree}
:maxdepth: 1

One object, three fsspec entry points <generated/00_minio_protocols>
```

## Data engineering

```{toctree}
:maxdepth: 1

Land and validate Palmer Penguins <generated/01_pandas_penguins_landing>
Build a partitioned Arrow taxi lake <generated/02_arrow_taxi_hive_dataset>
Query taxi data with DuckDB <generated/03_duckdb_taxi_sql>
Aggregate NOAA storms with Dask <generated/04_dask_noaa_storm_etl>
Read a GDELT ZIP archive through fsspec <generated/05_fsspec_gdelt_archive>
Use Polars with an Arrow dataset <generated/06_polars_taxi_interop>
```

## Machine learning and workflows

```{toctree}
:maxdepth: 1

Mirror a Hugging Face dataset <generated/07_huggingface_dataset_mirror>
Store a Lightning checkpoint remotely <generated/08_lightning_remote_checkpoint>
Build an Airflow object-storage ETL <generated/09_airflow_object_storage_etl>
Persist a Prefect result cache <generated/10_prefect_result_cache>
Publish an Intake feature catalog <generated/11_intake_feature_catalog>
Run Ray Data batch inference <generated/12_ray_batch_inference>
Exercise the DVC Objects contract <generated/13_dvc_objects_contract>
```

## Scientific and geospatial data

```{toctree}
:maxdepth: 1

Analyze climate data with Xarray and Zarr <generated/14_xarray_zarr_climate>
Virtualize an NWM file with Kerchunk <generated/15_kerchunk_nwm_virtual_dataset>
Rechunk an array for a query pattern <generated/16_rechunker_query_layout>
Read Sentinel COG windows with Rasterio <generated/17_sentinel_cog_windows>
Filter Natural Earth GeoParquet <generated/18_natural_earth_geoparquet>
```

## Performance

```{toctree}
:maxdepth: 1

Compare three equal-result S3 readers <generated/19_s3_reader_benchmark>
```

## Reuse the generated Markdown

Generate all twenty Markdown pages without building the site:

```console
just tutorials-export
```

The command writes standard fenced Markdown to
`docs/user-guide/tutorials/generated/`. The directory is generated and ignored
by Git; edit the marimo source under `examples/marimo/` instead. Sphinx runs the
same export automatically before discovering documentation pages.
