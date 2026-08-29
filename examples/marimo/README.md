# Executable solution notebooks

These marimo notebooks teach one end-to-end storage problem at a time. They
use public data, the repository's MinIO Compose service, and downstream
libraries' documented fsspec extension points.

## Start MinIO

Docker and Podman are both supported:

```console
docker compose up -d --wait
# or
podman compose up -d --wait
```

The defaults match `docker-compose.yml`. Override them with the existing
`OPENDAL_S3_*` environment variables.

## Run a notebook

Each notebook is a Python script with PEP 723 dependencies:

```console
uv run --script examples/marimo/00_minio_protocols.py
```

Open the reactive editor with the same inline dependencies and editable checkout:

```console
uvx marimo edit --sandbox examples/marimo/00_minio_protocols.py
```

## Solution catalog

| No. | Workflow | Downstream integration |
| --- | --- | --- |
| 00 | MinIO protocol routing | fsspec native, explicit, and gateway protocols |
| 01 | Penguins landing zone | pandas and Parquet |
| 02 | Partitioned taxi lake | Arrow Dataset and Hive partitions |
| 03 | Taxi analytics | DuckDB `register_filesystem` |
| 04 | Multi-year storm ETL | Dask DataFrame |
| 05 | Compressed event archive | fsspec ZIP filesystem |
| 06 | Lazy taxi aggregation | Polars and Arrow Dataset |
| 07 | Dataset mirror | Hugging Face Datasets |
| 08 | Remote model checkpoint | Lightning |
| 09 | Object-storage ETL | Airflow Task SDK |
| 10 | Durable result cache | Prefect |
| 11 | Feature catalog | Intake |
| 12 | Batch inference | Ray Data and Arrow filesystem bridge |
| 13 | Content-addressed objects | DVC Objects contract |
| 14 | Chunked climate analysis | Xarray and Zarr 3 |
| 15 | Virtual hydrology dataset | Kerchunk |
| 16 | Query-oriented layout | Rechunker and Zarr 2 |
| 17 | Satellite window reads | Rasterio COG opener |
| 18 | Spatially filtered vectors | GeoPandas and GeoParquet |
| 19 | Equal-result S3 reads | opendalfs, s3fs, and Arrow native S3 |

## Notebook contract

Every solution records its data source and license, lands immutable source
data under `raw/`, uses a notebook-specific output prefix, verifies its result,
and can be run again safely. Performance notebooks verify equal results before
timing, warm up every implementation, randomize execution order, and retain
raw samples instead of claiming a universal winner.

Shared code is intentionally small. `_shared.py` owns MinIO configuration,
bucket creation, source download manifests, checksums, and benchmark sampling.
Storage and data-library behavior remains in the official downstream APIs.
