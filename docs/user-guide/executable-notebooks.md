# Run the solution notebooks

The repository includes twenty executable marimo notebooks. Each notebook
starts with a real storage problem, obtains a public dataset, lands the source
in the repository's MinIO service, uses a downstream library through its
documented filesystem extension point, and verifies the result.

## Start the object store

Run the Compose file from the repository root with Docker or Podman:

```console
docker compose up -d --wait
# or
podman compose up -d --wait
```

The defaults use `http://127.0.0.1:9000`, the credentials `minioadmin`, and the
bucket `opendalfs-examples`. The existing `OPENDAL_S3_*` environment variables
override those values.

## Execute a notebook

Each notebook is a Python script with PEP 723 dependency metadata. `uv --script`
creates an isolated environment and installs the current checkout as an editable
local source:

```console
uv run --script \
  examples/marimo/01_pandas_penguins_landing.py
```

Open the same file in marimo's reactive editor with its PEP 723 sandbox:

```console
uvx marimo edit --sandbox examples/marimo/01_pandas_penguins_landing.py
```

Run the structural checks without executing the data workloads:

```console
uv run --group notebooks marimo check examples/marimo/[0-9]*.py
```

The `notebook` and `notebooks-test` recipes in the root `justfile` provide the
same commands for one notebook or the entire collection.

## Choose a solution

| Range | Area | Included workflows |
| --- | --- | --- |
| `00` | Foundations | MinIO health, native `s3`, explicit `opendal+s3`, and the `opendal` gateway |
| `01`–`06` | Data engineering | pandas landing, Arrow datasets, DuckDB SQL, Dask ETL, archive chaining, and Polars |
| `07`–`13` | ML and workflows | Hugging Face, Lightning, Airflow, Prefect, Intake, Ray, and DVC Objects |
| `14`–`18` | Scientific and geospatial | Xarray, Zarr, Kerchunk, Rechunker, COG, and GeoParquet |
| `19` | Performance | Equal-result comparison of opendalfs, s3fs, and Arrow native S3 |

Open the
[`examples/marimo` directory](https://github.com/fsspec/opendalfs/tree/main/examples/marimo)
to see the full titles, source attribution, inline dependencies, and commands.

## Interpret benchmark results

The benchmark notebooks do not assume that one backend wins. They read the
same MinIO objects, verify equal results, warm up each implementation, randomize
execution order, and retain every timing sample. Treat the results as local
MinIO measurements for the recorded package versions and workload—not as a
universal cloud-storage ranking.

Some notebooks intentionally use isolated dependency sets. Rechunker requires
Zarr 2, while the Xarray and direct Zarr examples use Zarr 3. Airflow and
Prefect are also kept separate because the repository tests them in conflicting
dependency groups.
