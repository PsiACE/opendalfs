# /// script
# requires-python = ">=3.12,<3.15"
# dependencies = [
#     "boto3>=1.37.3",
#     "fsspec>=2025.5.1",
#     "marimo>=0.17.0",
#     "opendalfs",
#     "pandas>=2.2",
#     "prefect==3.8.4",
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
    import zipfile
    from datetime import timedelta

    import pandas as pd
    from _shared import (
        MinioConfig,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        opendal_filesystem,
    )
    from prefect import flow, task
    from prefect.filesystems import RemoteFileSystem
    from prefect.serializers import JSONSerializer
    from prefect.tasks import task_input_hash

    import marimo as mo

    return (
        JSONSerializer,
        MinioConfig,
        RemoteFileSystem,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        flow,
        io,
        mo,
        opendal_filesystem,
        pd,
        task,
        task_input_hash,
        timedelta,
        zipfile,
    )


@app.cell
def _(mo):
    mo.md("""
    # Persist a Prefect task cache in MinIO

    [Prefect's result-persistence model](https://docs.prefect.io/v3/advanced/results)
    stores serialized task results in a storage block and lets a cache policy
    decide whether work can be reused. Here, `RemoteFileSystem` points at
    `opendal+s3://`; the same flow runs twice and the second run must reuse the
    persisted result.

        The input is UCI Iris (CC BY 4.0). This example runs Prefect's local execution
        engine, so no Prefect Cloud account or worker is required.
    """)
    return


@app.cell
def _(MinioConfig, RemoteFileSystem, ensure_minio, opendal_filesystem):
    prefect_minio = MinioConfig.from_env()
    ensure_minio(prefect_minio)
    prefect_fs = opendal_filesystem(prefect_minio)
    prefect_basepath = prefect_minio.url("10_prefect/results", protocol="opendal+s3")
    prefect_storage = RemoteFileSystem(
        basepath=prefect_basepath,
        settings=dict(prefect_fs.storage_options),
    )
    prefect_storage.save("opendalfs-marimo-10", overwrite=True)
    return prefect_fs, prefect_storage


@app.cell
def _(dataset_spec, fetch_to_minio, io, pd, prefect_fs, zipfile):
    iris_source = dataset_spec("iris")
    iris_key = "10_prefect/raw/iris.zip"
    iris_download = fetch_to_minio(prefect_fs, iris_source, iris_key)
    with zipfile.ZipFile(io.BytesIO(prefect_fs.cat_file(iris_key))) as archive:
        iris_csv = archive.read("bezdekIris.data")

    iris_columns = [
        "sepal_length",
        "sepal_width",
        "petal_length",
        "petal_width",
        "species",
    ]
    iris_frame = pd.read_csv(io.BytesIO(iris_csv), names=iris_columns)
    iris_records = iris_frame.to_dict(orient="records")

    assert len(iris_records) == 150
    assert iris_frame["species"].nunique() == 3
    return iris_download, iris_records, iris_source


@app.cell
def _(JSONSerializer, flow, prefect_storage, task, task_input_hash, timedelta):
    prefect_executions = {"count": 0}

    @task(
        cache_key_fn=task_input_hash,
        cache_expiration=timedelta(days=1),
        persist_result=True,
        result_storage=prefect_storage,
        result_storage_key="iris-summary.json",
        result_serializer=JSONSerializer(),
    )
    def summarize_iris(records):
        prefect_executions["count"] += 1
        species_counts = {}
        for record in records:
            species = record["species"]
            species_counts[species] = species_counts.get(species, 0) + 1
        return {
            "records": len(records),
            "species_counts": species_counts,
        }

    @flow(name="opendalfs-iris-summary", log_prints=False)
    def iris_summary_flow(records):
        return summarize_iris(records, return_state=True)

    return iris_summary_flow, prefect_executions


@app.cell
def _(iris_records, iris_summary_flow, prefect_executions):
    prefect_first_state = iris_summary_flow(iris_records)
    prefect_second_state = iris_summary_flow(iris_records)
    prefect_first_result = prefect_first_state.result()
    prefect_second_result = prefect_second_state.result()

    assert prefect_first_result == prefect_second_result
    assert prefect_second_state.name == "Cached"
    assert prefect_executions["count"] <= 1
    assert prefect_second_result == {
        "records": 150,
        "species_counts": {
            "Iris-setosa": 50,
            "Iris-versicolor": 50,
            "Iris-virginica": 50,
        },
    }
    return prefect_first_state, prefect_second_result, prefect_second_state


@app.cell
def _(
    iris_download,
    iris_source,
    mo,
    prefect_first_state,
    prefect_fs,
    prefect_second_result,
    prefect_second_state,
):
    prefect_objects = prefect_fs.find("10_prefect/results")
    assert any(key.endswith("iris-summary.json") for key in prefect_objects)

    mo.md(
        f"""
    ## Verified cache reuse

    - Data source: [{iris_source.source}]({iris_source.source})
    - License: **{iris_source.license}**
    - Raw archive: {iris_download.size:,} bytes
    - First state: `{prefect_first_state.name}`
    - Second state: `{prefect_second_state.name}`
    - Persisted result: `{prefect_objects}`
    - Summary: `{prefect_second_result}`

    Rerunning the notebook is safe: an earlier persisted result may make both
    states cached, while the invariant remains that no more than one local task
    execution is needed. See Prefect's official result persistence and cache-policy
    documentation for production retention choices.
    """
    )
    return


if __name__ == "__main__":
    app.run()
