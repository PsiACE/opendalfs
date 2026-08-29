# /// script
# requires-python = ">=3.12,<3.15"
# dependencies = [
#     "boto3>=1.37.3",
#     "fsspec>=2025.5.1",
#     "marimo>=0.17.0",
#     "opendalfs",
#     "pandas>=2.2",
#     "pyarrow>=22.0.0",
#     "ray[data]==2.56.0",
#     "s3fs>=2025.5.1",
# ]
# [tool.uv.sources]
# opendalfs = { path = "../..", editable = true }
# ///

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import os

    # Ray's uv hook uploads the repository without `.git`, which prevents the
    # editable setuptools-scm build used by this development checkout. Local
    # workers can inherit the already isolated script interpreter instead.
    os.environ.setdefault("RAY_ENABLE_UV_RUN_RUNTIME_ENV", "0")
    ray_uv_runtime_env_disabled = os.environ["RAY_ENABLE_UV_RUN_RUNTIME_ENV"] == "0"
    return (ray_uv_runtime_env_disabled,)


@app.cell
def _(ray_uv_runtime_env_disabled):
    import io
    import tempfile
    import urllib.parse
    import zipfile

    import pandas as pd
    import pyarrow as pa
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq
    import ray
    import s3fs
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

    assert ray_uv_runtime_env_disabled
    return (
        MinioConfig,
        benchmark,
        clear_prefix,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        io,
        mo,
        opendal_filesystem,
        pa,
        pafs,
        pd,
        pq,
        ray,
        s3fs,
        tempfile,
        urllib,
        zipfile,
    )


@app.cell
def _(mo):
    mo.md("""
    # Run Ray Data batch inference through Arrow's fsspec bridge

    [Ray Data's `read_parquet`](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_parquet.html)
    accepts a PyArrow filesystem for Parquet input and output; PyArrow exposes
    [`FSSpecHandler`](https://arrow.apache.org/docs/python/generated/pyarrow.fs.FSSpecHandler.html)
    for fsspec implementations. These two public extension points let OpenDAL
    participate without a Ray-specific adapter. A deterministic score substitutes
    for a model so this notebook tests the distributed data path rather than model
    quality.

        UCI Wine is CC BY 4.0. The local read benchmark includes OpenDAL via fsspec,
        `s3fs`, and Arrow's direct S3 client. It reports observations, not a promised
        winner: localhost timings are sensitive to caching and process startup.
    """)
    return


@app.cell
def _(
    MinioConfig,
    clear_prefix,
    ensure_minio,
    opendal_filesystem,
    pafs,
    s3fs,
    urllib,
):
    ray_minio = MinioConfig.from_env()
    ensure_minio(ray_minio)
    ray_fs = opendal_filesystem(ray_minio)
    ray_prefix = "12_ray"
    clear_prefix(ray_fs, ray_prefix)
    opendal_arrow_fs = pafs.PyFileSystem(pafs.FSSpecHandler(ray_fs))

    ray_s3fs = s3fs.S3FileSystem(**ray_minio.s3fs_options())
    s3fs_arrow_fs = pafs.PyFileSystem(pafs.FSSpecHandler(ray_s3fs))

    parsed_endpoint = urllib.parse.urlparse(ray_minio.endpoint)
    direct_arrow_fs = pafs.S3FileSystem(
        access_key=ray_minio.access_key_id,
        secret_key=ray_minio.secret_access_key,
        region=ray_minio.region,
        scheme=parsed_endpoint.scheme,
        endpoint_override=parsed_endpoint.netloc,
    )
    return (
        direct_arrow_fs,
        opendal_arrow_fs,
        ray_fs,
        ray_minio,
        ray_prefix,
        s3fs_arrow_fs,
    )


@app.cell
def _(dataset_spec, fetch_to_minio, io, pa, pd, ray_fs, ray_prefix, zipfile):
    ray_wine_source = dataset_spec("wine")
    ray_wine_key = f"{ray_prefix}/raw/wine.zip"
    ray_wine_download = fetch_to_minio(ray_fs, ray_wine_source, ray_wine_key)
    with zipfile.ZipFile(io.BytesIO(ray_fs.cat_file(ray_wine_key))) as archive:
        ray_wine_csv = archive.read("wine.data")

    ray_wine_columns = [
        "class",
        "alcohol",
        "malic_acid",
        "ash",
        "alcalinity",
        "magnesium",
        "phenols",
        "flavanoids",
        "nonflavanoid_phenols",
        "proanthocyanins",
        "color_intensity",
        "hue",
        "od280_od315",
        "proline",
    ]
    ray_wine_frame = pd.read_csv(io.BytesIO(ray_wine_csv), names=ray_wine_columns)
    inference_frame = pd.concat([ray_wine_frame] * 256, ignore_index=True)
    inference_frame.insert(0, "sample_id", range(len(inference_frame)))
    inference_table = pa.Table.from_pandas(inference_frame, preserve_index=False)

    assert len(inference_frame) == 45_568
    return inference_table, ray_wine_download, ray_wine_source


@app.cell
def _(inference_table, opendal_arrow_fs, pa, pq, ray_minio, ray_prefix):
    inference_prefix = f"{ray_minio.bucket}/{ray_prefix}/input"
    for part_index, part_table in enumerate(
        inference_table.to_batches(max_chunksize=11_392)
    ):
        pq.write_table(
            pa.Table.from_batches([part_table]),
            f"{inference_prefix}/part-{part_index}.parquet",
            filesystem=opendal_arrow_fs,
        )

    assert (
        pq.read_table(inference_prefix, filesystem=opendal_arrow_fs).num_rows
        == inference_table.num_rows
    )
    return (inference_prefix,)


@app.cell
def _(
    benchmark,
    direct_arrow_fs,
    inference_prefix,
    opendal_arrow_fs,
    pq,
    s3fs_arrow_fs,
):
    ray_read_benchmarks = benchmark(
        {
            "OpenDAL + Arrow fsspec": lambda: (
                pq.read_table(inference_prefix, filesystem=opendal_arrow_fs).num_rows
            ),
            "s3fs + Arrow fsspec": lambda: (
                pq.read_table(inference_prefix, filesystem=s3fs_arrow_fs).num_rows
            ),
            "Arrow direct S3": lambda: (
                pq.read_table(inference_prefix, filesystem=direct_arrow_fs).num_rows
            ),
        },
        repeat=3,
        seed=12,
    )
    assert all(result.median_s > 0 for result in ray_read_benchmarks)
    return (ray_read_benchmarks,)


@app.cell
def _(
    inference_prefix,
    opendal_arrow_fs,
    ray,
    ray_minio,
    ray_prefix,
    tempfile,
):
    ray_tempdir = tempfile.TemporaryDirectory(prefix="opendalfs-ray-")
    ray.init(
        address="local",
        num_cpus=2,
        include_dashboard=False,
        ignore_reinit_error=True,
        _temp_dir=ray_tempdir.name,
    )
    inference_dataset = ray.data.read_parquet(
        inference_prefix,
        filesystem=opendal_arrow_fs,
        columns=["sample_id", "alcohol", "color_intensity"],
    )

    def score_wine_batch(batch):
        return batch.assign(
            score=0.7 * batch["alcohol"] + 0.3 * batch["color_intensity"]
        )[["sample_id", "score"]]

    scored_dataset = inference_dataset.map_batches(
        score_wine_batch,
        batch_format="pandas",
    ).materialize()
    inference_output_prefix = f"{ray_minio.bucket}/{ray_prefix}/output"
    scored_dataset.write_parquet(
        inference_output_prefix,
        filesystem=opendal_arrow_fs,
    )

    scored_rows = scored_dataset.count()
    assert scored_rows == inference_dataset.count() == 45_568
    return inference_output_prefix, ray_tempdir, scored_rows


@app.cell
def _(
    inference_output_prefix,
    mo,
    opendal_arrow_fs,
    pq,
    ray,
    ray_read_benchmarks,
    ray_tempdir,
    ray_wine_download,
    ray_wine_source,
    scored_rows,
):
    written_rows = pq.read_table(
        inference_output_prefix,
        filesystem=opendal_arrow_fs,
    ).num_rows
    ray.shutdown()
    ray_tempdir.cleanup()
    assert written_rows == scored_rows

    benchmark_lines = "\n".join(
        f"- {result.label}: median {result.median_s:.4f} s, p95 {result.p95_s:.4f} s"
        for result in ray_read_benchmarks
    )
    mo.md(
        f"""
    ## Verified batch path

    - Data source: [{ray_wine_source.source}]({ray_wine_source.source})
    - License: **{ray_wine_source.license}**
    - Raw archive: {ray_wine_download.size:,} bytes
    - Scored and persisted rows: {written_rows:,}

    {benchmark_lines}

    Ray read and wrote the dataset using the same Arrow filesystem bridge. The
    benchmark isolates local Parquet reads; end-to-end Ray startup is intentionally
    excluded because it would obscure the storage comparison at this scale.
    """
    )
    return


if __name__ == "__main__":
    app.run()
