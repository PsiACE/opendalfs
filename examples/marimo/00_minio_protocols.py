# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
#     "fsspec>=2025.5.1",
#     "marimo>=0.17.0",
#     "opendalfs",
# ]
# [tool.uv.sources]
# opendalfs = { path = "../..", editable = true }
# ///

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import fsspec
    from _shared import (
        MinioConfig,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        native_s3_filesystem,
        opendal_filesystem,
    )

    import marimo as mo

    return (
        MinioConfig,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        fsspec,
        mo,
        native_s3_filesystem,
        opendal_filesystem,
    )


@app.cell
def _(mo):
    mo.md("""
    # One object, three fsspec entry points

    This first solution verifies the repository's MinIO service and shows how an
    existing `s3://` URL, an explicit `opendal+s3://` URL, and the service gateway
    `opendal://s3/...` reach the same object.

    The input is the public UCI Iris dataset (CC BY 4.0). Start MinIO with
    `podman compose up -d --wait` before running the notebook.
    """)
    return


@app.cell
def _(MinioConfig, ensure_minio, native_s3_filesystem, opendal_filesystem):
    minio = MinioConfig.from_env()
    ensure_minio(minio)
    native_fs = native_s3_filesystem(minio)
    explicit_fs = opendal_filesystem(minio)

    assert type(native_fs).__module__.startswith("opendalfs")
    assert type(explicit_fs).__module__.startswith("opendalfs")
    return explicit_fs, minio, native_fs


@app.cell
def _(dataset_spec, explicit_fs, fetch_to_minio):
    iris = dataset_spec("iris")
    iris_key = "raw/iris/iris.zip"
    iris_download = fetch_to_minio(explicit_fs, iris, iris_key)

    assert iris_download.size > 3_000
    assert len(iris_download.sha256) == 64
    return iris_download, iris_key


@app.cell
def _(explicit_fs, fsspec, iris_download, iris_key, minio, native_fs):
    explicit_bytes = explicit_fs.cat_file(iris_key)
    native_bytes = native_fs.cat_file(iris_key)

    gateway_url = f"opendal://s3/{minio.bucket}/{iris_key}"
    gateway_fs, gateway_path = fsspec.core.url_to_fs(
        gateway_url,
        **minio.opendal_options(),
    )
    gateway_bytes = gateway_fs.cat_file(gateway_path)

    assert explicit_bytes == native_bytes == gateway_bytes
    assert len(explicit_bytes) == iris_download.size

    protocol_result = {
        "s3://": type(native_fs).__name__,
        "opendal+s3://": type(explicit_fs).__name__,
        "opendal://s3/": type(gateway_fs).__name__,
        "bytes": len(explicit_bytes),
        "sha256": iris_download.sha256,
    }
    return (protocol_result,)


@app.cell
def _(mo, protocol_result):
    mo.md(
        f"""
    ## Verified

    All three entry points returned the same {protocol_result["bytes"]:,} bytes with
    SHA-256 `{protocol_result["sha256"]}`.
    """
    )
    return


if __name__ == "__main__":
    app.run()
