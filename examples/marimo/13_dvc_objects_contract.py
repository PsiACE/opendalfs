# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
#     "dvc-objects==5.2.0",
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
    import hashlib
    import tempfile
    from pathlib import Path

    from _shared import (
        MinioConfig,
        clear_prefix,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        opendal_filesystem,
        opened_local_copy,
    )
    from dvc_objects.fs.memory import MemoryFileSystem

    import marimo as mo

    return (
        MemoryFileSystem,
        MinioConfig,
        Path,
        clear_prefix,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        hashlib,
        mo,
        opendal_filesystem,
        opened_local_copy,
        tempfile,
    )


@app.cell
def _(mo):
    mo.md("""
    # Validate the dvc-objects filesystem contract (experimental)

    DVC's object database addresses content by hash, while the
    [`dvc-objects` package](https://github.com/iterative/dvc-objects) supplies the
    filesystem operations used to move those objects. This notebook exercises the
    package's supported fsspec-backed `MemoryFileSystem(fs=...)` entry point over
    OpenDAL: put, find, walk, get, and content verification.

        This is deliberately marked **experimental**. It proves the low-level
        `dvc-objects` 5.2 contract used by opendalfs integration tests; it does not
        claim that the high-level `dvc remote add` CLI recognizes `opendal+s3://`.
        The input is UCI Iris, licensed CC BY 4.0.
    """)
    return


@app.cell
def _(
    MemoryFileSystem,
    MinioConfig,
    clear_prefix,
    ensure_minio,
    opendal_filesystem,
):
    dvc_minio = MinioConfig.from_env()
    ensure_minio(dvc_minio)
    dvc_backend = opendal_filesystem(dvc_minio)
    dvc_prefix = "13_dvc"
    clear_prefix(dvc_backend, dvc_prefix)
    dvc_fs = MemoryFileSystem(fs=dvc_backend)
    return dvc_backend, dvc_fs, dvc_prefix


@app.cell
def _(
    dataset_spec,
    dvc_backend,
    dvc_fs,
    dvc_prefix,
    fetch_to_minio,
    hashlib,
    opened_local_copy,
):
    dvc_source = dataset_spec("iris")
    dvc_raw_key = f"{dvc_prefix}/raw/iris.zip"
    dvc_download = fetch_to_minio(dvc_backend, dvc_source, dvc_raw_key)
    dvc_payload = dvc_backend.cat_file(dvc_raw_key)
    dvc_md5 = hashlib.md5(dvc_payload, usedforsecurity=False).hexdigest()
    dvc_object_key = f"{dvc_prefix}/objects/md5/{dvc_md5[:2]}/{dvc_md5[2:]}"

    with opened_local_copy(dvc_backend, dvc_raw_key) as dvc_local_source:
        dvc_fs.put([str(dvc_local_source)], [dvc_object_key])

    assert dvc_backend.cat_file(dvc_object_key) == dvc_payload
    return dvc_download, dvc_md5, dvc_object_key, dvc_payload, dvc_source


@app.cell
def _(Path, dvc_fs, dvc_object_key, dvc_payload, dvc_prefix, tempfile):
    dvc_found_objects = set(dvc_fs.find(f"{dvc_prefix}/objects"))
    dvc_walk = [
        (root, sorted(directories), sorted(files))
        for root, directories, files in dvc_fs.walk(f"{dvc_prefix}/objects")
    ]

    with tempfile.TemporaryDirectory() as dvc_download_directory:
        restored_path = Path(dvc_download_directory, "iris.zip")
        dvc_fs.get([dvc_object_key], [str(restored_path)])
        restored_payload = restored_path.read_bytes()

    assert len(dvc_found_objects) == 1
    assert next(iter(dvc_found_objects)).endswith(dvc_object_key)
    assert any(dvc_object_key.rsplit("/", 1)[1] in files for _, _, files in dvc_walk)
    assert restored_payload == dvc_payload
    return dvc_found_objects, dvc_walk


@app.cell
def _(
    dvc_download,
    dvc_found_objects,
    dvc_md5,
    dvc_object_key,
    dvc_source,
    dvc_walk,
    mo,
):
    mo.md(
        f"""
    ## Verified low-level object contract

    - Data source: [{dvc_source.source}]({dvc_source.source})
    - License: **{dvc_source.license}**
    - Raw archive: {dvc_download.size:,} bytes
    - Content MD5: `{dvc_md5}`
    - Object key: `{dvc_object_key}`
    - `find()` result: `{sorted(dvc_found_objects)}`
    - `walk()` entries: {len(dvc_walk)}

    The downloaded bytes match the source object exactly, and the key is derived
    from the same content digest. This is the useful storage contract to stabilize
    before proposing a first-class OpenDAL remote to DVC itself.
    """
    )
    return


if __name__ == "__main__":
    app.run()
