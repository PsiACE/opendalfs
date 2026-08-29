# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
#     "datasets==5.0.1",
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
    import time

    from _shared import (
        MinioConfig,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        opendal_filesystem,
    )
    from datasets import IterableDataset, load_dataset

    import marimo as mo

    return (
        IterableDataset,
        MinioConfig,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        load_dataset,
        mo,
        opendal_filesystem,
        time,
    )


@app.cell
def _(mo):
    mo.md("""
    # Mirror a Hugging Face dataset for reproducible training

        This solution mirrors the pinned Palmer Penguins CSV from the Hugging Face
    Hub into the repository's MinIO service, then asks the official
    [`datasets.load_dataset()` remote-file workflow](https://huggingface.co/docs/datasets/loading)
    to consume the OpenDAL URL in eager and streaming modes. It does not add a
    dataset adapter.

        The dataset is released under CC0. Source and license metadata are preserved
        next to the mirrored object. Start MinIO with `podman compose up -d --wait`.
    """)
    return


@app.cell
def _(MinioConfig, ensure_minio, opendal_filesystem):
    hf_minio = MinioConfig.from_env()
    ensure_minio(hf_minio)
    hf_fs = opendal_filesystem(hf_minio)
    assert type(hf_fs).__module__.startswith("opendalfs")
    return hf_fs, hf_minio


@app.cell
def _(dataset_spec, fetch_to_minio, hf_fs):
    hf_source = dataset_spec("hf_penguins")
    hf_key = "07_huggingface/raw/penguins.csv"
    hf_download = fetch_to_minio(hf_fs, hf_source, hf_key)

    assert hf_download.size > 10_000
    assert len(hf_download.sha256) == 64
    assert hf_fs.exists(f"{hf_key}.source.json")
    return hf_download, hf_key, hf_source


@app.cell
def _(hf_fs, hf_key, hf_minio, load_dataset, time):
    hf_url = hf_minio.url(hf_key, protocol="opendal+s3")
    hf_storage_options = {"opendal+s3": hf_fs.storage_options}

    hf_eager_started = time.perf_counter()
    hf_eager = load_dataset(
        "csv",
        data_files=hf_url,
        split="train",
        storage_options=hf_storage_options,
    )
    hf_eager_seconds = time.perf_counter() - hf_eager_started

    hf_stream_started = time.perf_counter()
    hf_stream = load_dataset(
        "csv",
        data_files=hf_url,
        split="train",
        streaming=True,
        storage_options=hf_storage_options,
    )
    hf_first_five = list(hf_stream.take(5))
    hf_first_five_seconds = time.perf_counter() - hf_stream_started
    return (
        hf_eager,
        hf_eager_seconds,
        hf_first_five,
        hf_first_five_seconds,
        hf_stream,
        hf_url,
    )


@app.cell
def _(IterableDataset, hf_eager, hf_first_five, hf_stream):
    hf_expected_columns = {
        "species",
        "island",
        "bill_length_mm",
        "bill_depth_mm",
        "flipper_length_mm",
        "body_mass_g",
        "sex",
        "year",
    }
    hf_species = set(hf_eager.unique("species"))

    assert len(hf_eager) == 344
    assert set(hf_eager.column_names) == hf_expected_columns
    assert hf_species == {"Adelie", "Chinstrap", "Gentoo"}
    assert isinstance(hf_stream, IterableDataset)
    assert hf_first_five == list(hf_eager.select(range(5)))
    return


@app.cell
def _(
    hf_download,
    hf_eager,
    hf_eager_seconds,
    hf_first_five_seconds,
    hf_source,
    hf_url,
    mo,
):
    mo.md(
        f"""
    ## Verified mirror

    - Source: [{hf_source.source}]({hf_source.source})
    - License: **{hf_source.license}**
    - Mirrored URL: `{hf_url}`
    - Object: {hf_download.size:,} bytes, SHA-256 `{hf_download.sha256}`
    - Records: {len(hf_eager):,}
    - Eager preparation: {hf_eager_seconds:.3f} s
    - Streaming time to first five records: {hf_first_five_seconds:.3f} s

    The timings describe this local run; they are not a WAN-versus-MinIO
    performance claim. The contract is that the same pinned object works through
    both official Hugging Face access modes.
    """
    )
    return


if __name__ == "__main__":
    app.run()
