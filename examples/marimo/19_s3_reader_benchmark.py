# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3>=1.37.3",
#     "fsspec>=2025.5.1",
#     "marimo>=0.17.0",
#     "opendalfs",
#     "pyarrow>=22.0.0",
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
    import platform
    import urllib.parse

    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.fs as pafs
    import s3fs
    from _shared import (
        MinioConfig,
        benchmark,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        opendal_filesystem,
        write_json,
    )

    import marimo as mo

    return (
        MinioConfig,
        benchmark,
        dataset_spec,
        ds,
        ensure_minio,
        fetch_to_minio,
        mo,
        opendal_filesystem,
        pa,
        pafs,
        platform,
        s3fs,
        urllib,
        write_json,
    )


@app.cell
def _(mo):
    mo.md("""
    # A reproducible local-MinIO S3 reader benchmark

    This experiment compares storage adapters—not dataframe engines. All three
    cases use PyArrow Dataset, the same object, projection, predicate, output type,
    thread settings, and correctness check:

    1. Arrow `FSSpecHandler` over opendalfs;
    2. Arrow `FSSpecHandler` over s3fs;
    3. Arrow's native `S3FileSystem`.

    Official extension points and configuration:
    [Arrow + fsspec](https://arrow.apache.org/docs/python/filesystems.html) ·
    [Arrow S3](https://arrow.apache.org/docs/python/generated/pyarrow.fs.S3FileSystem.html) ·
    [s3fs S3-compatible storage](https://github.com/fsspec/s3fs/blob/main/docs/source/index.rst) ·
    [OpenDAL Python](https://opendal.apache.org/bindings/python/).

    The NYC TLC input uses the NYC Open Data Terms of Use. This is a loopback
    MinIO microbenchmark and must not be generalized to WAN or cloud performance.
    """)
    return


@app.cell
def _(MinioConfig, ensure_minio, opendal_filesystem):
    minio = MinioConfig.from_env()
    ensure_minio(minio)
    benchmark_fs = opendal_filesystem(minio)
    assert type(benchmark_fs).__module__.startswith("opendalfs")
    return benchmark_fs, minio


@app.cell
def _(benchmark_fs, dataset_spec, fetch_to_minio):
    benchmark_spec = dataset_spec("nyc_taxi")
    benchmark_key = "raw/s3-reader-benchmark/yellow_tripdata_2025-01.parquet"
    benchmark_download = fetch_to_minio(
        benchmark_fs,
        benchmark_spec,
        benchmark_key,
        timeout=120,
    )
    assert benchmark_download.size == 59_158_238
    return benchmark_download, benchmark_key, benchmark_spec


@app.cell
def _(benchmark_fs, minio, pafs, s3fs, urllib):
    endpoint = urllib.parse.urlparse(minio.endpoint)
    endpoint_override = endpoint.netloc or endpoint.path

    opendal_arrow_fs = pafs.PyFileSystem(pafs.FSSpecHandler(benchmark_fs))
    s3fs_instance = s3fs.S3FileSystem(**minio.s3fs_options())
    s3fs_arrow_fs = pafs.PyFileSystem(pafs.FSSpecHandler(s3fs_instance))
    native_arrow_fs = pafs.S3FileSystem(
        access_key=minio.access_key_id,
        secret_key=minio.secret_access_key,
        region=minio.region,
        scheme=endpoint.scheme,
        endpoint_override=endpoint_override,
    )
    return native_arrow_fs, opendal_arrow_fs, s3fs_arrow_fs


@app.cell
def _(
    benchmark_key,
    ds,
    minio,
    native_arrow_fs,
    opendal_arrow_fs,
    s3fs_arrow_fs,
):
    benchmark_columns = ["VendorID", "trip_distance", "total_amount"]
    benchmark_filter = ds.field("passenger_count") >= 1

    def scan_opendal():
        dataset = ds.dataset(
            f"{minio.bucket}/{benchmark_key}",
            filesystem=opendal_arrow_fs,
            format="parquet",
        )
        return dataset.to_table(columns=benchmark_columns, filter=benchmark_filter)

    bucket_key = f"{minio.bucket}/{benchmark_key}"

    def scan_s3fs():
        dataset = ds.dataset(
            bucket_key,
            filesystem=s3fs_arrow_fs,
            format="parquet",
        )
        return dataset.to_table(columns=benchmark_columns, filter=benchmark_filter)

    def scan_arrow_native():
        dataset = ds.dataset(
            bucket_key,
            filesystem=native_arrow_fs,
            format="parquet",
        )
        return dataset.to_table(columns=benchmark_columns, filter=benchmark_filter)

    scan_cases = {
        "opendalfs": scan_opendal,
        "s3fs": scan_s3fs,
        "arrow-native": scan_arrow_native,
    }
    return benchmark_columns, scan_cases


@app.cell
def _(scan_cases):
    correctness_tables = {label: scan_case() for label, scan_case in scan_cases.items()}
    expected_table = correctness_tables["opendalfs"]
    assert expected_table.num_rows > 2_000_000
    assert all(
        candidate.equals(expected_table) for candidate in correctness_tables.values()
    )
    return correctness_tables, expected_table


@app.cell
def _(benchmark, scan_cases):
    benchmark_results = benchmark(scan_cases, repeat=5, seed=202501)
    assert len(benchmark_results) == 3
    assert all(len(result.samples_s) == 5 for result in benchmark_results)
    return (benchmark_results,)


@app.cell
def _(
    benchmark_columns,
    benchmark_download,
    benchmark_fs,
    benchmark_results,
    benchmark_spec,
    expected_table,
    pa,
    platform,
    write_json,
):
    benchmark_report_key = "benchmarks/s3-reader/results.json"
    benchmark_report = {
        "scope": "local MinIO loopback microbenchmark",
        "dataset": {
            "source": benchmark_spec.source,
            "license": benchmark_spec.license,
            "key": benchmark_download.key,
            "size": benchmark_download.size,
            "sha256": benchmark_download.sha256,
        },
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "pyarrow": pa.__version__,
        },
        "workload": {
            "columns": benchmark_columns,
            "filter": "passenger_count >= 1",
            "rows": expected_table.num_rows,
        },
        "cases": [
            {
                "label": result.label,
                "samples_s": list(result.samples_s),
                "median_s": result.median_s,
                "p95_s": result.p95_s,
            }
            for result in benchmark_results
        ],
    }
    write_json(benchmark_fs, benchmark_report_key, benchmark_report)
    return benchmark_report, benchmark_report_key


@app.cell
def _(
    benchmark_report,
    benchmark_report_key,
    benchmark_results,
    correctness_tables,
    minio,
    mo,
):
    result_lines = "\n".join(
        f"- **{result.label}**: raw={list(result.samples_s)}, "
        f"median={result.median_s:.4f}s, p95={result.p95_s:.4f}s"
        for result in benchmark_results
    )
    mo.md(
        f"""
    ## Correctness first, then timing

    All {len(correctness_tables)} readers returned exactly equal Arrow tables with
    {benchmark_report["workload"]["rows"]:,} rows. Each reader received one warm-up;
    five measured rounds used a deterministic randomized order. Raw samples are
    retained rather than collapsing the experiment to one headline number.

    {result_lines}

    Report: `{minio.url(benchmark_report_key, protocol="opendal+s3")}`

    No winner is declared: loopback storage, reader defaults, OS caches, and MinIO
    request patterns all affect these measurements. For a deeper investigation,
    pair the saved samples with `mc admin trace --stats` request counts and bytes.
            """
    )
    return


if __name__ == "__main__":
    app.run()
