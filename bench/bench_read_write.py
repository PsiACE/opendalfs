from __future__ import annotations

import argparse
import os
import statistics
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from uuid import uuid4

import boto3
import fsspec
import pyarrow.fs as pafs
from botocore.exceptions import ClientError

DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024


@dataclass(frozen=True)
class BenchmarkProfile:
    write_chunk: int
    opendal_write_concurrent: int
    arrow_background_writes: bool


@dataclass(frozen=True)
class Backend:
    label: str
    filesystem: pafs.FileSystem
    create_base: Callable[[int], str]
    workers: int


PROFILES = {
    "optimized": BenchmarkProfile(
        write_chunk=DEFAULT_CHUNK_SIZE,
        opendal_write_concurrent=4,
        arrow_background_writes=True,
    ),
    "serial": BenchmarkProfile(
        write_chunk=DEFAULT_CHUNK_SIZE,
        opendal_write_concurrent=1,
        arrow_background_writes=False,
    ),
}


class OpenOptionsFileSystem:
    """Apply equivalent fsspec open options to wrapped backends."""

    def __init__(self, filesystem, *, block_size: int) -> None:
        self._filesystem = filesystem
        self._block_size = block_size

    @property
    def protocol(self):
        return self._filesystem.protocol

    def open(self, path, mode="rb", **kwargs):
        kwargs.setdefault("block_size", self._block_size)
        kwargs.setdefault("cache_type", "none")
        return self._filesystem.open(path, mode=mode, **kwargs)

    def __getattr__(self, name):
        return getattr(self._filesystem, name)


def _env_first(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def _parse_sizes(value: str) -> list[int]:
    sizes = [int(raw.strip()) for raw in value.split(",") if raw.strip()]
    if not sizes:
        raise ValueError("sizes must contain at least one entry")
    return sizes


def _load_config(args: argparse.Namespace) -> dict[str, str]:
    bucket = (
        args.bucket or _env_first("OPENDAL_S3_BUCKET", "AWS_S3_BUCKET") or "opendal"
    )
    region = (
        args.region
        or _env_first("OPENDAL_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    endpoint = (
        args.endpoint
        or _env_first("OPENDAL_S3_ENDPOINT", "AWS_ENDPOINT")
        or "http://127.0.0.1:9000"
    )
    access_key_id = (
        args.access_key_id
        or _env_first("OPENDAL_S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID")
        or "minioadmin"
    )
    secret_access_key = (
        args.secret_access_key
        or _env_first("OPENDAL_S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY")
        or "minioadmin"
    )
    return {
        "bucket": bucket,
        "region": region,
        "endpoint": endpoint,
        "access_key_id": access_key_id,
        "secret_access_key": secret_access_key,
    }


def _ensure_bucket(config: dict[str, str]) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        region_name=config["region"],
        aws_access_key_id=config["access_key_id"],
        aws_secret_access_key=config["secret_access_key"],
    )
    try:
        client.head_bucket(Bucket=config["bucket"])
    except ClientError:
        client.create_bucket(Bucket=config["bucket"])


def _run_benchmark(
    filesystem: pafs.FileSystem,
    base: str,
    size_mib: int,
    files: int,
    workers: int,
    stream_buffer_size: int,
) -> tuple[float, float]:
    payload = b"x" * (size_mib * 1024 * 1024)
    paths = [f"{base}/file-{index}.bin" for index in range(files)]

    def write_one(path: str) -> None:
        with filesystem.open_output_stream(
            path,
            buffer_size=stream_buffer_size,
        ) as writer:
            writer.write(payload)

    def read_one(path: str) -> None:
        with filesystem.open_input_stream(
            path,
            buffer_size=stream_buffer_size,
        ) as reader:
            data = reader.read()
        if data != payload:
            raise RuntimeError(f"data mismatch for {path}")

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(write_one, paths))
    write_seconds = time.perf_counter() - started

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(read_one, paths))
    read_seconds = time.perf_counter() - started
    return write_seconds, read_seconds


def _summarize(
    size_mib: int,
    files: int,
    timings: list[tuple[float, float]],
) -> tuple[float, float, list[float], list[float]]:
    total_mib = size_mib * files
    write_samples = [total_mib / write_seconds for write_seconds, _ in timings]
    read_samples = [total_mib / read_seconds for _, read_seconds in timings]
    return (
        statistics.median(write_samples),
        statistics.median(read_samples),
        write_samples,
        read_samples,
    )


def _report(
    backend: Backend,
    size_mib: int,
    files: int,
    timings: list[tuple[float, float]],
) -> tuple[float, float]:
    write_mibps, read_mibps, write_samples, read_samples = _summarize(
        size_mib,
        files,
        timings,
    )
    print(
        f"[{backend.label}] size {size_mib} MiB x {files}; "
        f"file workers {backend.workers}"
    )
    print(f"[{backend.label}] write median {write_mibps:.1f} MiB/s")
    print(f"[{backend.label}] read  median {read_mibps:.1f} MiB/s")
    if len(timings) > 1:
        write_values = ", ".join(f"{value:.1f}" for value in write_samples)
        read_values = ", ".join(f"{value:.1f}" for value in read_samples)
        print(f"[{backend.label}] write samples: {write_values}")
        print(f"[{backend.label}] read  samples: {read_values}")
    return write_mibps, read_mibps


def _arrow_direct_backend(
    config: dict[str, str],
    args: argparse.Namespace,
    profile: BenchmarkProfile,
) -> Backend:
    filesystem = pafs.S3FileSystem(
        access_key=config["access_key_id"],
        secret_key=config["secret_access_key"],
        region=config["region"],
        endpoint_override=config["endpoint"],
        allow_bucket_creation=True,
        background_writes=profile.arrow_background_writes,
    )
    return Backend(
        label="arrow-direct",
        filesystem=filesystem,
        create_base=lambda size_mib: (
            f"{config['bucket']}/{args.prefix}-arrow-{size_mib}mib-{uuid4()}"
        ),
        workers=args.workers,
    )


def _opendalfs_backend(
    config: dict[str, str],
    args: argparse.Namespace,
    profile: BenchmarkProfile,
) -> Backend:
    if args.opendalfs_path:
        import sys

        sys.path.insert(0, args.opendalfs_path)
        import opendal
        import opendal.file

        # OpenDAL 0.47 moved these types out of the package root. This lets the
        # same environment load an older opendalfs checkout for comparison.
        opendal.AsyncFile = opendal.file.AsyncFile
        opendal.File = opendal.file.File

    import opendalfs

    storage_options = {
        "bucket": config["bucket"],
        "region": config["region"],
        "endpoint": config["endpoint"],
        "access_key_id": config["access_key_id"],
        "secret_access_key": config["secret_access_key"],
    }
    if args.opendalfs_write_options:
        storage_options["write_options"] = {
            "chunk": profile.write_chunk,
            "concurrent": profile.opendal_write_concurrent,
        }
    backend = opendalfs.OpendalFileSystem("s3", **storage_options)
    filesystem = pafs.PyFileSystem(
        pafs.FSSpecHandler(
            OpenOptionsFileSystem(backend, block_size=profile.write_chunk)
        )
    )
    return Backend(
        label="arrow-fsspec-opendalfs",
        filesystem=filesystem,
        create_base=lambda size_mib: f"{args.prefix}-opendalfs-{size_mib}mib-{uuid4()}",
        workers=args.fsspec_workers,
    )


def _s3fs_backend(
    config: dict[str, str],
    args: argparse.Namespace,
    profile: BenchmarkProfile,
) -> Backend:
    backend = fsspec.filesystem(
        "s3",
        key=config["access_key_id"],
        secret=config["secret_access_key"],
        client_kwargs={
            "endpoint_url": config["endpoint"],
            "region_name": config["region"],
        },
        config_kwargs={"s3": {"addressing_style": "path"}},
        default_block_size=profile.write_chunk,
        default_cache_type="none",
        default_fill_cache=False,
        # S3File._upload_chunk is serial for the file-like open path.
        max_concurrency=1,
    )
    filesystem = pafs.PyFileSystem(
        pafs.FSSpecHandler(
            OpenOptionsFileSystem(backend, block_size=profile.write_chunk)
        )
    )
    return Backend(
        label="arrow-fsspec-s3",
        filesystem=filesystem,
        create_base=lambda size_mib: (
            f"{config['bucket']}/{args.prefix}-s3fs-{size_mib}mib-{uuid4()}"
        ),
        workers=args.fsspec_workers,
    )


def _resolve_profile(args: argparse.Namespace) -> BenchmarkProfile:
    selected = PROFILES[args.profile]
    return BenchmarkProfile(
        write_chunk=args.write_chunk or selected.write_chunk,
        opendal_write_concurrent=(
            args.write_concurrent or selected.opendal_write_concurrent
        ),
        arrow_background_writes=(
            selected.arrow_background_writes
            if args.arrow_background_writes is None
            else args.arrow_background_writes
        ),
    )


def _run_backends(
    backends: list[Backend],
    args: argparse.Namespace,
    size_mib: int,
) -> None:
    timings = {backend.label: [] for backend in backends}
    for round_index in range(args.rounds):
        offset = round_index % len(backends)
        ordered = backends[offset:] + backends[:offset]
        for backend in ordered:
            timings[backend.label].append(
                _run_benchmark(
                    backend.filesystem,
                    backend.create_base(size_mib),
                    size_mib,
                    args.files,
                    backend.workers,
                    args.stream_buffer_size,
                )
            )

    results: list[tuple[str, float, float]] = []
    for backend in backends:
        write_mibps, read_mibps = _report(
            backend,
            size_mib,
            args.files,
            timings[backend.label],
        )
        results.append((backend.label, write_mibps, read_mibps))

    print(f"\nsize {size_mib} MiB x {args.files} summary")
    print("backend                       write MiB/s  read MiB/s")
    for label, write_mibps, read_mibps in results:
        print(f"{label:<29} {write_mibps:>11.1f} {read_mibps:>11.1f}")
    print()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare Arrow direct, opendalfs, and s3fs on MinIO."
    )
    parser.add_argument(
        "--sizes",
        type=_parse_sizes,
        default="16,32,64",
        help="Comma-separated object sizes in MiB (default: 16,32,64)",
    )
    parser.add_argument("--files", type=int, default=4)
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Thread workers across files (default: 1)",
    )
    parser.add_argument(
        "--fsspec-workers",
        type=int,
        default=None,
        help="Override workers for fsspec backends (default: same as --workers)",
    )
    parser.add_argument("--rounds", type=int, default=7)
    parser.add_argument(
        "--profile",
        choices=tuple(PROFILES),
        default="optimized",
        help=(
            "optimized: OpenDAL concurrent writes and Arrow background writes; "
            "serial: disable both (default: optimized)"
        ),
    )
    parser.add_argument(
        "--stream-buffer-size",
        type=int,
        default=0,
        help="Arrow stream buffer size in bytes (default: disabled)",
    )
    parser.add_argument(
        "--write-chunk",
        type=int,
        help="Override the profile's shared fsspec/OpenDAL chunk size",
    )
    parser.add_argument(
        "--write-concurrent",
        type=int,
        help=(
            "Override OpenDAL per-file multipart concurrency; "
            "s3fs file-like uploads remain serial"
        ),
    )
    parser.add_argument(
        "--arrow-background-writes",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override Arrow direct background writes for the selected profile",
    )
    parser.add_argument("--prefix", default="opendalfs-repro")
    parser.add_argument("--bucket")
    parser.add_argument("--region")
    parser.add_argument("--endpoint")
    parser.add_argument("--access-key-id")
    parser.add_argument("--secret-access-key")
    parser.add_argument(
        "--opendalfs-path",
        help="Optional local path to opendalfs repo (adds to sys.path)",
    )
    parser.add_argument(
        "--opendalfs-write-options",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Disable when benchmarking a revision that predates write_options",
    )
    parser.add_argument(
        "--skip-s3fs",
        action="store_true",
        help="Skip fsspec+s3fs comparison",
    )
    args = parser.parse_args()
    if args.fsspec_workers is None:
        args.fsspec_workers = args.workers

    positive_values = {
        "--files": args.files,
        "--workers": args.workers,
        "--fsspec-workers": args.fsspec_workers,
        "--rounds": args.rounds,
    }
    for option, value in positive_values.items():
        if value <= 0:
            parser.error(f"{option} must be positive")
    if args.stream_buffer_size < 0:
        parser.error("--stream-buffer-size must be non-negative")
    for option, value in {
        "--write-chunk": args.write_chunk,
        "--write-concurrent": args.write_concurrent,
    }.items():
        if value is not None and value <= 0:
            parser.error(f"{option} must be positive")
    return args


def main() -> None:
    args = _parse_args()
    config = _load_config(args)
    _ensure_bucket(config)
    profile = _resolve_profile(args)

    print(
        f"profile={args.profile} chunk={profile.write_chunk} "
        f"opendal_concurrent={profile.opendal_write_concurrent} "
        f"arrow_background_writes={profile.arrow_background_writes} "
        f"opendalfs_write_options={args.opendalfs_write_options}"
    )
    print(
        "s3fs note: PyArrow uses the file-like open path, whose multipart "
        "parts are uploaded serially"
    )

    backends = [
        _arrow_direct_backend(config, args, profile),
        _opendalfs_backend(config, args, profile),
    ]
    if not args.skip_s3fs:
        backends.append(_s3fs_backend(config, args, profile))

    for size_mib in args.sizes:
        _run_backends(backends, args, size_mib)


if __name__ == "__main__":
    main()
