from __future__ import annotations

import gc
import hashlib
import json
import os
import random
import statistics
import time
import urllib.request
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

MIB = 1024 * 1024
DATASETS_FILE = Path(__file__).with_name("datasets.toml")


@dataclass(frozen=True, slots=True)
class MinioConfig:
    endpoint: str
    region: str
    bucket: str
    access_key_id: str
    secret_access_key: str

    @classmethod
    def from_env(cls) -> MinioConfig:
        return cls(
            endpoint=os.getenv("OPENDAL_S3_ENDPOINT", "http://127.0.0.1:9000"),
            region=os.getenv("OPENDAL_S3_REGION", "us-east-1"),
            bucket=os.getenv("OPENDAL_S3_BUCKET", "opendalfs-examples"),
            access_key_id=os.getenv("OPENDAL_S3_ACCESS_KEY_ID", "minioadmin"),
            secret_access_key=os.getenv("OPENDAL_S3_SECRET_ACCESS_KEY", "minioadmin"),
        )

    def opendal_options(self) -> dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "region": self.region,
            "access_key_id": self.access_key_id,
            "secret_access_key": self.secret_access_key,
        }

    def s3fs_options(self) -> dict[str, Any]:
        return {
            "key": self.access_key_id,
            "secret": self.secret_access_key,
            "client_kwargs": {
                "endpoint_url": self.endpoint,
                "region_name": self.region,
            },
            "use_ssl": self.endpoint.startswith("https://"),
        }

    def url(self, key: str, *, protocol: str = "s3") -> str:
        return f"{protocol}://{self.bucket}/{key.lstrip('/')}"


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    name: str
    url: str
    license: str
    source: str
    sha256: str | None = None


@dataclass(frozen=True, slots=True)
class DownloadResult:
    key: str
    size: int
    sha256: str
    reused: bool


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    label: str
    samples_s: tuple[float, ...]

    @property
    def median_s(self) -> float:
        return statistics.median(self.samples_s)

    @property
    def p95_s(self) -> float:
        ordered = sorted(self.samples_s)
        return ordered[min(len(ordered) - 1, round(0.95 * (len(ordered) - 1)))]


def dataset_spec(name: str) -> DatasetSpec:
    import tomllib

    datasets = tomllib.loads(DATASETS_FILE.read_text())["datasets"]
    try:
        raw = datasets[name]
    except KeyError as error:
        raise KeyError(f"Unknown example dataset {name!r}") from error
    return DatasetSpec(
        name=name,
        url=raw["url"],
        license=raw["license"],
        source=raw["source"],
        sha256=raw.get("sha256") or None,
    )


def ensure_minio(config: MinioConfig) -> None:
    import boto3
    from botocore.exceptions import ClientError, EndpointConnectionError

    client = boto3.client(
        "s3",
        endpoint_url=config.endpoint,
        region_name=config.region,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        config=boto3.session.Config(signature_version="s3v4"),
    )
    try:
        client.head_bucket(Bucket=config.bucket)
    except EndpointConnectionError as error:
        raise RuntimeError(
            f"MinIO is unavailable at {config.endpoint}. Start it with "
            "`podman compose up -d --wait`."
        ) from error
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status not in {400, 404}:
            raise
        client.create_bucket(Bucket=config.bucket)


def opendal_filesystem(config: MinioConfig):
    import fsspec

    return fsspec.filesystem(
        "opendal+s3",
        bucket=config.bucket,
        **config.opendal_options(),
    )


def standard_s3_filesystem(config: MinioConfig):
    import fsspec

    from opendalfs import register_opendal_standard_protocols

    register_opendal_standard_protocols()
    return fsspec.filesystem(
        "s3",
        bucket=config.bucket,
        **config.s3fs_options(),
    )


def sha256_file(fs, key: str) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with fs.open(key, "rb") as stream:
        while chunk := stream.read(MIB):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def fetch_to_minio(
    fs,
    spec: DatasetSpec,
    key: str,
    *,
    timeout: float = 60,
) -> DownloadResult:
    if fs.exists(key):
        digest, size = sha256_file(fs, key)
        if spec.sha256 is None or digest == spec.sha256:
            return DownloadResult(key=key, size=size, sha256=digest, reused=True)

    request = urllib.request.Request(  # noqa: S310
        spec.url,
        headers={"User-Agent": "opendalfs-marimo-examples/0.3"},
    )
    digest = hashlib.sha256()
    size = 0
    with (
        urllib.request.urlopen(request, timeout=timeout) as source,  # noqa: S310
        fs.open(key, "wb") as destination,
    ):
        while chunk := source.read(MIB):
            destination.write(chunk)
            digest.update(chunk)
            size += len(chunk)

    actual_sha256 = digest.hexdigest()
    if spec.sha256 is not None and actual_sha256 != spec.sha256:
        fs.rm_file(key)
        raise ValueError(
            f"Checksum mismatch for {spec.name}: expected {spec.sha256}, "
            f"received {actual_sha256}"
        )

    manifest = {
        "dataset": asdict(spec),
        "object": {"key": key, "size": size, "sha256": actual_sha256},
    }
    fs.pipe_file(
        f"{key}.source.json",
        json.dumps(manifest, indent=2, sort_keys=True).encode(),
    )
    return DownloadResult(
        key=key,
        size=size,
        sha256=actual_sha256,
        reused=False,
    )


def write_json(fs, key: str, value: Any) -> None:
    fs.pipe_file(key, json.dumps(value, indent=2, sort_keys=True).encode())


def clear_prefix(fs, prefix: str) -> None:
    if fs.exists(prefix):
        fs.rm(prefix, recursive=True)


def benchmark(
    cases: dict[str, Callable[[], Any]],
    *,
    repeat: int = 5,
    seed: int = 0,
) -> list[BenchmarkResult]:
    if repeat < 1:
        raise ValueError("repeat must be at least one")

    for case in cases.values():
        case()

    samples = {label: [] for label in cases}
    order = list(cases)
    generator = random.Random(seed)
    for _ in range(repeat):
        generator.shuffle(order)
        for label in order:
            gc.collect()
            started = time.perf_counter()
            cases[label]()
            samples[label].append(time.perf_counter() - started)

    return [
        BenchmarkResult(label=label, samples_s=tuple(values))
        for label, values in samples.items()
    ]


@contextmanager
def opened_local_copy(fs, key: str) -> Iterator[Path]:
    import tempfile

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory, Path(key).name)
        fs.get_file(key, path)
        yield path
