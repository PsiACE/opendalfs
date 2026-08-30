# opendalfs

[![PyPI](https://img.shields.io/pypi/v/opendalfs)](https://pypi.org/project/opendalfs/)
[![Tests](https://github.com/fsspec/opendalfs/actions/workflows/tests.yml/badge.svg)](https://github.com/fsspec/opendalfs/actions/workflows/tests.yml)
[![License](https://img.shields.io/github/license/fsspec/opendalfs)](https://github.com/fsspec/opendalfs/blob/main/LICENSE)

`opendalfs` is a [fsspec](https://filesystem-spec.readthedocs.io/) filesystem
backed by [Apache OpenDAL](https://opendal.apache.org/). It lets Python libraries
that work with fsspec use storage services supported by OpenDAL.

**Documentation:** [opendalfs.readthedocs.io](https://opendalfs.readthedocs.io/)

## Installation

`opendalfs` requires Python 3.12 or newer.

```console
pip install opendalfs
```

## Quick start

The OpenDAL memory service provides a small example that needs no credentials
and writes nothing to disk:

```python
from opendalfs import OpendalFileSystem

fs = OpendalFileSystem("memory")
fs.pipe_file("hello.txt", b"hello from opendalfs\n")

assert fs.cat_file("hello.txt") == b"hello from opendalfs\n"
```

`OpendalFileSystem` implements the fsspec filesystem interface, including
methods such as `open`, `ls`, `glob`, `info`, and `rm`.

## Connect to storage

Existing S3 URLs can use OpenDAL without changing their protocol. Common
`s3fs` option names are translated to their OpenDAL equivalents:

```python
import fsspec

fs = fsspec.filesystem(
    "s3",
    bucket="my-bucket",
    key="access-key",
    secret="secret-key",
    client_kwargs={"region_name": "us-east-1"},
)
```

Use the generic `opendal` protocol when the service is supplied separately:

```python
fs, path = fsspec.core.url_to_fs(
    "opendal:///path/to/file",
    scheme="memory",
)
```

The URL contains only the path. It never infers a service from the URL
authority. The package also installs explicit entry points for S3, Google
Cloud Storage, and Azure Blob Storage:

```python
import fsspec

fs = fsspec.filesystem(
    "opendal+s3",
    bucket="my-bucket",
    region="us-east-1",
)
```

These protocols also work in URLs accepted by fsspec-compatible libraries:

```text
opendal+s3://my-bucket/path/to/file
opendal+gcs://my-bucket/path/to/file
opendal+azblob://my-container/path/to/file
```

All other OpenDAL services use the configured protocol. The service belongs in
configuration, not in a new URL scheme:

```python
fs, path = fsspec.core.url_to_fs(
    "opendal:///path/to/file",
    scheme="memory",
)
```

If another package has already loaded its `s3` implementation, call
`register_opendal_standard_protocols()` during application startup to make the
OpenDAL implementation the deterministic winner for the current process.

Explicit OpenDAL service options are passed without being renamed; only the
standard `s3` compatibility entry point translates common `s3fs` names. See the
[documentation](https://opendalfs.readthedocs.io/) for storage configuration,
URL rules, supported operations, tested integrations, and the API reference.

## Executable notebooks

The repository includes twenty marimo solution notebooks covering data
engineering, ML workflows, scientific arrays, geospatial data, and storage
benchmarks. They use public datasets and the root MinIO Compose service:

```console
podman compose up -d --wait
uv run --script \
  examples/marimo/01_pandas_penguins_landing.py
```

See the [Tutorials chapter](docs/user-guide/tutorials/index.md) for the lessons,
or [`examples/marimo`](examples/marimo) for their source and reproducibility
rules.

## Community

- Read the [contributing guide](https://github.com/fsspec/opendalfs/blob/main/CONTRIBUTING.md)
  to set up a development environment and submit changes.
- Open an [issue](https://github.com/fsspec/opendalfs/issues/new/choose) for bugs
  and feature requests.
- Use [GitHub Discussions](https://github.com/fsspec/opendalfs/discussions) for
  questions and general discussion.

## License

`opendalfs` is licensed under the
[Apache License 2.0](https://github.com/fsspec/opendalfs/blob/main/LICENSE).
