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

The package registers fsspec protocols for S3, Google Cloud Storage, and Azure
Blob Storage:

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

For other services, construct `OpendalFileSystem("service", ...)` directly and pass the filesystem, a mapping, or an opened file to the consuming library. Available services depend on the installed OpenDAL Python binding.

### Use `s3://` URLs

Explicitly register `S3FileSystem` with fsspec to use `s3://` URLs with OpenDAL:

```python
import fsspec
from opendalfs import S3FileSystem

fsspec.register_implementation("s3", S3FileSystem, clobber=True)
```

Register during application startup, before constructing an S3 filesystem. Registration replaces the S3 implementation for subsequent fsspec lookups in the current process; installing or importing `opendalfs` does not change it automatically.

You can reuse supported s3fs options, including `key`, `secret`, `token`, `anon`, `endpoint_url`, and selected `client_kwargs`. Each instance is scoped to one bucket; the adapter does not support every s3fs option or feature.

`OpendalFileSystem` and the `opendal+...` protocols use OpenDAL option names unchanged. See [Connecting to storage](https://opendalfs.readthedocs.io/en/latest/user-guide/connecting-to-storage.html) for complete examples and the [configuration reference](https://opendalfs.readthedocs.io/en/latest/reference/configuration.html) for supported options.

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
