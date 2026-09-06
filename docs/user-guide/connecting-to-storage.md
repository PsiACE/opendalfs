# Connecting to storage

An `opendalfs` filesystem needs an OpenDAL service name and that service's
configuration. Choose the construction style that matches the library you are
using.

| Your application accepts | Use |
| --- | --- |
| A filesystem, mapping, or file-like object | `OpendalFileSystem("service", ...)` |
| An explicit OpenDAL URL | `opendal+s3://`, `opendal+gcs://`, or `opendal+azblob://` |
| An `s3://` URL | Register `S3FileSystem` with fsspec before using it |

## Construct the filesystem directly

Use {class}`opendalfs.OpendalFileSystem` when your code controls the filesystem
object:

```python
from opendalfs import OpendalFileSystem

fs = OpendalFileSystem(
    "s3",
    bucket="my-bucket",
    region="us-east-1",
    endpoint="https://s3.amazonaws.com",
)
```

The first argument selects the OpenDAL service. Remaining service-specific
keyword arguments are passed to the OpenDAL Python binding.

This also works for services without a URL adapter. For example, the local filesystem service needs no credentials:

```python
from tempfile import TemporaryDirectory

with TemporaryDirectory() as directory:
    fs = OpendalFileSystem("fs", root=directory)
    fs.pipe_file("hello.txt", b"Hello from OpenDAL!")
    assert fs.cat_file("hello.txt") == b"Hello from OpenDAL!"
```

Available services depend on the installed OpenDAL Python binding. Pass the filesystem, its mapping, or an opened file to libraries that accept these objects; no protocol registration is needed.

## Ask fsspec for a registered filesystem

The package installs fsspec entry points for S3, Google Cloud Storage, and Azure
Blob:

```python
import fsspec

fs = fsspec.filesystem(
    "opendal+s3",
    bucket="my-bucket",
    region="us-east-1",
)
```

## Use S3 URLs

Register `S3FileSystem` to use OpenDAL with `s3://` URLs and the supported s3fs configuration options:

```python
import fsspec
from opendalfs import S3FileSystem

fsspec.register_implementation("s3", S3FileSystem, clobber=True)
```

The registration replaces the S3 implementation for subsequent fsspec lookups in the current process. Run it during application startup, before constructing an S3 filesystem. Installing or importing `opendalfs` does not change `s3://`. Use `opendal+s3://` if you do not want to change the process-wide registration.

### Write and read a file

The following example uses a local MinIO service at `http://127.0.0.1:9000` with an existing `test-bucket` bucket. The default credentials are for local development only. Set the `OPENDAL_S3_*` environment variables to use your own endpoint, credentials, and bucket.

When working from this repository, `podman compose up -d --wait` starts the MinIO service. Create the bucket in the MinIO console at `http://localhost:9001` before running the example; the repository's documentation checks create their test bucket automatically.

```python
import os

bucket = os.environ.get("OPENDAL_S3_BUCKET", "test-bucket")
storage_options = {
    "key": os.environ.get("OPENDAL_S3_ACCESS_KEY_ID", "minioadmin"),
    "secret": os.environ.get("OPENDAL_S3_SECRET_ACCESS_KEY", "minioadmin"),
    "client_kwargs": {
        "endpoint_url": os.environ.get("OPENDAL_S3_ENDPOINT", "http://127.0.0.1:9000"),
        "region_name": os.environ.get("OPENDAL_S3_REGION", "us-east-1"),
    },
}
url = f"s3://{bucket}/docs/hello.txt"

with fsspec.open(url, "wt", **storage_options) as stream:
    stream.write("Hello from OpenDAL!")

with fsspec.open(url, "rt", **storage_options) as stream:
    assert stream.read() == "Hello from OpenDAL!"
```

The same `storage_options` can be passed to downstream libraries; see the {doc}`../integrations/pandas` example. The complete list of supported s3fs aliases is in {doc}`../reference/configuration`.

### Bucket scope and compatibility

An OpenDAL S3 operator is scoped to one bucket, so each `S3FileSystem` instance is also scoped to one bucket.
`url_to_fs` takes the bucket from the URL and returns a bucket-prefixed path:

```python
fs, path = fsspec.core.url_to_fs(url, **storage_options)
assert path == f"{bucket}/docs/hello.txt"
assert fs.cat_file(path) == b"Hello from OpenDAL!"

try:
    fs.cat_file(f"{bucket}-other/docs/hello.txt")
except ValueError:
    pass  # This filesystem cannot access another bucket.
else:
    raise AssertionError("A path in another bucket must be rejected")

```

Independent fsspec calls can use different buckets; create a separate filesystem for each bucket. When constructing `S3FileSystem` directly, pass `bucket=...` explicitly.

The adapter does not implement every s3fs option or feature. Only the aliases listed in the configuration reference are supported as s3fs options; unsupported `client_kwargs` raise `TypeError`.

## Understand OpenDAL URLs

The installed URL protocols use this form:

```text
opendal+<service>://<authority>/<path>
```

The authority supplies the bucket or container:

```text
opendal+s3://my-bucket/reports/2026.csv
opendal+gcs://my-bucket/reports/2026.csv
opendal+azblob://my-container/reports/2026.csv
```

These URLs use OpenDAL option names, not s3fs aliases. To read the same MinIO object written above:

```python
with fsspec.open(
    f"opendal+s3://{bucket}/docs/hello.txt",
    "rt",
    endpoint=storage_options["client_kwargs"]["endpoint_url"],
    region=storage_options["client_kwargs"]["region_name"],
    access_key_id=storage_options["key"],
    secret_access_key=storage_options["secret"],
) as stream:
    assert stream.read() == "Hello from OpenDAL!"

fs.rm(path)
```

Other OpenDAL services intentionally have no URL adapter.
Construct `OpendalFileSystem` directly and pass the filesystem, mapping, or opened file to the consuming library.
For application-defined URL protocols, use fsspec's `register_implementation()` with a filesystem class that implements the protocol's URL and path rules. Registration alone does not add those rules to `OpendalFileSystem`.

## Find service options

OpenDAL maintains the configuration reference for every service. Consult the
[OpenDAL service directory](https://opendal.apache.org/services/) for option
names, required fields, credential behavior, and backend-specific notes.

The installed `opendal+...` protocols pass OpenDAL option names through unchanged.
The opt-in `S3FileSystem` adapter accepts the common s3fs aliases listed in {doc}`../reference/configuration`.

Keep credentials outside source code. Read them from the provider's standard
environment, a secret manager, or environment variables that your application
passes into `storage_options`.
