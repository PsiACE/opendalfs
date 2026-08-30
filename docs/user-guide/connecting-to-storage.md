# Connecting to storage

An `opendalfs` filesystem needs an OpenDAL service name and that service's
configuration. Choose the construction style that matches the library you are
using.

| Situation | Entry point | Where the service comes from |
| --- | --- | --- |
| Existing compatible URL | `s3://bucket/key` | fixed by the standard protocol |
| Explicit Tier-0 OpenDAL URL | `opendal+s3://bucket/key` | fixed by the installed protocol |
| Any OpenDAL service | `opendal:///key` | `scheme` in storage configuration |

Only installed Tier-0 protocols have an `opendal+...` form. Other services do
not create filesystem classes or register protocols at runtime.

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

## Upgrade an existing S3 URL

`opendalfs` registers the standard `s3` protocol, so existing S3 paths can use
OpenDAL without a URL rewrite:

```python
import fsspec

fs, path = fsspec.core.url_to_fs(
    "s3://my-bucket/reports/2026.csv",
    key="access-key",
    secret="secret-key",
    client_kwargs={
        "endpoint_url": "https://s3.example.com",
        "region_name": "us-east-1",
    },
)
```

The compatibility layer translates common `s3fs` spellings such as `key`,
`secret`, `token`, `anon`, `endpoint_url`, and supported `client_kwargs`.
Unsupported `s3fs`-specific behavior raises `TypeError` instead of being
silently ignored.

Python packaging permits more than one distribution to publish the same entry
point name. If another S3 implementation has already been loaded, make
OpenDAL the explicit winner during application startup:

```python
from opendalfs import register_opendal_standard_protocols

register_opendal_standard_protocols()
```

## Ask fsspec for an explicit OpenDAL filesystem

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

The explicit `opendal+s3` form accepts OpenDAL option names directly and never
depends on which package owns the standard `s3` protocol.

## Use the generic OpenDAL protocol

Use `opendal://` when a library needs a stable generic protocol and can pass
the OpenDAL service through `storage_options`:

```python
import fsspec

fs, path = fsspec.core.url_to_fs(
    "opendal:///cache/item.bin",
    scheme="memory",
)
fs.pipe_file(path, b"cached")
```

The URL carries only the path. The service and all service-specific settings
remain explicit configuration. For example, a configured S3 filesystem keeps
its bucket outside the generic URL:

```python
fs, path = fsspec.core.url_to_fs(
    "opendal:///reports/2026.csv",
    scheme="s3",
    bucket="my-bucket",
    region="us-east-1",
)
```

Prefer `s3://my-bucket/key` when a compatible standard protocol exists. Use an
installed `opendal+...` protocol when the service should be fixed by the URL.
Use `opendal://` for every other service. The generic form never selects a
service from a URL segment or changes the fsspec registry.

## Pass a URL to another library

Libraries such as pandas and Dask usually accept an fsspec URL. This complete
example uses the memory service so it can run without credentials:

```python
import fsspec
import pandas as pd

storage_options = {"scheme": "memory"}
with fsspec.open("opendal:///data/table.csv", "wb", **storage_options) as stream:
    stream.write(b"name,value\nalice,1\nbob,2\n")

frame = pd.read_csv(
    "opendal:///data/table.csv",
    storage_options=storage_options,
)
assert frame["value"].tolist() == [1, 2]
```

If a library accepts an fsspec URL but has no `storage_options` parameter,
configure the generic protocol before calling that library:

```python
import fsspec

fsspec.config.conf["opendal"] = {
    "scheme": "memory",
}
```

This configuration is process-wide. Use it during application startup; prefer
the library's local `storage_options` argument when one is available.

For the Tier-0 S3 service, use a URL such as
`opendal+s3://my-bucket/data/table.parquet` and pass options such as `region`
through `storage_options`. Do not repeat `bucket` in `storage_options` when it
is already present in the URL.

## Understand the URL

Tier-0 explicit URLs use these fixed forms:

```text
opendal+s3://<bucket>/<path>
opendal+gcs://<bucket>/<path>
opendal+azblob://<container>/<path>
```

The configured URL is service-independent:

```text
opendal:///<path>
```

Its three slashes preserve the root-relative path while leaving the authority
empty. The service, bucket or root, and credentials belong in keyword arguments
or `storage_options`, not in the URL query string.

## Find service options

OpenDAL maintains the configuration reference for every service. Consult the
[OpenDAL service directory](https://opendal.apache.org/services/) for option
names, required fields, credential behavior, and backend-specific notes.

The explicit Tier-0 and generic `opendal://` forms do not rename those options.
For example, OpenDAL's
`access_key_id`, `secret_access_key`, and `endpoint` options use the same names
when passed through fsspec. Only the standard `s3://` compatibility entry point
translates common `s3fs` option names.

Keep credentials outside source code. Read them from the provider's standard
environment, a secret manager, or environment variables that your application
passes into `storage_options`.

See {doc}`../reference/protocols` for the complete installed protocol list.
