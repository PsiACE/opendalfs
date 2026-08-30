# Connecting to storage

An `opendalfs` filesystem needs an OpenDAL service name and that service's
configuration. Choose the construction style that matches the library you are
using.

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

`opendalfs` registers the native `s3` protocol, so existing S3 paths can use
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
from opendalfs import register_opendal_native_protocols

register_opendal_native_protocols(["s3"])
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
depends on which package owns the native `s3` protocol.

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

Prefer `s3://my-bucket/key` when a native protocol exists. Use
`opendal+<service>://` when the service should be visible in the protocol.
The generic form never selects a service from a URL segment.

## Register another OpenDAL service

Other OpenDAL services can be registered for the current Python process. The
memory service keeps this example credential-free:

```python
from opendalfs import register_opendal_service

protocol = register_opendal_service("memory")
fs = fsspec.filesystem(protocol)
```

Register several services together when an application needs them during
startup:

```python
from opendalfs import register_opendal_protocols

protocols = register_opendal_protocols(["memory", "fs", "oss"])
```

Repeating a registration is safe. Libraries that create their filesystem while
loading a dataset, catalog, or path object need registration to happen first.

## Pass a URL to another library

Libraries such as pandas and Dask usually accept an fsspec URL. This complete
example uses the memory service so it can run without credentials:

```python
import fsspec
import pandas as pd

from opendalfs import register_opendal_service

protocol = register_opendal_service("memory")
fs = fsspec.filesystem(protocol)
fs.pipe_file("data/table.csv", b"name,value\nalice,1\nbob,2\n")

frame = pd.read_csv("opendal+memory:///data/table.csv")
assert frame["value"].tolist() == [1, 2]
```

For a configured S3 service, use a URL such as
`opendal+s3://my-bucket/data/table.parquet` and pass options such as `region`
through `storage_options`. Do not repeat `bucket` in `storage_options` when it
is already present in the URL.

## Understand the URL

An `opendalfs` URL uses this form:

```text
opendal+<service>://<authority>/<path>
```

Object stores use the authority as their bucket or container:

```text
opendal+s3://my-bucket/reports/2026.csv
opendal+gcs://my-bucket/reports/2026.csv
opendal+azblob://my-container/reports/2026.csv
```

Services without a bucket-like scope use a hostless URL:

```text
opendal+memory:///cache/item.bin
opendal+fs:///reports/2026.csv
```

The three slashes preserve the root-relative path while leaving the authority
empty. Service options belong in keyword arguments or `storage_options`, not in
the URL query string.

## Find service options

OpenDAL maintains the configuration reference for every service. Consult the
[OpenDAL service directory](https://opendal.apache.org/services/) for option
names, required fields, credential behavior, and backend-specific notes.

The explicit `opendal+<service>` and generic `opendal://` forms do not rename
those options. For example, OpenDAL's
`access_key_id`, `secret_access_key`, and `endpoint` options use the same names
when passed through fsspec. Only the native `s3://` compatibility entry point
translates common `s3fs` option names.

Keep credentials outside source code. Read them from the provider's standard
environment, a secret manager, or environment variables that your application
passes into `storage_options`.

See {doc}`../reference/protocols` for the full list of authority mappings known
to the runtime registration helper.
