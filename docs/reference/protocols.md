# Protocol reference

`opendalfs` supports native compatibility protocols, a configured generic
protocol, and explicit service protocols.

## Installed entry points

These protocols are available after installing `opendalfs`:

| Protocol | OpenDAL service | URL authority |
| --- | --- | --- |
| `s3` | `s3` | `bucket` |
| `opendal` | supplied as `scheme` | path |
| `opendal+s3` | `s3` | `bucket` |
| `opendal+gcs` | `gcs` | `bucket` |
| `opendal+azblob` | `azblob` | `container` |

The native `s3` entry point translates common `s3fs` constructor options. Call
{func}`opendalfs.register_opendal_native_protocols` to overwrite an S3
implementation that another package loaded earlier in the same process.

## Generic protocol

The generic protocol keeps the service out of the URL:

```python
import fsspec

fs, path = fsspec.core.url_to_fs(
    "opendal:///path/to/file",
    scheme="memory",
)
```

The `scheme` and all service options are explicit configuration. No URL segment
is interpreted as a service, and resolving the URL never registers another
protocol.

## Explicit and runtime protocols

{func}`opendalfs.register_opendal_service` creates an fsspec implementation
for another OpenDAL service. The following services have an authority mapping
known to `opendalfs`:

| OpenDAL service | URL authority maps to |
| --- | --- |
| `aliyun-drive` | `drive_type` |
| `azblob` | `container` |
| `b2` | `bucket` |
| `cos` | `bucket` |
| `gcs` | `bucket` |
| `obs` | `bucket` |
| `oss` | `bucket` |
| `s3` | `bucket` |
| `tos` | `bucket` |
| `upyun` | `bucket` |

Other registered services use an empty URL authority and a root-relative path:

```text
opendal+memory:///path/to/file
```

## URL reconstruction

`strip_protocol` converts an fsspec URL into a filesystem-facing path.
`unstrip_protocol` restores the protocol and, for a scoped service, its
authority. Composite fsspec operations depend on this round trip when they
expand globs or recurse into directories.

See {doc}`../user-guide/connecting-to-storage` for an explanation of the path
model and construction styles.
