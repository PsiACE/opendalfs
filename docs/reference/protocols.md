# Protocol reference

`opendalfs` supports standard compatibility protocols, a configured generic
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

The standard `s3` entry point translates common `s3fs` constructor options. Call
{func}`opendalfs.register_opendal_standard_protocols` to overwrite an S3
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

## Tier-0 explicit protocols

The package only publishes explicit service protocols whose URL behavior it
supports as a stable contract:

| Protocol | URL authority maps to |
| --- | --- |
| `opendal+s3` | `bucket` |
| `opendal+gcs` | `bucket` |
| `opendal+azblob` | `container` |

There is no runtime `opendal+<service>` generation. Use the configured generic
protocol for every other OpenDAL service:

```text
opendal:///path/to/file
```

and pass `scheme="memory"`, `scheme="oss"`, or another supported OpenDAL
service in storage configuration.

## URL reconstruction

`strip_protocol` converts an fsspec URL into a filesystem-facing path.
`unstrip_protocol` restores the protocol and, for a scoped service, its
authority. Composite fsspec operations depend on this round trip when they
expand globs or recurse into directories.

See {doc}`../user-guide/connecting-to-storage` for an explanation of the path
model and construction styles.
