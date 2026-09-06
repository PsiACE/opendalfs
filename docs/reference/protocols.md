# Protocol reference

`opendalfs` installs three fsspec entry points:

| Protocol | OpenDAL service | URL authority |
| --- | --- | --- |
| `opendal+s3` | `s3` | `bucket` |
| `opendal+gcs` | `gcs` | `bucket` |
| `opendal+azblob` | `azblob` | `container` |

## Direct construction

{class}`opendalfs.OpendalFileSystem` accepts the OpenDAL service as its first argument and supports services that do not have an installed URL protocol.

## Explicit OpenDAL URLs

The three `opendal+...` protocols fix the service.
Their authority supplies the bucket or container, while the remaining URL is the operator-relative path.
They accept OpenDAL option names unchanged.

## Opt-in S3 routing

{class}`opendalfs.S3FileSystem` can replace fsspec's S3 implementation when an application must keep existing `s3://` URLs:

```python
import fsspec
from opendalfs import S3FileSystem

fsspec.register_implementation("s3", S3FileSystem, clobber=True)
```

Installing or importing opendalfs does not replace fsspec's S3 implementation. The explicit registration above replaces it for subsequent lookups in the current process.
The adapter accepts the common s3fs constructor options listed in {doc}`configuration`.
Each adapter instance is scoped to one bucket; paths belonging to another bucket raise `ValueError`. This is not a complete replacement for every s3fs feature.

See {doc}`../user-guide/connecting-to-storage` for complete examples.
