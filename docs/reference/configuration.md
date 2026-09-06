# Configuration reference

`OpendalFileSystem` accepts adapter settings, fsspec settings, and OpenDAL
service settings in one constructor.

## Adapter settings

| Option | Default | Meaning |
| --- | --- | --- |
| `scheme` | required | OpenDAL service name, such as `s3` or `memory` |
| `retries` | `5` | Maximum attempts configured on the OpenDAL retry layer |
| `write_concurrent` | `8` | Concurrent part uploads for opened writers |
| `write_chunk` | backend default | Part size in bytes for opened writers |

`write_concurrent` and `write_chunk` can also be passed to `fs.open` for one
writer.

## fsspec settings

| Option | Default | Meaning |
| --- | --- | --- |
| `asynchronous` | `False` | Use the async implementation directly |
| `loop` | fsspec default | Event loop used by sync wrappers |
| `batch_size` | fsspec default | Maximum concurrent batch operations |
| `use_listings_cache` | `True` | Cache directory listings |
| `listings_expiry_time` | no expiry | Expire cached listings after this many seconds |
| `max_paths` | unlimited | Maximum cached directory listings |
| `skip_instance_cache` | `False` | Bypass fsspec filesystem instance reuse |

Additional arguments accepted by {class}`fsspec.asyn.AsyncFileSystem` pass to
its constructor.

## Service settings

All remaining keyword arguments configure the OpenDAL service:

```python
from opendalfs import OpendalFileSystem

fs = OpendalFileSystem(
    "s3",
    bucket="my-bucket",
    endpoint="https://s3.amazonaws.com",
    region="us-east-1",
)
```

Use the [OpenDAL service directory](https://opendal.apache.org/services/) as the
configuration reference. Option names pass through unchanged.

## S3 compatibility options

The `S3FileSystem` adapter for `s3://` accepts the following `s3fs` aliases. These aliases do not apply to `OpendalFileSystem` or the `opendal+...` protocols, which use OpenDAL option names.

| s3fs option | OpenDAL S3 option |
| --- | --- |
| `key` | `access_key_id` |
| `secret` | `secret_access_key` |
| `token` | `session_token` |
| `anon` | `skip_signature` |
| `endpoint_url` | `endpoint` |
| `requester_pays` | `enable_request_payer` |
| `client_kwargs.aws_access_key_id` | `access_key_id` |
| `client_kwargs.aws_secret_access_key` | `secret_access_key` |
| `client_kwargs.aws_session_token` | `session_token` |
| `client_kwargs.endpoint_url` | `endpoint` |
| `client_kwargs.region_name` | `region` |

OpenDAL option names take precedence when both forms are provided.
Unsupported nested `client_kwargs` raise `TypeError`.
`client_kwargs` must be a mapping. Top-level aliases take precedence over aliases inside `client_kwargs`.

`default_block_size` sets the filesystem's default block size in bytes. It is an adapter setting, not an OpenDAL service option. Other s3fs-specific options and features are not guaranteed to be compatible.

## URL-derived settings

Registered service adapters can derive one setting from the URL authority. For
example, `opendal+s3://my-bucket/path` supplies `bucket="my-bucket"`.
Explicit filesystem construction requires the bucket keyword instead.

For `s3://my-bucket/path`, `S3FileSystem` uses `my-bucket` even when a different `bucket` is present in the storage options. Paths passed to this filesystem include the bucket, such as `my-bucket/path`; the underlying OpenDAL operator receives the bucket-relative key.

Do not provide conflicting values through the URL and keyword arguments.
