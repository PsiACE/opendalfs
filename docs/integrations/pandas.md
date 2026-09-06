# pandas

pandas accepts file-like objects and explicit fsspec filesystems.
Its URL APIs can also use the installed `opendal+s3`, `opendal+gcs`, and `opendal+azblob` protocols.

## Read a CSV from a file-like object

Construct any OpenDAL service directly and pass its opened file to pandas:

```python
import pandas as pd
from opendalfs import OpendalFileSystem

fs = OpendalFileSystem("memory")
fs.pipe_file("data/events.csv", b"name,value\nalice,1\nbob,2\n")

with fs.open("data/events.csv", "rb") as stream:
    frame = pd.read_csv(stream)
assert frame["value"].tolist() == [1, 2]
```

## Read and write Parquet with a filesystem

```python
path = "data/events.parquet"
frame.to_parquet(path, filesystem=fs, engine="pyarrow")
result = pd.read_parquet(path, filesystem=fs, engine="pyarrow")

pd.testing.assert_frame_equal(result, frame)
```

See {doc}`../user-guide/connecting-to-storage` for path and authority handling
with bucket-scoped services.

## Read and write CSV with S3 URLs

Register `S3FileSystem` before using pandas with `s3://` URLs. This selects OpenDAL for subsequent fsspec S3 lookups in the current process; it is not required for `opendal+s3://` URLs.

This example uses the local MinIO service and existing bucket described in {doc}`../user-guide/connecting-to-storage`. Set the environment variables below to connect to your own storage. The default credentials are only for local development.

```python
import os

import fsspec
import pandas as pd
from opendalfs import S3FileSystem

fsspec.register_implementation("s3", S3FileSystem, clobber=True)

bucket = os.environ.get("OPENDAL_S3_BUCKET", "test-bucket")
storage_options = {
    "key": os.environ.get("OPENDAL_S3_ACCESS_KEY_ID", "minioadmin"),
    "secret": os.environ.get("OPENDAL_S3_SECRET_ACCESS_KEY", "minioadmin"),
    "client_kwargs": {
        "endpoint_url": os.environ.get("OPENDAL_S3_ENDPOINT", "http://127.0.0.1:9000"),
        "region_name": os.environ.get("OPENDAL_S3_REGION", "us-east-1"),
    },
}
url = f"s3://{bucket}/docs/scores.csv"
frame = pd.DataFrame({"name": ["Alice", "Bob"], "score": [90, 85]})

frame.to_csv(url, index=False, storage_options=storage_options)
result = pd.read_csv(url, storage_options=storage_options)
pd.testing.assert_frame_equal(result, frame)

fs, path = fsspec.core.url_to_fs(url, **storage_options)
fs.rm(path)
```

Each OpenDAL S3 filesystem is scoped to one bucket. See {doc}`../reference/configuration` for the supported s3fs configuration options; not every s3fs feature is available.

## Test coverage

The repository tests:

- CSV and Parquet URL operations through `opendal+s3`
- Parquet round trips with an explicit filesystem

The documentation example checker also executes the `s3://` CSV round trip above.

The test runs against memory, local filesystem, and MinIO-backed S3 fixtures.
See
[`tests/integration/pandas/test_pandas.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/pandas/test_pandas.py).
