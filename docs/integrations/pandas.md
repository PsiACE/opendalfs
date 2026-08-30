# pandas

pandas accepts fsspec URLs through `storage_options`. Its Parquet methods can
also accept an explicit fsspec filesystem.

## Read a CSV from a URL

Configure the memory service and create a small input file. The same URL
pattern works for every OpenDAL service.

```python
import fsspec
import pandas as pd

storage_options = {"scheme": "memory"}
fs = fsspec.filesystem("opendal", **storage_options)
fs.pipe_file("data/events.csv", b"name,value\nalice,1\nbob,2\n")

frame = pd.read_csv(
    "opendal:///data/events.csv",
    storage_options=storage_options,
)
assert frame["value"].tolist() == [1, 2]
```

For object stores, the bucket comes from the URL and other service options pass
through `storage_options` to OpenDAL.

## Read and write Parquet with a filesystem

```python
path = "data/events.parquet"
frame.to_parquet(path, filesystem=fs, engine="pyarrow")
result = pd.read_parquet(path, filesystem=fs, engine="pyarrow")

pd.testing.assert_frame_equal(result, frame)
```

See {doc}`../user-guide/connecting-to-storage` for path and authority handling
with bucket-scoped services.

## Test coverage

The repository tests:

- CSV reads from a configured `opendal://` URL
- Parquet URL round trips through PyArrow
- Parquet round trips with an explicit filesystem

The test runs against memory, local filesystem, and MinIO-backed S3 fixtures.
See
[`tests/integration/pandas/test_pandas.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/pandas/test_pandas.py).
