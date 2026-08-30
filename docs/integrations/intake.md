# Intake

Intake catalogs can point to data using configured `opendal://` URLs, including paths
relative to the catalog itself.

## Open a CSV catalog

```python
import fsspec
import intake
import pandas as pd

fsspec.config.conf["opendal"] = {"scheme": "memory"}
catalog_url = "opendal:///intake/catalog.yml"
data_url = "opendal:///intake/data.csv"

with fsspec.open(data_url, "wt") as stream:
    pd.DataFrame({"value": [1, 2]}).to_csv(stream, index=False)

with fsspec.open(catalog_url, "wt") as stream:
    stream.write(
        """\
sources:
  data:
    driver: csv
    args:
      urlpath: '{{CATALOG_DIR}}/data.csv'
"""
    )

catalog = intake.open_catalog(catalog_url)
assert catalog.data.read()["value"].tolist() == [1, 2]
```

## Test coverage

The repository tests relative and full `opendal://` URLs in one catalog.

See
[`tests/integration/intake/test_catalog.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/intake/test_catalog.py).
