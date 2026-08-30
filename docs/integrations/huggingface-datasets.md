# Hugging Face Datasets

Hugging Face Datasets can load data from a configured `opendal://` URL in both
eager and streaming modes.

## Load JSON Lines from a URL

```python
from pathlib import Path

import fsspec
from datasets import load_dataset

protocol = "opendal"
storage_options = {
    "scheme": "fs",
    "root": str(Path("storage").resolve()),
}
data_url = "opendal:///datasets/records.jsonl"

with fsspec.open(data_url, "wb", **storage_options) as stream:
    stream.write(b'{"text":"first","label":0}\n')
    stream.write(b'{"text":"second","label":1}\n')

dataset = load_dataset(
    "json",
    data_files=data_url,
    split="train",
    cache_dir=Path("cache"),
    storage_options={protocol: storage_options},
)

assert list(dataset) == [
    {"text": "first", "label": 0},
    {"text": "second", "label": 1},
]
```

## Test coverage

The repository runs this case in eager and streaming modes against local
filesystem and S3-compatible backends.

See
[`tests/integration/huggingface_datasets/test_load_dataset.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/huggingface_datasets/test_load_dataset.py).
