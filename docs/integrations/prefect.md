# Prefect

<!-- docs-example-group: integration-prefect -->

Prefect's `RemoteFileSystem` block delegates storage access to fsspec and can
therefore use the configured `opendal` protocol.

## Read and write remote data

```python
import asyncio

import fsspec
from prefect.filesystems import RemoteFileSystem


async def main():
    protocol = "opendal"
    fs = fsspec.filesystem(protocol, scheme="memory", skip_instance_cache=True)
    basepath = "opendal://prefect/storage"
    remote = RemoteFileSystem(basepath=basepath, settings=fs.storage_options)

    path = await remote.write_path("result.txt", b"hello from Prefect")
    assert path == f"{basepath}/result.txt"
    assert await remote.read_path("result.txt") == b"hello from Prefect"


asyncio.run(main())
```

## Test coverage

The repository writes and reads a nested path through `RemoteFileSystem`. The
test runs against each configured OpenDAL test backend.

See
[`tests/integration/prefect/test_remote_filesystem.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/prefect/test_remote_filesystem.py).
