# universal-pathlib

`universal-pathlib` exposes fsspec filesystems through a familiar `pathlib`
interface.

## Work with remote paths

```python
import fsspec
from upath import UPath

fsspec.config.conf["opendal"] = {"scheme": "memory"}
root = UPath("universal-pathlib", protocol="opendal")

folder = root / "results"
folder.mkdir(parents=True)
text_path = folder / "result.txt"
text_path.write_text("hello from UPath")

assert text_path.read_text() == "hello from UPath"
assert list(folder.iterdir()) == [text_path]
assert text_path.exists()
```

## Test coverage

The repository tests text and byte I/O, directory listing, and path existence.

See
[`tests/integration/universal_pathlib/test_upath.py`](https://github.com/fsspec/opendalfs/blob/main/tests/integration/universal_pathlib/test_upath.py).
