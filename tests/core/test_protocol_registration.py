import pickle
import subprocess
import sys

import fsspec

from opendalfs import OpendalFileSystem
from opendalfs.registry import (
    OpendalAzBlobFileSystem,
    OpendalGCSFileSystem,
    OpendalNativeS3FileSystem,
    OpendalS3FileSystem,
    _OpendalServiceFileSystem,
    register_opendal_native_protocols,
    register_opendal_protocols,
    register_opendal_service,
)


class _ScopedMemoryFileSystem(_OpendalServiceFileSystem):
    """Memory backend with a bucket-shaped external namespace for path tests."""

    protocol = "opendal+memory"
    _authority_option = "bucket"

    def __init__(self, *args, **kwargs):
        kwargs.pop("bucket")
        super().__init__(*args, **kwargs)


def test_register_default_protocols():
    from fsspec.registry import get_filesystem_class

    registered = register_opendal_protocols()
    assert registered == ["opendal+azblob", "opendal+gcs", "opendal+s3"]

    assert get_filesystem_class("opendal+s3") is OpendalS3FileSystem
    assert get_filesystem_class("opendal+gcs") is OpendalGCSFileSystem
    assert get_filesystem_class("opendal+azblob") is OpendalAzBlobFileSystem


def test_native_s3_registration_replaces_existing_implementation(monkeypatch):
    import importlib

    from fsspec.implementations.memory import MemoryFileSystem

    registry = importlib.import_module("fsspec.registry")
    monkeypatch.setitem(registry._registry, "s3", MemoryFileSystem)

    assert register_opendal_native_protocols() == ["s3"]
    assert registry.get_filesystem_class("s3") is OpendalNativeS3FileSystem


def test_entry_points_resolve_in_an_isolated_process():
    code = """
import fsspec

from opendalfs.registry import (
    OpendalNativeS3FileSystem,
)
from opendalfs import OpendalFileSystem

assert fsspec.get_filesystem_class("s3") is OpendalNativeS3FileSystem
assert fsspec.get_filesystem_class("opendal") is OpendalFileSystem

fs, path = fsspec.core.url_to_fs(
    "opendal:///isolated/item.bin",
    scheme="memory",
    skip_instance_cache=True,
)
fs.pipe_file(path, b"isolated")
assert fs.cat_file(path) == b"isolated"
"""

    subprocess.run(
        [sys.executable, "-I", "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )


def test_strip_protocol_and_kwargs():
    assert (
        OpendalS3FileSystem._strip_protocol("opendal+s3://bucket/dir/file.txt")
        == "bucket/dir/file.txt"
    )
    assert OpendalS3FileSystem._get_kwargs_from_urls(
        "opendal+s3://bucket/dir/file.txt"
    ) == {"bucket": "bucket"}
    assert OpendalS3FileSystem._get_kwargs_from_urls(
        "opendal+s3://bucket/dir/file.txt?bucket=other&region=elsewhere"
    ) == {"bucket": "bucket"}

    assert (
        OpendalAzBlobFileSystem._strip_protocol(
            "opendal+azblob://container/dir/file.txt"
        )
        == "container/dir/file.txt"
    )
    assert OpendalAzBlobFileSystem._get_kwargs_from_urls(
        "opendal+azblob://container/dir/file.txt"
    ) == {"container": "container"}


def test_dynamic_service_registration_uses_opendal_authority_option():
    from fsspec.registry import get_filesystem_class

    protocol = register_opendal_service("oss")
    assert protocol == "opendal+oss"

    cls = get_filesystem_class(protocol)
    assert cls.protocol == protocol
    assert cls._strip_protocol("opendal+oss://bucket/dir/file.txt") == (
        "bucket/dir/file.txt"
    )
    assert cls._get_kwargs_from_urls("opendal+oss://bucket/dir/file.txt") == {
        "bucket": "bucket"
    }


def test_dynamic_service_registration_does_not_guess_authority_option():
    from fsspec.registry import get_filesystem_class

    protocol = register_opendal_service("webdav")
    cls = get_filesystem_class(protocol)

    assert cls._strip_protocol("opendal+webdav://host/dir/file.txt") == (
        "/host/dir/file.txt"
    )
    assert cls._get_kwargs_from_urls("opendal+webdav://host/dir/file.txt") == {}


def test_empty_service_registration_fails_early():
    import pytest

    with pytest.raises(ValueError, match="cannot be empty"):
        register_opendal_service("")


def test_opendal_protocol_uses_explicit_service_configuration():
    from fsspec.registry import register_implementation

    register_implementation("opendal", OpendalFileSystem, clobber=True)
    fs, path = fsspec.core.url_to_fs(
        "opendal:///data/item.bin",
        scheme="memory",
        skip_instance_cache=True,
    )

    assert isinstance(fs, OpendalFileSystem)
    assert fs.scheme == "memory"
    assert path == "/data/item.bin"
    assert fs.unstrip_protocol(path) == "opendal:///data/item.bin"

    fs.pipe_file(path, b"generic")
    assert fs.cat_file(path) == b"generic"
    assert fs.info(path)["name"] == path.lstrip("/")


def test_dynamic_filesystem_is_pickleable():
    protocol = register_opendal_service("memory")
    fs = fsspec.filesystem(protocol, skip_instance_cache=True)

    restored = pickle.loads(pickle.dumps(fs))  # noqa: S301

    assert restored.protocol == protocol
    assert restored.service_name == "memory"


def test_dynamic_service_paths_without_authority_match_fsspec_memory():
    from fsspec.implementations.memory import MemoryFileSystem
    from fsspec.registry import get_filesystem_class

    protocol = register_opendal_service("memory")
    cls = get_filesystem_class(protocol)
    assert cls._strip_protocol(["opendal+memory:///one", "opendal+memory://two"]) == [
        "/one",
        "/two",
    ]

    opendal_fs = cls(skip_instance_cache=True)
    memory_fs = MemoryFileSystem(skip_instance_cache=True)
    root = "integration/path-contract"

    def path_behavior(fs):
        path = fs._strip_protocol(fs.unstrip_protocol(root))
        file_path = f"{path}/one.txt"
        nested_path = f"{path}/nested/two.txt"
        fs.pipe_file(file_path, b"one")
        fs.pipe_file(nested_path, b"two")
        behavior = {
            "path": path,
            "name": fs.info(file_path)["name"],
            "find": fs.find(path),
            "walk": list(fs.walk(path)),
        }
        fs.rm_file(file_path)
        behavior["find_after_rm"] = fs.find(path)
        return behavior

    assert path_behavior(opendal_fs) == path_behavior(memory_fs)


def test_backend_key_can_start_with_the_authority(tmp_path):
    fs = _ScopedMemoryFileSystem(bucket="bucket", skip_instance_cache=True)
    directory = "bucket/bucket"
    source = f"{directory}/source.txt"
    copied = f"{directory}/copied.txt"
    moved = f"{directory}/moved.txt"

    fs.pipe_file(source, b"content")
    with fs.open(source, "ab") as source_file:
        assert source_file.path == source
        source_file.write(b" appended")

    download = tmp_path / "source.txt"
    fs.get_file(source, download)
    fs.cp_file(source, copied)
    fs.mv(copied, moved)

    assert download.read_bytes() == b"content appended"
    assert fs.cat_file(moved) == b"content appended"
    assert fs.info(moved)["name"] == moved
    assert set(fs.ls(directory, detail=False)) == {source, moved}
