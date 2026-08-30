import subprocess
import sys

import fsspec
import pytest

from opendalfs.registry import (
    OpendalAzBlobFileSystem,
    OpendalGCSFileSystem,
    OpendalS3FileSystem,
    S3FileSystem,
    register_opendal_standard_protocols,
)


def test_installed_service_protocols_resolve():
    from fsspec.registry import get_filesystem_class

    assert get_filesystem_class("opendal+s3") is OpendalS3FileSystem
    assert get_filesystem_class("opendal+gcs") is OpendalGCSFileSystem
    assert get_filesystem_class("opendal+azblob") is OpendalAzBlobFileSystem


def test_standard_s3_registration_replaces_existing_implementation(monkeypatch):
    import importlib

    from fsspec.implementations.memory import MemoryFileSystem

    registry = importlib.import_module("fsspec.registry")
    monkeypatch.setitem(registry._registry, "s3", MemoryFileSystem)

    assert register_opendal_standard_protocols() == ["s3"]
    assert registry.get_filesystem_class("s3") is S3FileSystem


def test_entry_points_resolve_in_an_isolated_process():
    code = """
import fsspec

assert fsspec.get_filesystem_class("s3").__module__ == "opendalfs.registry"

fs, path = fsspec.core.url_to_fs(
    "opendal:///isolated/item.bin",
    scheme="memory",
    skip_instance_cache=True,
)
fs.pipe_file(path, b"isolated")
assert fs.cat_file(path) == b"isolated"

try:
    fsspec.get_filesystem_class("opendal+memory")
except ValueError as error:
    assert "Protocol not known" in str(error)
else:
    raise AssertionError("non-Tier-0 services must use opendal://")
"""

    subprocess.run(
        [sys.executable, "-I", "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize(
    ("url", "options", "filesystem_type", "scope_option", "scope"),
    [
        (
            "opendal+s3://bucket/dir/file.txt",
            {"region": "us-east-1"},
            OpendalS3FileSystem,
            "bucket",
            "bucket",
        ),
        (
            "opendal+gcs://bucket/dir/file.txt",
            {},
            OpendalGCSFileSystem,
            "bucket",
            "bucket",
        ),
        (
            "opendal+azblob://container/dir/file.txt",
            {
                "endpoint": "https://account.blob.core.windows.net",
                "account_name": "account",
            },
            OpendalAzBlobFileSystem,
            "container",
            "container",
        ),
    ],
)
def test_explicit_service_url_selects_scope(
    url, options, filesystem_type, scope_option, scope
):
    fs, path = fsspec.core.url_to_fs(url, skip_instance_cache=True, **options)

    assert isinstance(fs, filesystem_type)
    assert fs.storage_options[scope_option] == scope
    assert path == f"{scope}/dir/file.txt"


def test_backend_key_can_start_with_the_authority(s3_fs, s3_config, tmp_path):
    fs = s3_fs
    directory = f"{s3_config.bucket}/bucket"
    source = f"{directory}/source.txt"
    copied = f"{directory}/copied.txt"
    moved = f"{directory}/moved.txt"

    fs.pipe_file(source, b"content")

    download = tmp_path / "source.txt"
    fs.get_file(source, download)
    fs.cp_file(source, copied)
    fs.mv(copied, moved)

    assert download.read_bytes() == b"content"
    assert fs.cat_file(moved) == b"content"
    assert fs.info(moved)["name"] == moved
    assert set(fs.ls(directory, detail=False)) == {source, moved}
