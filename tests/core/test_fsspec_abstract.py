"""Behavior contracts shared by fsspec and OpenDAL-backed filesystems."""

import posixpath

import pytest
from fsspec.tests.abstract import AbstractFixtures
from fsspec.tests.abstract.copy import AbstractCopyTests
from fsspec.tests.abstract.get import AbstractGetTests
from fsspec.tests.abstract.open import AbstractOpenTests
from fsspec.tests.abstract.pipe import AbstractPipeTests
from fsspec.tests.abstract.put import AbstractPutTests

from opendalfs import OpendalFileSystem


class _OpendalFixtures(AbstractFixtures):
    """Shared fixtures for fsspec contracts backed by OpenDAL."""

    @pytest.fixture
    def fs_join(self):
        return posixpath.join

    @pytest.fixture
    def fs_path(self):
        return "contract"

    @pytest.fixture
    def supports_empty_directories(self, fs):
        return fs.async_fs.capability().create_dir


class _MemoryFixtures(_OpendalFixtures):
    """Configure OpenDAL memory for each cached and uncached contract case."""

    @pytest.fixture(params=[True, False], ids=["cached", "uncached"])
    def fs(self, request):
        return OpendalFileSystem(
            scheme="memory",
            asynchronous=False,
            skip_instance_cache=True,
            use_listings_cache=request.param,
        )


class _S3Fixtures(_OpendalFixtures):
    """Configure OpenDAL S3 against MinIO for contract cases."""

    @pytest.fixture(params=[True, False], ids=["cached", "uncached"])
    def fs(self, s3_fs, request):
        s3_fs.dircache.use_listings_cache = request.param
        return s3_fs


class TestMemoryCopy(AbstractCopyTests, _MemoryFixtures):
    pass


class TestMemoryGet(AbstractGetTests, _MemoryFixtures):
    pass


class TestMemoryPut(AbstractPutTests, _MemoryFixtures):
    pass


class TestMemoryPipe(AbstractPipeTests, _MemoryFixtures):
    pass


class TestMemoryOpen(AbstractOpenTests, _MemoryFixtures):
    pass


class TestS3Copy(AbstractCopyTests, _S3Fixtures):
    pass


class TestS3Get(AbstractGetTests, _S3Fixtures):
    pass


class TestS3Put(AbstractPutTests, _S3Fixtures):
    pass


class TestS3Pipe(AbstractPipeTests, _S3Fixtures):
    pass


class TestS3Open(AbstractOpenTests, _S3Fixtures):
    pass


def test_memory_directory_semantics(memory_fs):
    memory_fs.makedirs("empty/nested")

    assert memory_fs.isdir("empty")
    assert memory_fs.isdir("empty/nested")
    assert memory_fs.ls("empty/nested") == []

    memory_fs.touch("empty/file")
    with pytest.raises(OSError, match="Directory not empty"):
        memory_fs.rmdir("empty")
    assert memory_fs.isfile("empty/file")


def test_memory_change_tokens_survive_listing_cache(memory_fs):
    memory_fs.pipe_file("cached", b"old")
    memory_fs.ls("")
    old_checksum = memory_fs.checksum("cached")
    old_ukey = memory_fs.ukey("cached")

    memory_fs.pipe_file("cached", b"new")

    assert memory_fs.cat_file("cached") == b"new"
    assert memory_fs.checksum("cached") != old_checksum
    assert memory_fs.ukey("cached") != old_ukey


def test_s3_recursive_copy_preserves_opendal_directories(s3_fs):
    assert s3_fs.async_fs.capability().create_dir
    s3_fs.makedirs("source/empty")

    s3_fs.copy("source", "target", recursive=True)

    assert s3_fs.isdir("target/empty")


def test_s3_change_tokens_use_opendal_metadata(s3_fs, monkeypatch):
    s3_fs.pipe_file("metadata-token", b"data")
    info = s3_fs.info("metadata-token", refresh=True)
    assert info.get("etag")

    def fail_content_read(*args, **kwargs):
        raise AssertionError("change tokens should use OpenDAL metadata")

    monkeypatch.setattr(s3_fs, "cat_file", fail_content_read)

    assert s3_fs.checksum("metadata-token")
    assert s3_fs.ukey("metadata-token")
