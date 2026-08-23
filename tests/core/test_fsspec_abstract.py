import posixpath

import pytest
from fsspec.tests.abstract import AbstractFixtures
from fsspec.tests.abstract.copy import AbstractCopyTests
from fsspec.tests.abstract.get import AbstractGetTests
from fsspec.tests.abstract.open import AbstractOpenTests
from fsspec.tests.abstract.pipe import AbstractPipeTests
from fsspec.tests.abstract.put import AbstractPutTests

from opendalfs import OpendalFileSystem


class TestMemoryFsspecContract(
    AbstractFixtures,
    AbstractCopyTests,
    AbstractGetTests,
    AbstractPutTests,
    AbstractPipeTests,
    AbstractOpenTests,
):
    """Run fsspec's reusable filesystem contract against OpenDAL memory."""

    @pytest.fixture
    def fs(self):
        return OpendalFileSystem(
            scheme="memory",
            asynchronous=False,
            skip_instance_cache=True,
        )

    @pytest.fixture
    def fs_join(self):
        return posixpath.join

    @pytest.fixture
    def fs_path(self):
        return "contract"


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
