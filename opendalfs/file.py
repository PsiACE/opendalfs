from __future__ import annotations

import io
from typing import TYPE_CHECKING

from fsspec.asyn import AbstractAsyncStreamedFile
from fsspec.spec import AbstractBufferedFile
from opendal.exceptions import NotFound

if TYPE_CHECKING:
    from opendal.file import AsyncFile as OpendalAsyncFile
    from opendal.file import File as OpendalFile


class OpendalBufferedFile(AbstractBufferedFile):
    """Existing fsspec buffered read path backed by OpenDAL range reads."""

    def _fetch_range(self, start: int, end: int) -> bytes:
        if start >= end:
            return b""
        return self.fs.operator.read(self.path, offset=start, size=end - start)


class OpendalWriter(io.IOBase):
    """Thin write-only wrapper over an OpenDAL file."""

    def __init__(
        self,
        fs,
        path: str,
        mode: str,
        file: OpendalFile,
        size: int | None,
    ) -> None:
        self.fs = fs
        self.path = path
        self.mode = mode
        self._file = file
        self._size = size
        self._loc = size if mode == "ab" and size is not None else 0
        self._closed = file.closed

    @property
    def size(self) -> int | None:
        return self._size

    @property
    def closed(self) -> bool:  # type: ignore[override]
        return self._closed

    def readable(self) -> bool:
        return False

    def writable(self) -> bool:
        return self.mode in {"wb", "ab", "xb"} and not self.closed

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        try:
            return self._file.tell()
        except OSError:
            return self._loc

    def write(self, data: bytes | bytearray | memoryview) -> int:
        if not self.writable():
            raise ValueError("File not in write mode")
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if not isinstance(data, (bytes, bytearray, memoryview)):
            data = memoryview(data).tobytes()
        if not data:
            return 0
        written = self._file.write(data)
        if written is None:
            written = len(data)
        self._loc += written
        self._size = max(self._size or 0, self._loc)
        return written

    def flush(self) -> None:
        if self.closed:
            raise ValueError("Flush on closed file")
        self._file.flush()

    def close(self) -> None:
        if self.closed:
            return
        try:
            self._file.close()
        finally:
            self._closed = True
            self.fs.invalidate_cache(self.path)
            self.fs.invalidate_cache(self.fs._parent(self.path))

    def commit(self) -> None:
        self.close()

    def discard(self) -> None:
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class OpendalAsyncBufferedFile(AbstractAsyncStreamedFile):
    """Async buffered file implementation for OpenDAL."""

    _opendal_writer: OpendalAsyncFile | None
    _append_via_write: bool
    _initiated: bool
    _exclusive_create: bool

    def __init__(
        self,
        fs,
        path,
        mode="rb",
        block_size="default",
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        size=None,
        **kwargs,
    ):
        self._exclusive_create = mode == "xb"
        normalized_mode = "wb" if self._exclusive_create else mode
        super().__init__(
            fs,
            path,
            mode=normalized_mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            size=size,
            **kwargs,
        )

        self._opendal_writer = None
        self._append_via_write = False
        self._initiated = False

    async def _fetch_range(self, start: int, end: int):
        if start >= end:
            return b""

        length = end - start
        return await self.fs.async_fs.read(self.path, offset=start, size=length)

    async def _upload_chunk(self, final: bool = False):
        if not self._initiated:
            raise RuntimeError("Upload has not been initiated")

        self.buffer.seek(0)
        chunk = self.buffer.read()

        if not chunk:
            if not final:
                return False
            if self.mode == "ab" and self._append_via_write:
                if not await self.fs.async_fs.exists(self.path):
                    await self.fs.async_fs.write(self.path, b"")
                return None
            await self._commit_upload()
            return None

        if self.mode == "ab" and self._append_via_write:
            await self.fs.async_fs.write(self.path, chunk, append=True)
            return None

        if self._opendal_writer is None:
            self._opendal_writer = await self.fs.async_fs.open(self.path, "wb")

        await self._opendal_writer.write(chunk)

        if final:
            await self._commit_upload()
        return None

    async def _initiate_upload(self) -> None:
        if self._initiated:
            return

        if self._exclusive_create and await self.fs.async_fs.exists(self.path):
            raise FileExistsError(self.path)

        if self.mode == "ab":
            cap = self.fs.async_fs.capability()
            if getattr(cap, "write_can_append", False):
                self._append_via_write = True
                self.offset = self.loc
            else:
                try:
                    existing = await self.fs.async_fs.read(self.path)
                except (FileNotFoundError, NotFound):
                    existing = b""
                if existing:
                    self._opendal_writer = await self.fs.async_fs.open(self.path, "wb")
                    await self._opendal_writer.write(existing)
                    self.offset = len(existing)

        self._initiated = True

    async def _commit_upload(self) -> None:
        if self.mode == "ab" and self._append_via_write:
            return

        if self._opendal_writer is None:
            await self.fs.async_fs.write(self.path, b"")
            return

        try:
            await self._opendal_writer.close()
        finally:
            self._opendal_writer = None

    async def close(self):
        if self.closed:
            return

        try:
            await super().close()
        finally:
            if self._opendal_writer is not None:
                try:
                    await self._opendal_writer.close()
                finally:
                    self._opendal_writer = None
