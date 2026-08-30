from __future__ import annotations

from typing import Any, ClassVar
from urllib.parse import urlsplit

from .compat import translate_s3fs_options
from .fs import OpendalFileSystem


class _OpendalServiceFileSystem(OpendalFileSystem):
    protocol: ClassVar[str]
    service: ClassVar[str]
    authority_option: ClassVar[str]

    @staticmethod
    def _adapt_storage_options(options: dict[str, Any]) -> dict[str, Any]:
        return options

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        service = kwargs.pop("scheme", type(self).service)
        if service != type(self).service:
            raise ValueError(
                f"Protocol {type(self).protocol!r} uses the "
                f"{type(self).service!r} service, not {service!r}"
            )
        storage_options = type(self)._adapt_storage_options(kwargs)
        super().__init__(type(self).service, *args, **storage_options)

    @property
    def _authority(self) -> str:
        return self.storage_options.get(type(self).authority_option, "")

    def _to_operator_path(self, path: str) -> str:
        path = super()._to_operator_path(path)
        # Service adapters expose authority/path, while the OpenDAL operator
        # is already scoped by the corresponding service option.
        authority = self._authority
        if path == authority:
            return ""
        if authority and path.startswith(f"{authority}/"):
            return path[len(authority) + 1 :]
        return path

    def _add_scope(self, path: str) -> str:
        authority = self._authority
        return f"{authority}/{path}" if path else authority

    async def _ls(self, path: str, detail=True, **kwargs):
        entries = await super()._ls(path, detail=detail, **kwargs)
        if not detail:
            return [self._add_scope(path) for path in entries]
        return [{**entry, "name": self._add_scope(entry["name"])} for entry in entries]

    async def _info(self, path: str, **kwargs):
        info = await super()._info(path, **kwargs)
        return {**info, "name": self._add_scope(info["name"])}

    def unstrip_protocol(self, name: str) -> str:
        path = self._add_scope(self._to_operator_path(name))
        return super().unstrip_protocol(path)

    @classmethod
    def _get_kwargs_from_urls(cls, path: str) -> dict[str, Any]:
        if "://" not in path:
            return {}

        parsed = urlsplit(path)
        if parsed.scheme != cls.protocol:
            return {}

        if not parsed.netloc:
            return {}
        return {cls.authority_option: parsed.netloc}


class OpendalS3FileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+s3"
    service = "s3"
    authority_option = "bucket"


class OpendalGCSFileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+gcs"
    service = "gcs"
    authority_option = "bucket"


class OpendalAzBlobFileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+azblob"
    service = "azblob"
    authority_option = "container"


class S3FileSystem(OpendalS3FileSystem):
    """OpenDAL S3 filesystem accepting the common s3fs constructor spelling."""

    protocol = "s3"
    _adapt_storage_options = staticmethod(translate_s3fs_options)

    def __init__(
        self, *args: Any, default_block_size: int | None = None, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        if default_block_size is not None:
            self.blocksize = default_block_size


_STANDARD_FILESYSTEMS: dict[str, type[_OpendalServiceFileSystem]] = {
    "s3": S3FileSystem,
}


def _register_filesystem(protocol: str, cls: type[OpendalFileSystem]) -> str:
    from fsspec.registry import get_filesystem_class, register_implementation

    register_implementation(protocol, cls, clobber=True)
    if get_filesystem_class(protocol) is not cls:
        raise RuntimeError(f"OpenDAL did not win registration for {protocol!r}")
    return protocol


def register_opendal_standard_protocols() -> list[str]:
    """Make OpenDAL win registration for supported standard fsspec protocols.

    Returns
    -------
    list of str
        Replaced protocol names in sorted order.
    """
    return sorted(
        _register_filesystem(protocol, cls)
        for protocol, cls in _STANDARD_FILESYSTEMS.items()
    )
