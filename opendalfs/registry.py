from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar
from urllib.parse import urlsplit

from .fs import OpendalFileSystem

# OpenDAL services whose native URI maps its authority directly to one option.
_AUTHORITY_OPTION_BY_SERVICE = {
    "aliyun-drive": "drive_type",
    "azblob": "container",
    "b2": "bucket",
    "cos": "bucket",
    "gcs": "bucket",
    "obs": "bucket",
    "oss": "bucket",
    "s3": "bucket",
    "tos": "bucket",
    "upyun": "bucket",
}


class _OpendalServiceFileSystem(OpendalFileSystem):
    protocol: ClassVar[str]
    _authority_option: ClassVar[str | None] = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.pop("scheme", None)
        service = type(self).protocol.removeprefix("opendal+")
        super().__init__(service, *args, **kwargs)

    @property
    def _authority(self) -> str:
        if self._authority_option is None:
            return ""
        return self.storage_options.get(self._authority_option, "")

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, list):
            return super()._strip_protocol(path)

        path = super()._strip_protocol(path)
        if cls._authority_option is None and path:
            return f"/{path.lstrip('/')}"
        return path

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

    def _add_authority(self, path: str) -> str:
        authority = self._authority
        return f"{authority}/{path}" if path else authority

    async def _ls(self, path: str, detail=True, **kwargs):
        entries = await super()._ls(path, detail=detail, **kwargs)
        if not detail:
            return [self._add_authority(path) for path in entries]
        return [
            {**entry, "name": self._add_authority(entry["name"])} for entry in entries
        ]

    async def _info(self, path: str, **kwargs):
        info = await super()._info(path, **kwargs)
        return {**info, "name": self._add_authority(info["name"])}

    def unstrip_protocol(self, name: str) -> str:
        path = self._add_authority(self._to_operator_path(name))
        return super().unstrip_protocol(path)

    @classmethod
    def _get_kwargs_from_urls(cls, path: str) -> dict[str, Any]:
        if "://" not in path:
            return {}

        parsed = urlsplit(path)
        if parsed.scheme != cls.protocol:
            return {}

        if not parsed.netloc or cls._authority_option is None:
            return {}
        return {cls._authority_option: parsed.netloc}


class OpendalS3FileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+s3"
    _authority_option = _AUTHORITY_OPTION_BY_SERVICE["s3"]


class OpendalGCSFileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+gcs"
    _authority_option = _AUTHORITY_OPTION_BY_SERVICE["gcs"]


class OpendalAzBlobFileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+azblob"
    _authority_option = _AUTHORITY_OPTION_BY_SERVICE["azblob"]


_BUILTIN_FILESYSTEMS: dict[str, type[_OpendalServiceFileSystem]] = {
    "s3": OpendalS3FileSystem,
    "gcs": OpendalGCSFileSystem,
    "azblob": OpendalAzBlobFileSystem,
}
_DYNAMIC_FILESYSTEMS: dict[str, type[_OpendalServiceFileSystem]] = {}


def register_opendal_service(service: str) -> str:
    """Register one OpenDAL service as an fsspec protocol.

    Parameters
    ----------
    service : str
        OpenDAL service name, such as ``"memory"``, ``"s3"``, or ``"oss"``.

    Returns
    -------
    str
        The registered protocol in the form ``"opendal+<service>"``.

    Notes
    -----
    Registration applies to the current Python process. Repeating a
    registration for the same service reuses the generated filesystem class.
    """
    from fsspec.registry import register_implementation

    protocol = f"opendal+{service}"
    cls = _BUILTIN_FILESYSTEMS.get(service)
    if cls is None:
        cls = _DYNAMIC_FILESYSTEMS.get(service)
    if cls is None:
        safe = "".join([c if c.isalnum() else "_" for c in service])
        name = f"Opendal_{safe}_FileSystem"
        cls = type(
            name,
            (_OpendalServiceFileSystem,),
            {
                "protocol": protocol,
                "_authority_option": _AUTHORITY_OPTION_BY_SERVICE.get(service),
            },
        )
        _DYNAMIC_FILESYSTEMS[service] = cls

    register_implementation(protocol, cls)
    return protocol


def register_opendal_protocols(services: list[str] | None = None) -> list[str]:
    """Register a collection of OpenDAL services with fsspec.

    Parameters
    ----------
    services : list of str, optional
        OpenDAL service names to register. If omitted, registers the built-in
        S3, Google Cloud Storage, and Azure Blob adapters.

    Returns
    -------
    list of str
        Registered protocol names in sorted order.
    """
    if services is None:
        services = list(_BUILTIN_FILESYSTEMS)

    return sorted({register_opendal_service(service) for service in services})


_S3FS_OPTION_ALIASES = {
    "key": "access_key_id",
    "secret": "secret_access_key",
    "token": "session_token",
    "anon": "skip_signature",
    "endpoint_url": "endpoint",
    "requester_pays": "enable_request_payer",
}
_S3FS_CLIENT_OPTION_ALIASES = {
    "aws_access_key_id": "access_key_id",
    "aws_secret_access_key": "secret_access_key",
    "aws_session_token": "session_token",
    "endpoint_url": "endpoint",
    "region_name": "region",
}
_S3FS_BOOLEAN_OPTIONS = {"anon", "requester_pays"}


def _translate_s3fs_options(options: dict[str, Any]) -> dict[str, Any]:
    translated = options.copy()
    for s3fs_name, opendal_name in _S3FS_OPTION_ALIASES.items():
        if (value := translated.pop(s3fs_name, None)) is not None:
            if s3fs_name in _S3FS_BOOLEAN_OPTIONS and isinstance(value, bool):
                value = str(value).lower()
            translated.setdefault(opendal_name, value)

    client_options = translated.pop("client_kwargs", None)
    if client_options is None:
        client_options = {}
    if not isinstance(client_options, Mapping):
        raise TypeError("S3 option 'client_kwargs' must be a mapping")
    client_options = dict(client_options)
    for s3fs_name, opendal_name in _S3FS_CLIENT_OPTION_ALIASES.items():
        if (value := client_options.pop(s3fs_name, None)) is not None:
            translated.setdefault(opendal_name, value)
    if client_options:
        unsupported = ", ".join(sorted(client_options))
        raise TypeError(f"Unsupported S3 client_kwargs: {unsupported}")

    return translated


class S3FileSystem(OpendalS3FileSystem):
    """Route standard ``s3://`` URLs through OpenDAL."""

    protocol = "s3"

    def __init__(
        self,
        *args: Any,
        default_block_size: int | None = None,
        _bucket_from_url: str | None = None,
        **kwargs: Any,
    ) -> None:
        options = _translate_s3fs_options(kwargs)
        if _bucket_from_url is not None:
            options["bucket"] = _bucket_from_url
        self._bucket = options.get("bucket", "")
        super().__init__(*args, **options)
        if default_block_size is not None:
            self.blocksize = default_block_size

    @property
    def _authority(self) -> str:
        return self._bucket

    def _to_operator_path(self, path: str) -> str:
        path = OpendalFileSystem._to_operator_path(self, path)
        if not path or path == self._authority:
            return ""
        prefix = f"{self._authority}/"
        if self._authority and path.startswith(prefix):
            return path[len(prefix) :]
        raise ValueError(
            f"S3 path {path!r} does not belong to bucket {self._authority!r}"
        )

    def unstrip_protocol(self, name: str) -> str:
        return OpendalFileSystem.unstrip_protocol(self, name)

    @classmethod
    def _get_kwargs_from_urls(cls, path: str) -> dict[str, Any]:
        options = super()._get_kwargs_from_urls(path)
        if bucket := options.pop("bucket", None):
            options["_bucket_from_url"] = bucket
        return options
