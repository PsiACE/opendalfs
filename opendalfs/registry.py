from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar
from urllib.parse import urlsplit

from .compat import translate_s3fs_options
from .fs import OpendalFileSystem


@dataclass(frozen=True, slots=True)
class ServiceSpec:
    """URL shape for one OpenDAL service."""

    name: str
    authority_option: str | None = None


# Like Tetos' explicit provider list, this is the single extension point for
# services whose URL authority has service-specific meaning.
SERVICE_SPECS = tuple(
    ServiceSpec(service, authority_option)
    for service, authority_option in (
        ("aliyun-drive", "drive_type"),
        ("azblob", "container"),
        ("b2", "bucket"),
        ("cos", "bucket"),
        ("gcs", "bucket"),
        ("obs", "bucket"),
        ("oss", "bucket"),
        ("s3", "bucket"),
        ("tos", "bucket"),
        ("upyun", "bucket"),
    )
)
_SERVICE_SPEC_BY_NAME = {spec.name: spec for spec in SERVICE_SPECS}


def get_service_spec(service: str) -> ServiceSpec:
    """Return the URL description for an OpenDAL service."""
    if not service:
        raise ValueError("OpenDAL service name cannot be empty")
    return _SERVICE_SPEC_BY_NAME.get(service, ServiceSpec(service))


class _OpendalServiceFileSystem(OpendalFileSystem):
    protocol: ClassVar[str]
    service: ClassVar[str | None] = None
    _authority_option: ClassVar[str | None] = None
    _accepts_default_block_size: ClassVar[bool] = False

    @staticmethod
    def _adapt_storage_options(options: dict[str, Any]) -> dict[str, Any]:
        return options

    @classmethod
    def _class_authority_option(cls) -> str | None:
        if cls._authority_option is not None or cls.service is None:
            return cls._authority_option
        return get_service_spec(cls.service).authority_option

    def __init__(
        self,
        *args: Any,
        service: str | None = None,
        **kwargs: Any,
    ) -> None:
        kwargs.pop("scheme", None)
        default_block_size = None
        if type(self)._accepts_default_block_size:
            default_block_size = kwargs.pop("default_block_size", None)
        declared_service = type(self).service
        if declared_service is not None and service not in (None, declared_service):
            raise ValueError(
                f"Protocol {type(self).protocol!r} uses the {declared_service!r} "
                f"service, not {service!r}"
            )

        resolved_service = (
            service or declared_service or type(self).protocol.removeprefix("opendal+")
        )
        spec = get_service_spec(resolved_service)
        self.service_name = resolved_service
        self._resolved_authority_option = (
            type(self)._authority_option or spec.authority_option
        )
        storage_options = type(self)._adapt_storage_options(kwargs)
        super().__init__(resolved_service, *args, **storage_options)
        if default_block_size is not None:
            self.blocksize = default_block_size

    def __reduce__(self):
        return (
            _rebuild_service_filesystem,
            (
                type(self).protocol,
                self.service_name,
                self.storage_args,
                self.storage_options,
            ),
        )

    @property
    def _authority(self) -> str:
        if self._resolved_authority_option is None:
            return ""
        return self.storage_options.get(self._resolved_authority_option, "")

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, list):
            return super()._strip_protocol(path)

        path = super()._strip_protocol(path)
        if cls._class_authority_option() is None and path:
            return f"/{path.lstrip('/')}"
        return path

    def _to_operator_path(self, path: str) -> str:
        path = super()._to_operator_path(path)
        path = self._remove_service(path)
        # Service adapters expose authority/path, while the OpenDAL operator
        # is already scoped by the corresponding service option.
        authority = self._authority
        if path == authority:
            return ""
        if authority and path.startswith(f"{authority}/"):
            return path[len(authority) + 1 :]
        return path

    def _remove_service(self, path: str) -> str:
        return path

    def _add_authority(self, path: str) -> str:
        authority = self._authority
        return f"{authority}/{path}" if path else authority

    def _add_service(self, path: str) -> str:
        return path

    def _add_scope(self, path: str) -> str:
        return self._add_service(self._add_authority(path))

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

        authority_option = cls._class_authority_option()
        if not parsed.netloc or authority_option is None:
            return {}
        return {authority_option: parsed.netloc}


class OpendalS3FileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+s3"
    service = "s3"


class OpendalGCSFileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+gcs"
    service = "gcs"


class OpendalAzBlobFileSystem(_OpendalServiceFileSystem):
    protocol = "opendal+azblob"
    service = "azblob"


class S3FileSystem(_OpendalServiceFileSystem):
    """OpenDAL S3 filesystem accepting the common s3fs constructor spelling."""

    protocol = "s3"
    service = "s3"
    _accepts_default_block_size = True
    _adapt_storage_options = staticmethod(translate_s3fs_options)


_BUILTIN_FILESYSTEMS: dict[str, type[_OpendalServiceFileSystem]] = {
    "s3": OpendalS3FileSystem,
    "gcs": OpendalGCSFileSystem,
    "azblob": OpendalAzBlobFileSystem,
}
_STANDARD_FILESYSTEMS: dict[str, type[_OpendalServiceFileSystem]] = {
    "s3": S3FileSystem,
}
_DYNAMIC_FILESYSTEMS: dict[str, type[_OpendalServiceFileSystem]] = {}


def _rebuild_service_filesystem(
    protocol: str,
    service: str,
    storage_args: tuple[Any, ...],
    storage_options: dict[str, Any],
):
    if protocol in _STANDARD_FILESYSTEMS:
        cls = _STANDARD_FILESYSTEMS[protocol]
    else:
        register_opendal_service(service, clobber=True)
        cls = _filesystem_class_for_service(service)
    return cls(*storage_args, **storage_options)


def _filesystem_class_for_service(
    service: str,
) -> type[_OpendalServiceFileSystem]:
    get_service_spec(service)
    cls = _BUILTIN_FILESYSTEMS.get(service) or _DYNAMIC_FILESYSTEMS.get(service)
    if cls is not None:
        return cls

    safe_name = "".join(
        character if character.isalnum() else "_" for character in service
    )
    cls = type(
        f"Opendal_{safe_name}_FileSystem",
        (_OpendalServiceFileSystem,),
        {
            "__module__": __name__,
            "protocol": f"opendal+{service}",
            "service": service,
        },
    )
    _DYNAMIC_FILESYSTEMS[service] = cls
    return cls


def _register_filesystem(
    protocol: str,
    cls: type[OpendalFileSystem],
    *,
    clobber: bool,
) -> str:
    from fsspec.registry import get_filesystem_class, register_implementation

    register_implementation(protocol, cls, clobber=clobber)
    if get_filesystem_class(protocol) is not cls:
        raise RuntimeError(f"OpenDAL did not win registration for {protocol!r}")
    return protocol


def register_opendal_service(service: str, *, clobber: bool = True) -> str:
    """Register one OpenDAL service as an fsspec protocol.

    Parameters
    ----------
    service : str
        OpenDAL service name, such as ``"memory"``, ``"s3"``, or ``"oss"``.
    clobber : bool
        Replace an existing implementation for the generated protocol. The
        default makes the explicitly requested OpenDAL service win.

    Returns
    -------
    str
        The registered protocol in the form ``"opendal+<service>"``.

    Notes
    -----
    Registration applies to the current Python process. Repeating a
    registration for the same service reuses the generated filesystem class.
    """
    protocol = f"opendal+{service}"
    cls = _filesystem_class_for_service(service)
    return _register_filesystem(protocol, cls, clobber=clobber)


def register_opendal_standard_protocols(
    protocols: list[str] | None = None,
) -> list[str]:
    """Make OpenDAL win registration for supported standard fsspec protocols.

    Parameters
    ----------
    protocols : list of str, optional
        Standard protocol names to replace. If omitted, replaces every standard
        protocol supported by this version of ``opendalfs``.

    Returns
    -------
    list of str
        Replaced protocol names in sorted order.
    """
    if protocols is None:
        protocols = list(_STANDARD_FILESYSTEMS)

    registered = []
    for protocol in protocols:
        try:
            cls = _STANDARD_FILESYSTEMS[protocol]
        except KeyError as error:
            supported = ", ".join(sorted(_STANDARD_FILESYSTEMS))
            raise ValueError(
                f"Unsupported standard OpenDAL protocol {protocol!r}; "
                f"choose from: {supported}"
            ) from error
        registered.append(_register_filesystem(protocol, cls, clobber=True))
    return sorted(set(registered))


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
