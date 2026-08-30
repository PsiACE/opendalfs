from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _set_compatible_option(
    options: dict[str, Any],
    target: str,
    value: Any,
    *,
    source: str,
) -> None:
    if value is None:
        return

    current = options.get(target)
    if current is not None and current != value:
        raise TypeError(
            f"Conflicting S3 options: {source!r} and {target!r} have different values"
        )
    options[target] = value


def translate_s3fs_options(options: dict[str, Any]) -> dict[str, Any]:
    """Translate common s3fs constructor options to OpenDAL S3 options."""
    translated = options.copy()

    client_kwargs = translated.pop("client_kwargs", None) or {}
    if not isinstance(client_kwargs, Mapping):
        raise TypeError("S3 option 'client_kwargs' must be a mapping")
    client_kwargs = dict(client_kwargs)

    client_option_names = {
        "aws_access_key_id": "access_key_id",
        "aws_secret_access_key": "secret_access_key",
        "aws_session_token": "session_token",
        "endpoint_url": "endpoint",
        "region_name": "region",
    }
    for source, target in client_option_names.items():
        _set_compatible_option(
            translated,
            target,
            client_kwargs.pop(source, None),
            source=f"client_kwargs.{source}",
        )

    if client_kwargs:
        unsupported = ", ".join(sorted(client_kwargs))
        raise TypeError(f"Unsupported S3 client_kwargs: {unsupported}")

    alias_names = {
        "key": "access_key_id",
        "username": "access_key_id",
        "secret": "secret_access_key",
        "password": "secret_access_key",
        "token": "session_token",
        "endpoint_url": "endpoint",
        "requester_pays": "enable_request_payer",
    }
    for source, target in alias_names.items():
        if source in translated:
            _set_compatible_option(
                translated,
                target,
                translated.pop(source),
                source=source,
            )

    if "anon" in translated:
        _set_compatible_option(
            translated,
            "skip_signature",
            translated.pop("anon"),
            source="anon",
        )

    unsupported_names = {
        "cache_regions",
        "config_kwargs",
        "default_cache_type",
        "default_fill_cache",
        "fixed_upload_size",
        "local_expiry_check",
        "max_concurrency",
        "s3_additional_kwargs",
        "session",
        "version_aware",
    }
    unsupported = sorted(unsupported_names.intersection(translated))
    if unsupported:
        joined = ", ".join(unsupported)
        raise TypeError(f"Unsupported s3fs options for OpenDAL S3: {joined}")

    use_ssl = translated.pop("use_ssl", None)
    endpoint = translated.get("endpoint")
    if use_ssl is False and isinstance(endpoint, str) and "://" not in endpoint:
        translated["endpoint"] = f"http://{endpoint}"

    return translated
