import pytest

from opendalfs.compat import translate_s3fs_options


def test_translate_common_s3fs_options():
    translated = translate_s3fs_options({
        "key": "access",
        "secret": "secret",
        "token": "session",
        "anon": False,
        "endpoint_url": "http://localhost:9000",
        "requester_pays": True,
        "client_kwargs": {"region_name": "us-east-1"},
    })

    assert translated == {
        "access_key_id": "access",
        "secret_access_key": "secret",
        "session_token": "session",
        "skip_signature": False,
        "endpoint": "http://localhost:9000",
        "enable_request_payer": True,
        "region": "us-east-1",
    }


def test_translate_rejects_conflicting_s3_options():
    with pytest.raises(TypeError, match="different values"):
        translate_s3fs_options({
            "access_key_id": "opendal",
            "key": "s3fs",
        })


def test_translate_rejects_unsupported_s3fs_options():
    with pytest.raises(TypeError, match="version_aware"):
        translate_s3fs_options({"version_aware": True})


def test_translate_rejects_unsupported_client_kwargs():
    with pytest.raises(TypeError, match="verify"):
        translate_s3fs_options({"client_kwargs": {"verify": False}})


def test_translate_uses_http_for_host_only_endpoint_when_ssl_is_disabled():
    assert (
        translate_s3fs_options({"endpoint_url": "localhost:9000", "use_ssl": False})[
            "endpoint"
        ]
        == "http://localhost:9000"
    )
