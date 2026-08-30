import pytest

from opendalfs import S3FileSystem


@pytest.mark.parametrize(
    ("options", "message"),
    [
        (
            {
                "access_key_id": "opendal",
                "key": "s3fs",
            },
            "Conflicting S3 options",
        ),
        ({"version_aware": True}, "Unsupported s3fs options"),
    ],
)
def test_standard_s3_rejects_incompatible_options(options, message):
    with pytest.raises(TypeError, match=message):
        S3FileSystem(bucket="bucket", **options)
