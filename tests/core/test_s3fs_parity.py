import pytest


def _bucket_path(path: str) -> str:
    return f"test-bucket/{path}"


def _call_mkdir(fs, path: str, create_parents: bool):
    try:
        fs.mkdir(path, create_parents=create_parents)
    except Exception as exc:  # pragma: no cover - parity helper
        return exc
    return None


def _append_tell(fs, path: str):
    fs.pipe_file(path, b"hello")
    with fs.open(path, "ab") as f:
        start = f.tell()
        f.write(b"world")
        end = f.tell()
    return start, end, fs.cat_file(path)


def _seek_read(fs, path: str):
    fs.pipe_file(path, b"0123456789")
    with fs.open(path, "rb") as f:
        first = f.read(3)
        pos1 = f.tell()
        f.seek(2, 1)
        pos2 = f.tell()
        f.seek(-3, 2)
        tail = f.read()
    return first, pos1, pos2, tail


@pytest.mark.usefixtures("minio_server")
def test_mkdir_create_parents_matches_s3fs(s3_fs, s3fs_fs):
    path = "parity/mkdir/child"
    exc_s3fs = _call_mkdir(s3fs_fs, _bucket_path(path), create_parents=False)
    exc_opendal = _call_mkdir(s3_fs, path, create_parents=False)

    assert (exc_s3fs is None) == (exc_opendal is None)
    if exc_s3fs is not None and exc_opendal is not None:
        assert exc_opendal.__class__.__name__ == exc_s3fs.__class__.__name__


@pytest.mark.usefixtures("minio_server")
def test_info_trailing_slash_matches_s3fs(s3_fs, s3fs_fs):
    path = "parity/info-dir"
    s3_fs.mkdir(path)
    s3fs_fs.mkdir(_bucket_path(path))

    info_opendal = s3_fs.info(path + "/")
    info_s3fs = s3fs_fs.info(_bucket_path(path) + "/")

    assert info_opendal["type"] == info_s3fs["type"]
    assert info_opendal["name"].endswith("/") == info_s3fs["name"].endswith("/")


@pytest.mark.usefixtures("minio_server")
def test_open_append_tell_matches_s3fs(s3_fs, s3fs_fs):
    opendal = _append_tell(s3_fs, "parity/append-opendal.txt")
    s3fs = _append_tell(s3fs_fs, _bucket_path("parity/append-s3fs.txt"))

    assert opendal == s3fs


@pytest.mark.usefixtures("minio_server")
def test_seek_behavior_matches_s3fs(s3_fs, s3fs_fs):
    opendal = _seek_read(s3_fs, "parity/seek-opendal.txt")
    s3fs = _seek_read(s3fs_fs, _bucket_path("parity/seek-s3fs.txt"))

    assert opendal == s3fs
