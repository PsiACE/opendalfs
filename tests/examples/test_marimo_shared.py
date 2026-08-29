import pytest

from examples.marimo._shared import MinioConfig, benchmark, dataset_spec


def test_minio_config_uses_project_defaults(monkeypatch):
    for name in (
        "OPENDAL_S3_ENDPOINT",
        "OPENDAL_S3_REGION",
        "OPENDAL_S3_BUCKET",
        "OPENDAL_S3_ACCESS_KEY_ID",
        "OPENDAL_S3_SECRET_ACCESS_KEY",
    ):
        monkeypatch.delenv(name, raising=False)

    config = MinioConfig.from_env()

    assert config.endpoint == "http://127.0.0.1:9000"
    assert config.region == "us-east-1"
    assert config.bucket == "opendalfs-examples"
    assert config.url("data/item") == "s3://opendalfs-examples/data/item"


def test_dataset_manifest_exposes_source_and_checksum():
    iris = dataset_spec("iris")

    assert iris.license == "CC BY 4.0"
    assert iris.source.startswith("https://archive.ics.uci.edu/")
    assert len(iris.sha256 or "") == 64


def test_dataset_manifest_rejects_unknown_name():
    with pytest.raises(KeyError, match="Unknown example dataset"):
        dataset_spec("missing")


def test_benchmark_warms_up_and_collects_every_sample():
    calls = {"first": 0, "second": 0}

    def run(label):
        calls[label] += 1
        return label

    results = benchmark(
        {
            "first": lambda: run("first"),
            "second": lambda: run("second"),
        },
        repeat=3,
    )

    assert calls == {"first": 4, "second": 4}
    assert {result.label for result in results} == {"first", "second"}
    assert all(len(result.samples_s) == 3 for result in results)


def test_benchmark_requires_a_sample():
    with pytest.raises(ValueError, match="at least one"):
        benchmark({"case": lambda: None}, repeat=0)
