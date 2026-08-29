# /// script
# requires-python = ">=3.12,<3.15"
# dependencies = [
#     "boto3>=1.37.3",
#     "fsspec>=2025.5.1",
#     "lightning==2.6.5",
#     "marimo>=0.17.0",
#     "opendalfs",
#     "pandas>=2.2",
# ]
# [tool.uv.sources]
# opendalfs = { path = "../..", editable = true }
# ///

import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import io
    import zipfile

    import fsspec
    import pandas as pd
    import torch
    from _shared import (
        MinioConfig,
        clear_prefix,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        opendal_filesystem,
    )
    from lightning.pytorch import LightningModule, Trainer, seed_everything
    from lightning.pytorch.callbacks import ModelCheckpoint
    from torch.utils.data import DataLoader, TensorDataset

    import marimo as mo

    return (
        DataLoader,
        LightningModule,
        MinioConfig,
        ModelCheckpoint,
        TensorDataset,
        Trainer,
        clear_prefix,
        dataset_spec,
        ensure_minio,
        fetch_to_minio,
        fsspec,
        io,
        mo,
        opendal_filesystem,
        pd,
        seed_everything,
        torch,
        zipfile,
    )


@app.cell
def _(mo):
    mo.md("""
    # Recover a Lightning training run from MinIO

    [Lightning's checkpoint documentation](https://lightning.ai/docs/pytorch/stable/common/checkpointing_basic.html)
    uses fsspec for remote paths. This notebook follows the public
    `ModelCheckpoint` and `load_from_checkpoint` workflow with an `opendal+s3://`
    directory, then proves that a fresh model has the same parameters and
    predictions.

        The training input is the UCI Wine classification dataset, licensed CC BY 4.0.
        Object stores do not provide POSIX symlinks, so this solution deliberately does
        not use `save_last="link"`.
    """)
    return


@app.cell
def _(
    MinioConfig,
    clear_prefix,
    ensure_minio,
    fsspec,
    opendal_filesystem,
    seed_everything,
    torch,
):
    lightning_minio = MinioConfig.from_env()
    ensure_minio(lightning_minio)
    lightning_fs = opendal_filesystem(lightning_minio)
    lightning_prefix = "08_lightning"
    clear_prefix(lightning_fs, lightning_prefix)
    fsspec.config.conf["opendal+s3"] = dict(lightning_fs.storage_options)
    seed_everything(7, workers=True)
    torch.set_float32_matmul_precision("high")
    return lightning_fs, lightning_minio, lightning_prefix


@app.cell
def _(
    dataset_spec,
    fetch_to_minio,
    io,
    lightning_fs,
    lightning_prefix,
    pd,
    torch,
    zipfile,
):
    wine_source = dataset_spec("wine")
    wine_key = f"{lightning_prefix}/raw/wine.zip"
    wine_download = fetch_to_minio(lightning_fs, wine_source, wine_key)

    with zipfile.ZipFile(io.BytesIO(lightning_fs.cat_file(wine_key))) as archive:
        wine_csv = archive.read("wine.data")

    wine_frame = pd.read_csv(io.BytesIO(wine_csv), header=None)
    wine_features = torch.tensor(wine_frame.iloc[:, 1:].to_numpy(), dtype=torch.float32)
    wine_targets = torch.tensor(wine_frame.iloc[:, 0].to_numpy() - 1, dtype=torch.long)
    wine_mean = wine_features.mean(dim=0)
    wine_std = wine_features.std(dim=0).clamp_min(1e-6)
    wine_features = (wine_features - wine_mean) / wine_std

    assert wine_features.shape == (178, 13)
    assert set(wine_targets.tolist()) == {0, 1, 2}
    return wine_download, wine_features, wine_source, wine_targets


@app.cell
def _(LightningModule, torch):
    class WineClassifier(LightningModule):
        def __init__(self, input_features: int = 13, classes: int = 3):
            super().__init__()
            self.save_hyperparameters()
            self.network = torch.nn.Sequential(
                torch.nn.Linear(input_features, 16),
                torch.nn.ReLU(),
                torch.nn.Linear(16, classes),
            )

        def training_step(self, batch, batch_idx):
            features, targets = batch
            loss = torch.nn.functional.cross_entropy(self.network(features), targets)
            self.log("train_loss", loss, on_epoch=True)
            return loss

        def configure_optimizers(self):
            return torch.optim.SGD(self.parameters(), lr=0.05)

        def forward(self, features):
            return self.network(features)

    return (WineClassifier,)


@app.cell
def _(
    DataLoader,
    ModelCheckpoint,
    TensorDataset,
    Trainer,
    WineClassifier,
    lightning_minio,
    lightning_prefix,
    wine_features,
    wine_targets,
):
    checkpoint_url = lightning_minio.url(
        f"{lightning_prefix}/checkpoints", protocol="opendal+s3"
    )
    checkpoint_callback = ModelCheckpoint(
        dirpath=checkpoint_url,
        filename="wine-{epoch:02d}",
        save_top_k=1,
        monitor="train_loss",
        mode="min",
    )
    lightning_loader = DataLoader(
        TensorDataset(wine_features, wine_targets),
        batch_size=32,
        shuffle=False,
        num_workers=0,
    )
    trained_wine_model = WineClassifier()
    lightning_trainer = Trainer(
        max_epochs=1,
        accelerator="cpu",
        devices=1,
        logger=False,
        callbacks=[checkpoint_callback],
        deterministic=True,
        enable_progress_bar=False,
        enable_model_summary=False,
    )
    lightning_trainer.fit(trained_wine_model, train_dataloaders=lightning_loader)
    best_checkpoint_url = checkpoint_callback.best_model_path

    assert best_checkpoint_url.startswith("opendal+s3://")
    return best_checkpoint_url, trained_wine_model


@app.cell
def _(
    WineClassifier,
    best_checkpoint_url,
    lightning_fs,
    lightning_minio,
    lightning_prefix,
    torch,
    trained_wine_model,
    wine_features,
):
    restored_wine_model = WineClassifier.load_from_checkpoint(best_checkpoint_url)
    trained_wine_model.eval()
    restored_wine_model.eval()

    for parameter_name, parameter_value in trained_wine_model.state_dict().items():
        torch.testing.assert_close(
            restored_wine_model.state_dict()[parameter_name], parameter_value
        )

    with torch.no_grad():
        original_predictions = trained_wine_model(wine_features[:16])
        restored_predictions = restored_wine_model(wine_features[:16])
    torch.testing.assert_close(original_predictions, restored_predictions)

    checkpoint_key = best_checkpoint_url.removeprefix(
        f"opendal+s3://{lightning_minio.bucket}/"
    )
    checkpoint_objects = lightning_fs.find(f"{lightning_prefix}/checkpoints")
    checkpoint_object = next(
        path for path in checkpoint_objects if path.endswith(checkpoint_key)
    )
    checkpoint_size = lightning_fs.size(checkpoint_object)

    assert checkpoint_size > 0
    return (checkpoint_size,)


@app.cell
def _(best_checkpoint_url, checkpoint_size, mo, wine_download, wine_source):
    mo.md(
        f"""
    ## Verified recovery

    - Data source: [{wine_source.source}]({wine_source.source})
    - License: **{wine_source.license}**
    - Raw archive: {wine_download.size:,} bytes
    - Checkpoint: `{best_checkpoint_url}`
    - Checkpoint size: {checkpoint_size:,} bytes

    A separately constructed `WineClassifier` restored every state-dict tensor and
    produced identical logits for a fixed batch. That is the useful recovery
    contract; a tiny local checkpoint is not a meaningful storage throughput
    benchmark.
    """
    )
    return


if __name__ == "__main__":
    app.run()
