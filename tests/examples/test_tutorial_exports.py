from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from docs import export_tutorials


def make_notebooks(root, numbers=range(20)):
    for number in numbers:
        root.joinpath(f"{number:02d}_tutorial.py").touch()


def test_tutorial_notebooks_requires_the_complete_sequence(tmp_path, monkeypatch):
    make_notebooks(tmp_path, range(19))
    monkeypatch.setattr(export_tutorials, "NOTEBOOKS_ROOT", tmp_path)

    with pytest.raises(RuntimeError, match="complete tutorial sequence 00-19"):
        export_tutorials.tutorial_notebooks()


def test_export_tutorials_replaces_stale_pages(tmp_path, monkeypatch):
    notebooks_root = tmp_path / "notebooks"
    output_root = tmp_path / "generated"
    notebooks_root.mkdir()
    output_root.mkdir()
    make_notebooks(notebooks_root)
    stale_page = output_root / "retired.md"
    stale_page.write_text("stale")
    commands = []

    def fake_run(command, **options):
        commands.append((command, options))
        output = Path(command[command.index("--output") + 1])
        output.write_text(f"# {Path(command[-3]).stem}\n")
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(export_tutorials, "NOTEBOOKS_ROOT", notebooks_root)
    monkeypatch.setattr(export_tutorials.subprocess, "run", fake_run)

    pages = export_tutorials.export_tutorials(output_root)

    assert len(pages) == len(commands) == 20
    assert not stale_page.exists()
    assert all(page.exists() for page in pages)
    assert all("pymdown" in command for command, _ in commands)
    assert all(
        options["cwd"] == export_tutorials.REPOSITORY_ROOT for _, options in commands
    )
