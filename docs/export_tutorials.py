from __future__ import annotations

import subprocess
import sys
from pathlib import Path

DOCS_ROOT = Path(__file__).resolve().parent
REPOSITORY_ROOT = DOCS_ROOT.parent
NOTEBOOKS_ROOT = REPOSITORY_ROOT / "examples" / "marimo"
OUTPUT_ROOT = DOCS_ROOT / "user-guide" / "tutorials" / "generated"


def tutorial_notebooks() -> tuple[Path, ...]:
    notebooks = tuple(sorted(NOTEBOOKS_ROOT.glob("[0-9][0-9]_*.py")))
    numbers = [int(notebook.name[:2]) for notebook in notebooks]
    if numbers != list(range(20)):
        raise RuntimeError(
            f"Expected the complete tutorial sequence 00-19, found {numbers!r}"
        )
    return notebooks


def export_tutorials(output_root: Path = OUTPUT_ROOT) -> tuple[Path, ...]:
    notebooks = tutorial_notebooks()
    output_root.mkdir(parents=True, exist_ok=True)
    expected_names = {notebook.with_suffix(".md").name for notebook in notebooks}

    for stale_page in output_root.glob("*.md"):
        if stale_page.name not in expected_names:
            stale_page.unlink()

    exported_pages = []
    for notebook in notebooks:
        output = output_root / notebook.with_suffix(".md").name
        command = [
            sys.executable,
            "-m",
            "marimo",
            "export",
            "md",
            "--flavor",
            "pymdown",
            "--force",
            str(notebook),
            "--output",
            str(output),
        ]
        completed = subprocess.run(
            command,
            cwd=REPOSITORY_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode:
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(f"Failed to export {notebook.name}: {detail}")
        exported_pages.append(output)

    return tuple(exported_pages)


if __name__ == "__main__":
    pages = export_tutorials()
    print(
        f"Exported {len(pages)} tutorials to {OUTPUT_ROOT.relative_to(REPOSITORY_ROOT)}"
    )
