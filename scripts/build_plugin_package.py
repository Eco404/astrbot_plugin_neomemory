#!/usr/bin/env python3
"""Build an AstrBot plugin ZIP in the repository's dist directory."""

from __future__ import annotations

import re
import zipfile
from pathlib import Path

RUNTIME_FILES = (
    "main.py",
    "metadata.yaml",
    "_conf_schema.json",
    "requirements.txt",
    "README.md",
)
RUNTIME_DIRECTORIES = ("adapters", "core", "storage", "views")
IGNORED_NAMES = {"__pycache__", ".DS_Store", "Thumbs.db"}
IGNORED_SUFFIXES = (".pyc", ".pyo", ".db", ".db-wal", ".db-shm", ".log", ".tmp")


def metadata_value(metadata: str, key: str) -> str:
    """Read a top-level name or version scalar without requiring PyYAML."""
    match = re.search(rf"^{key}:\s*['\"]?([^'\"\s#]+)", metadata, re.MULTILINE)
    if match is None:
        raise ValueError(f"metadata.yaml is missing {key}")
    return match.group(1)


def build_package(root: Path) -> Path:
    """Package runtime files from the working tree, including local changes."""
    metadata = (root / "metadata.yaml").read_text(encoding="utf-8")
    name = metadata_value(metadata, "name")
    version = metadata_value(metadata, "version").removeprefix("v")
    files = [root / filename for filename in RUNTIME_FILES]
    for directory in RUNTIME_DIRECTORIES:
        files.extend(
            path
            for path in (root / directory).rglob("*")
            if path.is_file()
            and not any(part in IGNORED_NAMES for part in path.relative_to(root).parts)
            and not path.name.endswith(IGNORED_SUFFIXES)
        )

    output = root / "dist" / f"{name}-v{version}.zip"
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(files):
            archive.write(path, f"{name}/{path.relative_to(root).as_posix()}")

    print(f"Built: {output}")
    print(f"Files: {len(files)}; size: {output.stat().st_size:,} bytes")
    return output


if __name__ == "__main__":
    build_package(Path(__file__).resolve().parents[1])
