"""Cross-platform content fingerprints for release artifacts."""

from __future__ import annotations

import hashlib
from pathlib import Path

TEXT_SUFFIXES = frozenset({".csv", ".json", ".jsonl", ".md", ".txt", ".yaml", ".yml"})


def canonical_file_bytes(path: Path) -> bytes:
    """Return bytes that are stable across Windows and Unix Git checkouts."""
    content = Path(path).read_bytes()
    if Path(path).suffix.lower() in TEXT_SUFFIXES:
        content = content.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return content


def file_fingerprint(path: Path) -> dict[str, str | int]:
    """Return a SHA-256 and size for the canonical content of *path*."""
    content = canonical_file_bytes(path)
    return {
        "sha256": hashlib.sha256(content).hexdigest(),
        "canonical_bytes": len(content),
    }
