"""Small filesystem helpers shared by pipeline and collection writers."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

PathLike = str | Path


@contextmanager
def atomic_output_path(path: PathLike) -> Iterator[Path]:
    """Yield a temporary sibling path and replace ``path`` only on success.

    Keeping the temporary file in the destination directory makes the final
    ``os.replace`` operation atomic on the same filesystem. If serialization
    fails, the previous destination is left untouched and the temporary file is
    removed.
    """
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
    )
    os.close(fd)
    temporary = Path(temporary_name)

    try:
        yield temporary
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
