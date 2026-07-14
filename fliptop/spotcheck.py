"""Randomly display battles from the published ``ft_battles.json`` output.

The command is intentionally read-only and uses only the standard library. It
samples JSON-lines records without loading the whole output into memory.

    fliptop-spotcheck
    fliptop-spotcheck 10
    fliptop-spotcheck 10 --path path/to/alternate.json
"""

from __future__ import annotations

import argparse
import json
import math
import random
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import PROCESSED_DATA_DIR, PROJECT_ROOT

DEFAULT_PATH = PROCESSED_DATA_DIR / "ft_battles.json"
DEFAULT_COUNT = 5


class SpotcheckError(ValueError):
    """Raised when a battle sample cannot be read from an output file."""


def sample_battles(
    path: Path,
    count: int,
    *,
    rng: random.Random | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Return ``count`` uniformly sampled JSON-lines records and the row count."""
    if count <= 0:
        raise SpotcheckError("the number of battles must be a positive integer")

    rng = rng or random.Random()
    sample: list[dict[str, Any]] = []
    total = 0

    try:
        with path.open(encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise SpotcheckError(
                        f"{path}: line {line_number} is not valid JSON ({exc.msg})"
                    ) from exc
                if not isinstance(row, dict):
                    raise SpotcheckError(
                        f"{path}: line {line_number} must contain a JSON object"
                    )

                total += 1
                if len(sample) < count:
                    sample.append(row)
                    continue

                replacement = rng.randrange(total)
                if replacement < count:
                    sample[replacement] = row
    except OSError as exc:
        detail = exc.strerror or str(exc)
        raise SpotcheckError(f"could not read {path}: {detail}") from exc

    if total < count:
        raise SpotcheckError(
            f"requested {count} battles, but {path} contains only {total}"
        )

    rng.shuffle(sample)
    return sample, total


def _text(value: Any) -> str:
    if value is None:
        return "unknown"
    text = str(value).strip()
    return text or "unknown"


def _is_na(value: Any) -> bool:
    return value is None or str(value).strip().casefold() in {"", "na", "nan"}


def _date(value: Any) -> str:
    """Format the epoch-millisecond dates used by ft_battles as ISO dates."""
    if value is None or isinstance(value, bool):
        return "unknown"

    if isinstance(value, (int, float)):
        if not math.isfinite(value):
            return "unknown"
        try:
            return datetime.fromtimestamp(value / 1000, tz=UTC).date().isoformat()
        except (OSError, OverflowError, ValueError):
            return str(value)

    raw = str(value).strip()
    if not raw:
        return "unknown"
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return raw


def _result(row: dict[str, Any]) -> str:
    battle_type = _text(row.get("battle_type"))
    winner = row.get("winner")

    if battle_type.casefold() == "promo":
        return "promo (no judging)"
    if str(winner).strip().casefold() == "na":
        return "draw (judged)" if battle_type.casefold() == "judged" else "no winner"
    if _is_na(winner):
        return f"unknown ({battle_type})"

    votes_winner = row.get("votes_winner")
    votes_loser = row.get("votes_loser")
    if _is_na(votes_winner) or _is_na(votes_loser):
        return f"{_text(winner)} wins ({battle_type}; score unknown)"
    return f"{_text(winner)} wins {_text(votes_winner)}-{_text(votes_loser)} ({battle_type})"


def format_battle(row: dict[str, Any], *, number: int, count: int) -> str:
    """Format one published battle as a compact, human-readable block."""
    lines = [
        f"Battle {number} of {count}",
        f"  Matchup: {_text(row.get('matchup'))}",
        f"  Event: {_text(row.get('event_name'))}",
        f"  Event date: {_date(row.get('event_date'))}",
        f"  Location: {_text(row.get('event_location'))}",
        f"  Uploaded: {_date(row.get('upload_date'))}",
        f"  Result: {_result(row)}",
    ]

    raw_urls = row.get("url")
    urls = raw_urls if isinstance(raw_urls, list) else [raw_urls]
    urls = [_text(url) for url in urls] or ["unknown"]
    lines.append(f"  URL: {urls[0]}")
    lines.extend(f"       {url}" for url in urls[1:])
    return "\n".join(lines)


def _display_path(path: Path) -> Path:
    try:
        return path.resolve().relative_to(PROJECT_ROOT)
    except ValueError:
        return path


def run(*, count: int = DEFAULT_COUNT, path: Path = DEFAULT_PATH) -> None:
    battles, total = sample_battles(path, count)
    print(f"Randomly selected {count} of {total} battles from {_display_path(path)}.\n")
    print(
        "\n\n".join(
            format_battle(row, number=number, count=count)
            for number, row in enumerate(battles, start=1)
        )
    )


def _positive_int(value: str) -> int:
    try:
        count = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if count <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return count


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Display a random sample from the published FlipTop battles output."
    )
    parser.add_argument(
        "count",
        nargs="?",
        type=_positive_int,
        default=DEFAULT_COUNT,
        help=f"number of battles to display (default: {DEFAULT_COUNT})",
    )
    parser.add_argument(
        "--path",
        type=Path,
        default=DEFAULT_PATH,
        help="alternate newline-delimited ft_battles JSON file",
    )
    args = parser.parse_args(argv)

    try:
        run(count=args.count, path=args.path)
    except SpotcheckError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
