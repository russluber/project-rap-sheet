"""Tests for the read-only fliptop-spotcheck CLI."""

from __future__ import annotations

import json
import random
from pathlib import Path

import pytest

from fliptop import PROCESSED_DATA_DIR, spotcheck


def _write_rows(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(f"{json.dumps(row)}\n" for row in rows), encoding="utf-8")


def test_sample_battles_is_without_replacement(tmp_path):
    path = tmp_path / "battles.json"
    _write_rows(path, [{"id": str(i)} for i in range(20)])

    sample, total = spotcheck.sample_battles(path, 7, rng=random.Random(42))

    assert total == 20
    assert len(sample) == 7
    assert len({row["id"] for row in sample}) == 7


def test_sample_battles_rejects_a_request_larger_than_the_file(tmp_path):
    path = tmp_path / "battles.json"
    _write_rows(path, [{"id": "one"}, {"id": "two"}])

    with pytest.raises(spotcheck.SpotcheckError, match="requested 3 battles.*contains only 2"):
        spotcheck.sample_battles(path, 3)


def test_sample_battles_reports_malformed_json_line(tmp_path):
    path = tmp_path / "battles.json"
    path.write_text('{"id": "valid"}\nnot json\n', encoding="utf-8")

    with pytest.raises(spotcheck.SpotcheckError, match="line 2 is not valid JSON"):
        spotcheck.sample_battles(path, 1)


def test_format_battle_displays_requested_audit_fields_and_urls():
    row = {
        "matchup": "Loonie vs Abra",
        "event_name": "Ahon 3",
        "event_date": 1355529600000,
        "event_location": "Makati Cinema Square",
        "upload_date": 1356912000000,
        "battle_type": "judged",
        "winner": "Loonie",
        "votes_winner": "5",
        "votes_loser": "0",
        "url": ["https://youtu.be/part1", "https://youtu.be/part2"],
    }

    output = spotcheck.format_battle(row, number=1, count=5)

    assert "Battle 1 of 5" in output
    assert "Matchup: Loonie vs Abra" in output
    assert "Event: Ahon 3" in output
    assert "Event date: 2012-12-15" in output
    assert "Location: Makati Cinema Square" in output
    assert "Uploaded: 2012-12-31" in output
    assert "Result: Loonie wins 5-0 (judged)" in output
    assert "https://youtu.be/part1" in output
    assert "https://youtu.be/part2" in output
    assert "Title:" not in output
    assert "Duration:" not in output


@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        ({"battle_type": "promo", "winner": "NA"}, "promo (no judging)"),
        ({"battle_type": "judged", "winner": "NA"}, "draw (judged)"),
        (
            {"battle_type": "judged", "winner": "Abra", "votes_winner": "NA"},
            "Abra wins (judged; score unknown)",
        ),
    ],
)
def test_result_summary_handles_promo_draw_and_unknown_score(fields, expected):
    assert spotcheck._result(fields) == expected


def test_main_defaults_to_five_and_the_processed_output(monkeypatch):
    called = {}

    def fake_run(*, count, path):
        called.update(count=count, path=path)

    monkeypatch.setattr(spotcheck, "run", fake_run)

    spotcheck.main([])

    assert called == {"count": 5, "path": PROCESSED_DATA_DIR / "ft_battles.json"}


def test_main_accepts_a_count_and_alternate_path(tmp_path, monkeypatch):
    called = {}
    alternate = tmp_path / "candidate.json"

    def fake_run(*, count, path):
        called.update(count=count, path=path)

    monkeypatch.setattr(spotcheck, "run", fake_run)

    spotcheck.main(["12", "--path", str(alternate)])

    assert called == {"count": 12, "path": alternate}
