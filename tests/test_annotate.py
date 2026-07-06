"""
Tests for fliptop.annotate helpers that are pure (no interactive input):
resolve_targets (the --redo lookup) and _current_result_str.
"""

from __future__ import annotations

import pandas as pd

from fliptop import annotate
from fliptop import annotations as ann


def _battles():
    return pd.DataFrame(
        {
            "id": ["aaaaaaaaaaa", "bbbbbbbbbbb", ["c1c1c1c1c1c", "c2"]],
            "emcee1": ["Loonie", "Abra", "Shehyee"],
            "emcee2": ["Abra", "Shehyee", "Loonie"],
            "matchup": ["Loonie vs Abra", "Abra vs Shehyee", "Shehyee vs Loonie"],
            "upload_date": pd.to_datetime(["2020-01-01", "2021-01-01", "2022-01-01"]),
        }
    )


def test_resolve_targets_by_matchup_text():
    targets = annotate.resolve_targets(_battles(), "Abra")
    # "Abra" appears in two matchups
    assert set(targets["battle_key"]) == {"aaaaaaaaaaa", "bbbbbbbbbbb"}


def test_resolve_targets_by_exact_matchup():
    targets = annotate.resolve_targets(_battles(), "Shehyee vs Loonie")
    assert targets["battle_key"].tolist() == ["c1c1c1c1c1c"]  # multi-part -> first id


def test_resolve_targets_by_video_id():
    targets = annotate.resolve_targets(_battles(), "bbbbbbbbbbb")
    assert targets["battle_key"].tolist() == ["bbbbbbbbbbb"]


def test_resolve_targets_by_url():
    url = "https://www.youtube.com/watch?v=aaaaaaaaaaa"
    targets = annotate.resolve_targets(_battles(), url)
    assert targets["battle_key"].tolist() == ["aaaaaaaaaaa"]


def test_resolve_targets_no_match_is_empty():
    assert annotate.resolve_targets(_battles(), "Nobody").empty


def test_resolve_targets_returns_rematches_newest_first():
    # Two battles between the same emcees on different dates (a rematch).
    rematch = pd.DataFrame(
        {
            "id": ["older000000", "newer000000"],
            "emcee1": ["Lhipkram", "Lhipkram"],
            "emcee2": ["Jonas", "Jonas"],
            "matchup": ["Lhipkram vs Jonas", "Lhipkram vs Jonas"],
            "event_name": ["Bwelta Balentong 5", "Unibersikulo 9"],
            "upload_date": pd.to_datetime(["2018-11-07", "2021-09-25"]),
        }
    )
    targets = annotate.resolve_targets(rematch, "Lhipkram vs Jonas")
    assert len(targets) == 2
    # newest upload first, so the picker numbers them recent -> old
    assert targets["battle_key"].tolist() == ["newer000000", "older000000"]


def test_current_result_str_reports_stored_and_missing():
    results = pd.DataFrame(
        [ann.make_result_row(id="aaaaaaaaaaa", winner="Loonie", battle_type="judged",
                             votes_winner=5, votes_loser=0, votes_nv=0, votes_ot=0, overtime="no")],
        columns=ann.RESULTS_COLUMNS,
    )
    assert "Loonie wins 5-0" in annotate._current_result_str(results, "aaaaaaaaaaa")
    assert annotate._current_result_str(results, "bbbbbbbbbbb") == "not yet recorded"


def test_summarize_promo_reports_no_judging():
    row = ann.make_result_row(id="aaaaaaaaaaa", winner=ann.NA, battle_type="promo")
    assert annotate._summarize(row) == "promo (no judging)"


def test_summarize_draw_distinguishes_it_from_promo():
    row = ann.make_result_row(id="aaaaaaaaaaa", winner=ann.NA, battle_type="judged")
    assert annotate._summarize(row) == "draw (judged, no winner)"


def test_summarize_judged_without_score_reports_unknown():
    row = ann.make_result_row(id="aaaaaaaaaaa", winner="Loonie", battle_type="judged")
    assert annotate._summarize(row) == "Loonie wins (score unknown)"


def test_prompt_winner_accepts_draw(monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _: "d")
    assert annotate._prompt_winner("Loonie", "Abra") is annotate._DRAW


def test_collect_draw_does_not_prompt_for_score(monkeypatch):
    def fail():
        raise AssertionError("draw must not prompt for a score")

    monkeypatch.setattr(annotate, "_prompt_score", fail)
    fields = annotate._collect_judging(annotate._DRAW)
    assert fields == {"winner": ann.NA, "battle_type": "judged"}


def test_collect_judging_records_winner_loser_score(monkeypatch):
    monkeypatch.setattr(annotate, "_prompt_score", lambda: (5, 0))
    monkeypatch.setattr(annotate, "_prompt_yes_no", lambda *args: "no")

    fields = annotate._collect_judging("Abra")

    assert fields["votes_winner"] == 5
    assert fields["votes_loser"] == 0
    assert fields["winner"] == "Abra"


def test_summarize_reports_winner_loser_score():
    row = ann.make_result_row(
        id="aaaaaaaaaaa",
        winner="Abra",
        battle_type="judged",
        votes_winner=5,
        votes_loser=0,
        votes_nv=0,
        votes_ot=0,
        overtime="no",
    )
    assert annotate._summarize(row) == "Abra wins 5-0"


def test_prompt_notes_preserves_current_note_on_blank(monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _: "")
    assert annotate._prompt_notes("existing note") == "existing note"


def test_prompt_notes_replaces_current_note(monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _: "replacement")
    assert annotate._prompt_notes("existing note") == "replacement"
