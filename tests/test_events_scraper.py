"""
Tests for the network-free helpers in the events scraper's incremental mode:

    _existing_event_names, _filter_known_links, _merge_event_frames,
    merge_events_into_csv

The scraper lives in scripts/ (a standalone CLI outside the package), so it is
loaded here by file path. The actual HTTP scraping is not exercised.
"""

from __future__ import annotations

import importlib.util

import pandas as pd
import pytest

from fliptop import PROJECT_ROOT

pytest.importorskip("bs4")
pytest.importorskip("requests")

_SCRIPT = PROJECT_ROOT / "scripts" / "fetch_events_metadata_from_fliptop_web.py"
_spec = importlib.util.spec_from_file_location("fetch_events_web", _SCRIPT)
fe = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fe)


def _events(matchup, event_name, desc, vid):
    return pd.DataFrame(
        {
            "matchup": matchup,
            "event_name": event_name,
            "event_description": desc,
            "video_id": vid,
        }
    )


# ---------------------------------------------------------------------------
# _filter_known_links
# ---------------------------------------------------------------------------

def test_filter_known_links_drops_known_names():
    links = [("Ahon 16", "u1"), ("New Event", "u2")]
    assert fe._filter_known_links(links, {"Ahon 16"}) == [("New Event", "u2")]


def test_filter_known_links_noop_without_skip_set():
    links = [("Ahon 16", "u1")]
    assert fe._filter_known_links(links, None) == links
    assert fe._filter_known_links(links, set()) == links


# ---------------------------------------------------------------------------
# _existing_event_names
# ---------------------------------------------------------------------------

def test_existing_event_names_reads_csv(tmp_path):
    p = tmp_path / "events.csv"
    _events(["A vs B"], ["Ahon 16"], ["d"], ["v1"]).to_csv(p, index=False)
    assert fe._existing_event_names(str(p)) == {"Ahon 16"}


def test_existing_event_names_missing_file_is_empty(tmp_path):
    assert fe._existing_event_names(str(tmp_path / "nope.csv")) == set()


# ---------------------------------------------------------------------------
# _merge_event_frames
# ---------------------------------------------------------------------------

def test_merge_upserts_by_video_id_new_wins():
    existing = _events(["A vs B", "C vs D"], ["E1", "E1"], ["old", "keep"], ["v1", "v2"])
    new = _events(["A vs B", "E vs F"], ["E1", "E2"], ["NEW", "added"], ["v1", "v3"])

    out = fe._merge_event_frames(existing, new).set_index("video_id")

    assert out.loc["v1", "event_description"] == "NEW"     # replaced
    assert out.loc["v2", "event_description"] == "keep"    # untouched (outside scrape)
    assert out.loc["v3", "event_description"] == "added"   # newly added
    assert out.index.is_unique


def test_merge_keeps_rows_without_video_id():
    existing = _events(["A vs B"], ["E1"], ["d1"], [None])
    new = _events(["C vs D"], ["E2"], ["d2"], [None])

    out = fe._merge_event_frames(existing, new)

    # both unkeyed rows survive (can't be upserted, so they are preserved)
    assert len(out) == 2
    assert set(out["matchup"]) == {"A vs B", "C vs D"}


# ---------------------------------------------------------------------------
# merge_events_into_csv (file round-trip)
# ---------------------------------------------------------------------------

def test_merge_into_csv_first_write_then_merge(tmp_path):
    p = str(tmp_path / "events.csv")

    fe.merge_events_into_csv(_events(["A vs B"], ["E1"], ["d1"], ["v1"]), p)
    assert len(pd.read_csv(p)) == 1  # file did not exist -> plain write

    fe.merge_events_into_csv(
        _events(["A vs B", "C vs D"], ["E1", "E2"], ["d1-new", "d2"], ["v1", "v2"]), p
    )
    out = pd.read_csv(p).set_index("video_id")
    assert len(out) == 2
    assert out.loc["v1", "event_description"] == "d1-new"  # upserted
    assert out.loc["v2", "event_description"] == "d2"      # added
