"""
Tests for the network-free helpers in the VerseTracker event-date scraper:

    event_slug, event_url, parse_event_date, write_event_dates_to_csv

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

from bs4 import BeautifulSoup  # noqa: E402  (after importorskip)

_SCRIPT = PROJECT_ROOT / "scripts" / "fetch_versetracker_event_dates.py"
_spec = importlib.util.spec_from_file_location("fetch_versetracker", _SCRIPT)
vt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(vt)


# ---------------------------------------------------------------------------
# event_slug / event_url
# ---------------------------------------------------------------------------

def test_event_slug_basic_and_multiword():
    assert vt.event_slug("Ahon 12") == "fliptop-ahon-12"
    assert vt.event_slug("Bwelta Balentong 7") == "fliptop-bwelta-balentong-7"
    assert vt.event_slug("Second Sight 8") == "fliptop-second-sight-8"


def test_event_slug_collapses_punctuation_and_trims():
    # multiple non-alnum chars collapse to one hyphen; no leading/trailing hyphen
    assert vt.event_slug("  Grain   Assault  11 ") == "fliptop-grain-assault-11"
    assert vt.event_slug("Won Minutes (Cebu)") == "fliptop-won-minutes-cebu"


def test_event_url_uses_base_and_slug():
    assert vt.event_url("Ahon 12") == "https://versetracker.com/events/fliptop-ahon-12"
    assert (
        vt.event_url("Ahon 12", base="https://example.com")
        == "https://example.com/events/fliptop-ahon-12"
    )


# ---------------------------------------------------------------------------
# parse_event_date
# ---------------------------------------------------------------------------

def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def test_parse_event_date_extracts_iso_from_element():
    html = (
        '<div class="event-info"><h1>AHON 12</h1>'
        '<div class="event-date"><img src="x.png" />December 8, 2021</div></div>'
    )
    assert vt.parse_event_date(_soup(html)) == "2021-12-08"


def test_parse_event_date_missing_element_is_none():
    assert vt.parse_event_date(_soup("<div>no date here</div>")) is None


def test_parse_event_date_unparseable_text_is_none():
    html = '<div class="event-date"><img/>coming soon</div>'
    assert vt.parse_event_date(_soup(html)) is None


# ---------------------------------------------------------------------------
# write_event_dates_to_csv (file round-trip, sorted, fixed schema)
# ---------------------------------------------------------------------------

def test_write_event_dates_to_csv_sorts_and_keeps_schema(tmp_path):
    p = str(tmp_path / "vt.csv")
    df = pd.DataFrame(
        {
            "event_name": ["Zoning 10", "Ahon 12"],
            "event_date": ["2020-09-30", "2021-12-08"],
            "source_url": ["u2", "u1"],
        }
    )
    vt.write_event_dates_to_csv(df, p)
    out = pd.read_csv(p)
    assert list(out.columns) == ["event_name", "event_date", "source_url"]
    assert out["event_name"].tolist() == ["Ahon 12", "Zoning 10"]  # sorted by name


def test_strict_scrape_refuses_partial_reference_overwrite(monkeypatch):
    monkeypatch.setattr(vt, "fetch_event_date", lambda *args, **kwargs: None)
    monkeypatch.setattr(vt.time, "sleep", lambda *_: None)

    with pytest.raises(vt.IncompleteScrapeError, match="refusing partial"):
        vt.scrape_event_dates(["Missing Event"], strict=True, verbose=False)
