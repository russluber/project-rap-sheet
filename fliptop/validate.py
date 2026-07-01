"""
fliptop.validate

Output data-quality gate for the built ``df_battles`` table.

``build_df_battles`` is a long chain of heuristic filters, merges, and overrides;
a change in a raw source's shape (a re-scrape, a YouTube API tweak) can silently
produce a malformed table - blank emcee names, duplicate battles, dates in the
future - that still writes cleanly to disk. :func:`validate_df_battles` checks the
invariants the pipeline is supposed to guarantee and returns a list of
human-readable problems (empty == ok), mirroring
:func:`fliptop.annotations.validate_result_row`.

The refresh CLI runs it after every build and aborts *before writing* if anything
is wrong, so a regression fails loudly instead of shipping a broken
``data/processed/df_battles.json``.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from .annotations import battle_key
from .battles import FINAL_COLUMNS

# event_date_source is a small closed vocabulary; missing (undated battles) is ok.
EVENT_DATE_SOURCES = {"website", "description", "versetracker", "manual"}

# FlipTop's first battles are from 2010; nothing should be dated before then, and
# no battle can have happened in the future.
EARLIEST_EVENT_DATE = pd.Timestamp("2010-01-01")


def validate_df_battles(df: pd.DataFrame, *, today: date | None = None) -> list[str]:
    """
    Return a list of data-quality problems with a built ``df_battles`` (empty == ok).

    Checks the invariants ``build_df_battles`` should guarantee:

    * every expected column is present (see :data:`fliptop.battles.FINAL_COLUMNS`);
    * one row per battle - the scalar battle key (first id for consolidated
      multi-part battles) is present and unique;
    * every battle has two non-blank emcees;
    * ``event_date_source`` is drawn from the known vocabulary (missing allowed);
    * ``event_date`` is within a plausible window (>= 2010, not in the future).

    ``today`` overrides the upper date bound (defaults to the current date); handy
    for deterministic tests.
    """
    problems: list[str] = []

    if df.empty:
        problems.append("df_battles is empty")
        return problems

    missing = [c for c in FINAL_COLUMNS if c not in df.columns]
    if missing:
        problems.append(f"missing expected columns: {', '.join(missing)}")

    # One row per battle: unique, present scalar key.
    if "id" in df.columns:
        keys = df["id"].map(battle_key)
        n_missing = int(keys.isna().sum())
        if n_missing:
            problems.append(f"{n_missing} battle(s) have no usable id")
        present = keys.dropna()
        dup_values = present[present.duplicated()].unique().tolist()
        if dup_values:
            shown = ", ".join(map(str, dup_values[:5]))
            more = "" if len(dup_values) <= 5 else f" (+{len(dup_values) - 5} more)"
            problems.append(f"{len(dup_values)} duplicate battle id(s): {shown}{more}")

    # Every battle needs two named emcees.
    for col in ("emcee1", "emcee2"):
        if col in df.columns:
            blank = df[col].isna() | (df[col].astype("string").str.strip() == "")
            n_blank = int(blank.sum())
            if n_blank:
                problems.append(f"{n_blank} battle(s) have a blank {col}")

    # event_date_source is a closed vocabulary (missing = undated battle, allowed).
    if "event_date_source" in df.columns:
        seen = set(df["event_date_source"].dropna().unique())
        unknown = sorted(seen - EVENT_DATE_SOURCES)
        if unknown:
            problems.append(
                "unexpected event_date_source value(s): "
                + ", ".join(map(repr, unknown))
            )

    # event_date within a plausible window (NaT rows are left undated and skipped).
    if "event_date" in df.columns:
        dates = pd.to_datetime(df["event_date"], errors="coerce")
        upper = pd.Timestamp(today or date.today())
        n_early = int((dates < EARLIEST_EVENT_DATE).sum())
        n_future = int((dates > upper).sum())
        if n_early:
            problems.append(
                f"{n_early} event_date(s) before {EARLIEST_EVENT_DATE.date()}"
            )
        if n_future:
            problems.append(f"{n_future} event_date(s) in the future (after {upper.date()})")

    return problems


def summarize_df_battles(df: pd.DataFrame) -> str:
    """
    One-line build summary: battle count and the ``event_date_source`` breakdown.

    Printed by the refresh CLI as a quick sanity read on each build.
    """
    n = len(df)
    if "event_date_source" not in df.columns:
        return f"{n} battles"

    counts = df["event_date_source"].value_counts(dropna=False)
    parts = [f"{'none' if pd.isna(k) else k}={v}" for k, v in counts.items()]
    return f"{n} battles; event_date_source: {', '.join(parts)}"
