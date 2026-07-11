"""
fliptop.validate

Output data-quality gates for the built battle tables.

The raw-to-metadata build is a long chain of heuristic filters, merges, and
overrides; a change in a raw source's shape (a re-scrape, a YouTube API tweak)
can silently produce a malformed table. The final ``ft_battles`` table then adds
the id-keyed battle results and selects the project-level analysis columns.

``validate_battle_metadata`` guards the rich intermediate metadata table.
``validate_ft_battles`` guards the final result-enriched table written to
``data/processed/ft_battles.json``.

The refresh CLI runs it after every build and aborts *before writing* if anything
is wrong, so a regression fails loudly instead of shipping a broken
``data/processed/ft_battles.json``.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from .annotations import BATTLE_TYPES, NA, battle_key, validate_votes, validate_winner
from .battles import METADATA_COLUMNS
from .publish import FINAL_COLUMNS, FINAL_OUTPUT_FORBIDDEN_COLUMNS

# event_date_source is a small closed vocabulary; missing (undated battles) is ok.
EVENT_DATE_SOURCES = {"website", "description", "versetracker", "manual"}

# FlipTop's first battles are from 2010; nothing should be dated before then, and
# no battle can have happened in the future.
EARLIEST_EVENT_DATE = pd.Timestamp("2010-01-01")


def _check_expected_columns(
    df: pd.DataFrame,
    expected_columns: list[str],
    label: str,
) -> list[str]:
    missing = [c for c in expected_columns if c not in df.columns]
    if missing:
        return [f"{label} is missing expected columns: {', '.join(missing)}"]
    if list(df.columns) != expected_columns:
        return [f"{label} columns are out of order or include unexpected columns"]
    return []


def _check_battle_identity(
    df: pd.DataFrame,
    *,
    label: str,
    allow_list_ids: bool,
) -> list[str]:
    problems: list[str] = []
    if "id" not in df.columns:
        return problems

    if not allow_list_ids:
        list_ids = df["id"].map(lambda x: isinstance(x, list))
        if bool(list_ids.any()):
            problems.append(f"{int(list_ids.sum())} {label} row(s) have list-valued id")

    keys = df["id"].map(battle_key)
    n_missing = int(keys.isna().sum())
    if n_missing:
        problems.append(f"{n_missing} {label} row(s) have no usable id")
    present = keys.dropna()
    dup_values = present[present.duplicated()].unique().tolist()
    if dup_values:
        shown = ", ".join(map(str, dup_values[:5]))
        more = "" if len(dup_values) <= 5 else f" (+{len(dup_values) - 5} more)"
        problems.append(f"{len(dup_values)} duplicate battle id(s): {shown}{more}")
    return problems


def _check_emcees(df: pd.DataFrame) -> list[str]:
    problems: list[str] = []
    for col in ("emcee1", "emcee2"):
        if col in df.columns:
            blank = df[col].isna() | (df[col].astype("string").str.strip() == "")
            n_blank = int(blank.sum())
            if n_blank:
                problems.append(f"{n_blank} battle(s) have a blank {col}")
    if {"emcee1", "emcee2"} <= set(df.columns):
        same = df["emcee1"].astype("string").str.strip() == df["emcee2"].astype("string").str.strip()
        if bool(same.any()):
            problems.append(f"{int(same.sum())} battle(s) have the same emcee twice")
    return problems


def _check_required_text(df: pd.DataFrame, columns: tuple[str, ...]) -> list[str]:
    problems: list[str] = []
    for column in columns:
        if column not in df.columns:
            continue
        blank = df[column].isna() | df[column].astype("string").str.strip().eq("")
        count = int(blank.sum())
        if count:
            problems.append(f"{count} battle(s) have a blank {column}")
    return problems


def _check_event_dates(df: pd.DataFrame, *, today: date | None = None) -> list[str]:
    problems: list[str] = []
    if "event_date" not in df.columns:
        return problems

    dates = pd.to_datetime(df["event_date"], errors="coerce")
    upper = pd.Timestamp(today or date.today())
    n_early = int((dates < EARLIEST_EVENT_DATE).sum())
    n_future = int((dates > upper).sum())
    if n_early:
        problems.append(f"{n_early} event_date(s) before {EARLIEST_EVENT_DATE.date()}")
    if n_future:
        problems.append(f"{n_future} event_date(s) in the future (after {upper.date()})")
    return problems


def validate_battle_metadata(
    df: pd.DataFrame,
    *,
    today: date | None = None,
) -> list[str]:
    """
    Return data-quality problems with the rich battle metadata table.

    This table may still carry list-valued ``id``/``url`` for consolidated
    multi-part battles and includes provenance columns such as
    ``event_date_source``.
    """
    problems: list[str] = []

    if df.empty:
        problems.append("battle metadata is empty")
        return problems

    problems.extend(_check_expected_columns(df, METADATA_COLUMNS, "battle metadata"))
    problems.extend(_check_battle_identity(df, label="metadata", allow_list_ids=True))
    problems.extend(_check_emcees(df))
    problems.extend(_check_required_text(df, ("event_name", "event_location")))

    # event_date_source is a closed vocabulary (missing = undated battle, allowed).
    if "event_date_source" in df.columns:
        seen = set(df["event_date_source"].dropna().unique())
        unknown = sorted(seen - EVENT_DATE_SOURCES)
        if unknown:
            problems.append(
                "unexpected event_date_source value(s): "
                + ", ".join(map(repr, unknown))
            )

    problems.extend(_check_event_dates(df, today=today))

    return problems


def validate_ft_battles(df: pd.DataFrame, *, today: date | None = None) -> list[str]:
    """
    Return data-quality problems with the final result-enriched ``ft_battles``.

    Checks the published output schema, rejects metadata/audit-only columns,
    requires one scalar id per battle, verifies non-blank emcees, plausible
    event dates, valid result fields, and winners that match one of the two
    emcees when a judged battle has a winner.
    """
    problems: list[str] = []

    if df.empty:
        problems.append("ft_battles is empty")
        return problems

    forbidden = [c for c in FINAL_OUTPUT_FORBIDDEN_COLUMNS if c in df.columns]
    if forbidden:
        problems.append(
            "ft_battles contains metadata/audit-only column(s): "
            + ", ".join(forbidden)
        )

    problems.extend(_check_expected_columns(df, FINAL_COLUMNS, "ft_battles"))
    problems.extend(_check_battle_identity(df, label="ft_battles", allow_list_ids=False))
    problems.extend(_check_emcees(df))
    problems.extend(_check_event_dates(df, today=today))

    if "battle_type" in df.columns:
        battle_type = df["battle_type"].astype("string").str.strip()
        missing = battle_type.isna() | (battle_type == "")
        if bool(missing.any()):
            problems.append(f"{int(missing.sum())} battle(s) are missing battle_type")
        unknown = sorted(set(battle_type.dropna()) - set(BATTLE_TYPES))
        if unknown:
            problems.append("unexpected battle_type value(s): " + ", ".join(map(repr, unknown)))

    for col in ("votes_winner", "votes_loser"):
        if col in df.columns:
            bad = ~df[col].map(validate_votes)
            if bool(bad.any()):
                problems.append(f"{int(bad.sum())} battle(s) have invalid {col}")

    if {"battle_type", "winner", "emcee1", "emcee2"} <= set(df.columns):
        for idx, row in df.iterrows():
            battle_type = str(row["battle_type"]).strip()
            winner = str(row["winner"]).strip()
            if battle_type == "promo" and winner != NA:
                problems.append(f"row {idx}: promo battle has non-NA winner {winner!r}")
            if (
                battle_type == "judged"
                and winner != NA
                and not validate_winner(winner, row["emcee1"], row["emcee2"])
            ):
                problems.append(
                    f"row {idx}: winner {winner!r} is not one of "
                    f"{row['emcee1']!r} / {row['emcee2']!r}"
                )

    return problems


def summarize_battle_metadata(df: pd.DataFrame) -> str:
    """One-line metadata build summary."""
    n = len(df)
    if "event_date_source" not in df.columns:
        return f"{n} battles"

    counts = df["event_date_source"].value_counts(dropna=False)
    parts = [f"{'none' if pd.isna(k) else k}={v}" for k, v in counts.items()]
    return f"{n} battles; event_date_source: {', '.join(parts)}"


def summarize_ft_battles(df: pd.DataFrame) -> str:
    """
    One-line final-output summary: battle count and result-type breakdown.

    Printed by the refresh CLI as a quick sanity read on each build.
    """
    n = len(df)
    if "battle_type" not in df.columns:
        return f"{n} battles"

    counts = df["battle_type"].value_counts(dropna=False)
    parts = [f"{'none' if pd.isna(k) else k}={v}" for k, v in counts.items()]
    return f"{n} battles; battle_type: {', '.join(parts)}"
