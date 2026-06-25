"""
fliptop.annotations

Storage and helpers for manually-collected battle results, kept deliberately
separate from the auto-built df_battles table.

The authoritative store is an append-only CSV keyed by battle ``id``:

    data/annotations/battle_results.csv
    columns: id, winner, battle_type,
             votes_winner, votes_loser, votes_nv, votes_ot, overtime, notes

A battle is one of two kinds, which the host announces, and the rest of the
fields are recorded as explicit, structured values:

    battle_type      "judged" | "promo"
                     "judged" = a real, decided battle with a winner.
                     "promo"  = exhibition/promo bout with no winner by design.
    winner           the winning emcee for a judged battle, else "NA" (promo).
    votes_winner     judges who voted for the winner   (int, else "NA")
    votes_loser      judges who voted for the loser     (int, else "NA")
    votes_nv         judges who did not vote (NV)        (int, else "NA")
    votes_ot         judges who voted to go to overtime  (int, else "NA")
    overtime         did the battle go to an OT round?   "yes" | "no" | "NA"
    notes            free text, or the literal "none"

A judged battle whose score was not recorded keeps its ``winner`` but stores
"NA" in every vote column (and ``overtime``); the vote columns being "NA" is
exactly what marks "winner known, score unknown".

The recorded vote tally is always the *final* (post-overtime) tally. Panel size
is not fixed (5 / 7 / 9 judges) and is simply the sum of the vote columns. All
values are stored as text so the CSV has no blank cells; convert the vote
columns with ``pd.to_numeric`` for analysis.

The store is never regenerated wholesale - the annotate workflow only adds rows
for battles not yet recorded. Use ``merge_results`` to join it onto df_battles
on demand; df_battles.json itself is left clean.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd

from . import DATA_DIR

PathLike = str | Path

ANNOTATIONS_DIR = DATA_DIR / "annotations"
RESULTS_PATH = ANNOTATIONS_DIR / "battle_results.csv"

RESULTS_COLUMNS = [
    "id",
    "winner",
    "battle_type",
    "votes_winner",
    "votes_loser",
    "votes_nv",
    "votes_ot",
    "overtime",
    "notes",
]

VOTE_COLUMNS = ["votes_winner", "votes_loser", "votes_nv", "votes_ot"]

BATTLE_TYPES = ("judged", "promo")
NA = "NA"          # explicit not-applicable marker (no blank cells)
NO_NOTES = "none"  # explicit empty-notes marker

# YouTube id from a watch?v=... or youtu.be/... URL.
_VIDEO_ID_RE = re.compile(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})")


# ---------------------------------------------------------------------------
# Battle identity
# ---------------------------------------------------------------------------

def battle_key(id_value) -> Optional[str]:
    """
    Canonical scalar key for a battle.

    df_battles stores ``id`` as a string for single uploads and as a list of
    part ids for consolidated multi-part battles; the key is the first id.
    """
    if isinstance(id_value, list):
        return id_value[0] if id_value else None
    if pd.isna(id_value):
        return None
    return str(id_value)


def extract_video_id(url: str) -> Optional[str]:
    """Pull the YouTube video id out of a watch / youtu.be URL (or None)."""
    if not isinstance(url, str):
        return None
    m = _VIDEO_ID_RE.search(url)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_winner(winner: str, emcee1: str, emcee2: str) -> bool:
    """A winner must be one of the two emcees (case-insensitive, trimmed)."""
    if not isinstance(winner, str):
        return False
    w = winner.strip().casefold()
    return w in {str(emcee1).strip().casefold(), str(emcee2).strip().casefold()}


def validate_battle_type(battle_type: str) -> bool:
    return battle_type in BATTLE_TYPES


def validate_votes(value) -> bool:
    """A vote count is a non-negative integer, or the NA marker."""
    if value is None:
        return False
    s = str(value).strip()
    if s == NA:
        return True
    return s.isdigit()


def validate_overtime(value) -> bool:
    return str(value).strip() in ("yes", "no", NA)


def validate_result_row(row: dict) -> list[str]:
    """
    Return a list of human-readable problems with a result row (empty == ok).

    Enforces the cross-field rules:

    * promo  - no winner: ``winner`` and every vote column and ``overtime`` are
      all the NA marker.
    * judged - a real winner. The score is either fully recorded (every vote
      column an integer and ``overtime`` yes/no) or not recorded at all (every
      vote column NA and ``overtime`` NA); a half-filled tally is rejected.
    """
    problems: list[str] = []
    battle_type = str(row.get("battle_type", "")).strip()

    if not validate_battle_type(battle_type):
        problems.append(f"battle_type must be one of {BATTLE_TYPES}")
        return problems

    votes = [str(row.get(col, "")).strip() for col in VOTE_COLUMNS]
    overtime = str(row.get("overtime", "")).strip()
    winner = str(row.get("winner", "")).strip()

    if battle_type == "promo":
        if winner != NA:
            problems.append(f"winner must be {NA!r} for a promo battle")
        for col, v in zip(VOTE_COLUMNS + ["overtime"], votes + [overtime]):
            if v != NA:
                problems.append(f"{col} must be {NA!r} for a promo battle")
        return problems

    # judged
    if winner == NA or not winner:
        problems.append("winner must be a real emcee for a judged battle")

    all_int = all(v.isdigit() for v in votes)
    all_na = all(v == NA for v in votes)
    if all_int:
        if overtime not in ("yes", "no"):
            problems.append("overtime must be 'yes' or 'no' when the score is recorded")
    elif all_na:
        if overtime != NA:
            problems.append(f"overtime must be {NA!r} when the score is not recorded")
    else:
        problems.append(
            "vote columns must be either all integers (score recorded) "
            f"or all {NA!r} (score unknown), not a mix"
        )
    return problems


# ---------------------------------------------------------------------------
# Store I/O
# ---------------------------------------------------------------------------

def load_results(path: PathLike = RESULTS_PATH) -> pd.DataFrame:
    """Load the results store as text, or an empty frame with the right schema."""
    path = Path(path)
    if not path.exists():
        return pd.DataFrame(columns=RESULTS_COLUMNS)
    # keep_default_na=False so the literal "NA"/"none" markers stay as text.
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    for col in RESULTS_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[RESULTS_COLUMNS]


def save_results(results: pd.DataFrame, path: PathLike = RESULTS_PATH) -> Path:
    """Write the results store as CSV (sorted by id for stable diffs)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    out = results.reindex(columns=RESULTS_COLUMNS).sort_values("id")
    out.to_csv(path, index=False)
    return path


def make_result_row(
    *,
    id: str,
    winner: str = NA,
    battle_type: str = "judged",
    votes_winner=NA,
    votes_loser=NA,
    votes_nv=NA,
    votes_ot=NA,
    overtime: str = NA,
    notes: str = NO_NOTES,
) -> dict:
    """Build a fully-populated (no blank cells) result row."""
    return {
        "id": id,
        "winner": winner if winner else NA,
        "battle_type": battle_type,
        "votes_winner": str(votes_winner),
        "votes_loser": str(votes_loser),
        "votes_nv": str(votes_nv),
        "votes_ot": str(votes_ot),
        "overtime": overtime,
        "notes": notes if str(notes).strip() else NO_NOTES,
    }


def upsert_result(results: pd.DataFrame, row: dict) -> pd.DataFrame:
    """Insert or replace the row for ``row['id']``; returns a new frame."""
    results = results[results["id"] != row["id"]]
    new = pd.DataFrame([row], columns=RESULTS_COLUMNS)
    return pd.concat([results, new], ignore_index=True)


# ---------------------------------------------------------------------------
# Pending / merge
# ---------------------------------------------------------------------------

def pending_battles(
    df_battles: pd.DataFrame,
    results: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Battles from df_battles not yet in the results store, newest upload first.

    Adds a scalar ``battle_key`` column for joining/recording. A battle is
    considered done once it has a row in the store.
    """
    if results is None:
        results = load_results()

    done = set(results["id"])

    work = df_battles.copy()
    work["battle_key"] = work["id"].map(battle_key)
    pending = work[~work["battle_key"].isin(done)]

    if "upload_date" in pending.columns:
        pending = pending.sort_values("upload_date", ascending=False)
    return pending.reset_index(drop=True)


def merge_results(
    df_battles: pd.DataFrame,
    results: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Left-join the results store onto df_battles by battle key (analysis helper).

    Returns a new frame with the result columns added; does not mutate the
    input and does not touch df_battles.json. Vote columns remain text (with
    'NA' where not applicable); convert with pd.to_numeric for analysis.
    """
    if results is None:
        results = load_results()

    out = df_battles.copy()
    out["battle_key"] = out["id"].map(battle_key)
    merged = out.merge(
        results.rename(columns={"id": "battle_key"}),
        on="battle_key",
        how="left",
    )
    return merged
