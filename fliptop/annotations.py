"""
fliptop.annotations

Storage and helpers for manually-collected battle results, kept deliberately
separate from the auto-built df_battles table.

The authoritative store is an append-only CSV keyed by battle ``id``:

    data/annotations/battle_results.csv
    columns: id, winner, judging_status,
             votes_winner, votes_loser, votes_nv, votes_ot, overtime, notes

Judging is recorded as explicit, structured fields rather than one ambiguous
string:

    judging_status   "scored" | "no_decision" | "unknown"
    votes_winner     judges who voted for the winner   (int, else "NA")
    votes_loser      judges who voted for the loser     (int, else "NA")
    votes_nv         judges who did not vote (NV)        (int, else "NA")
    votes_ot         judges who voted to go to overtime  (int, else "NA")
    overtime         did the battle go to an OT round?   "yes" | "no" | "NA"
    notes            free text, or the literal "none"

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
    "judging_status",
    "votes_winner",
    "votes_loser",
    "votes_nv",
    "votes_ot",
    "overtime",
    "notes",
]

VOTE_COLUMNS = ["votes_winner", "votes_loser", "votes_nv", "votes_ot"]

JUDGING_STATUSES = ("scored", "no_decision", "unknown")
NA = "NA"          # explicit not-applicable marker (no blank cells)
NO_NOTES = "none"  # explicit empty-notes marker

# YouTube id from a watch?v=... or youtu.be/... URL.
_VIDEO_ID_RE = re.compile(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})")

# Legacy judging string patterns (from the old single-column format).
_LEGACY_2 = re.compile(r"^(\d+)-(\d+)$")
_LEGACY_OT = re.compile(r"^(\d+)-(\d+)-(\d+)\(OT\)$", re.I)
_LEGACY_NV = re.compile(r"^(\d+)-(\d+)-(\d+)\(NV\)$", re.I)


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


def validate_status(status: str) -> bool:
    return status in JUDGING_STATUSES


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

    Enforces the cross-field rules: a 'scored' row needs integer vote columns
    and a yes/no overtime; a non-scored row must have NA in those columns.
    """
    problems: list[str] = []
    status = str(row.get("judging_status", "")).strip()

    if not validate_status(status):
        problems.append(f"judging_status must be one of {JUDGING_STATUSES}")
        return problems

    if status == "scored":
        for col in VOTE_COLUMNS:
            v = str(row.get(col, "")).strip()
            if v == NA or not v.isdigit():
                problems.append(f"{col} must be an integer when scored (got {v!r})")
        if str(row.get("overtime", "")).strip() not in ("yes", "no"):
            problems.append("overtime must be 'yes' or 'no' when scored")
    else:
        for col in VOTE_COLUMNS + ["overtime"]:
            if str(row.get(col, "")).strip() != NA:
                problems.append(f"{col} must be {NA!r} when status is {status!r}")
    return problems


# ---------------------------------------------------------------------------
# Legacy judging string -> structured fields
# ---------------------------------------------------------------------------

def parse_legacy_judging(raw) -> dict:
    """
    Convert an old single-column judging value into structured fields.

    Returns a dict with judging_status / vote columns / overtime, plus a
    boolean ``needs_review`` flagging values whose interpretation is uncertain
    (the ambiguous '(OT)' cases and anything unrecognized).
    """
    def scored(vw, vl, nv, ot, overtime, review=False):
        return {
            "judging_status": "scored",
            "votes_winner": str(vw),
            "votes_loser": str(vl),
            "votes_nv": str(nv),
            "votes_ot": str(ot),
            "overtime": overtime,
            "needs_review": review,
        }

    def non_scored(status, review=False):
        return {
            "judging_status": status,
            "votes_winner": NA,
            "votes_loser": NA,
            "votes_nv": NA,
            "votes_ot": NA,
            "overtime": NA,
            "needs_review": review,
        }

    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return non_scored("unknown")
    s = str(raw).strip()
    if not s:
        return non_scored("unknown")
    if s.casefold() == "promo":
        return non_scored("no_decision", review=True)

    m = _LEGACY_2.match(s)
    if m:
        return scored(m.group(1), m.group(2), 0, 0, "no")

    m = _LEGACY_NV.match(s)
    if m:
        return scored(m.group(1), m.group(2), int(m.group(3)), 0, "no")

    m = _LEGACY_OT.match(s)
    if m:
        third = int(m.group(3))
        # Ambiguous legacy form: a non-zero third number means that many judges
        # voted for overtime (battle did NOT go to OT); a zero third number with
        # the (OT) flag means the battle DID go to overtime. Flag for review.
        if third > 0:
            return scored(m.group(1), m.group(2), 0, third, "no", review=True)
        return scored(m.group(1), m.group(2), 0, 0, "yes", review=True)

    return non_scored("unknown", review=True)


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
    judging_status: str = "unknown",
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
        "judging_status": judging_status,
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


# ---------------------------------------------------------------------------
# One-time migration from the legacy xlsx
# ---------------------------------------------------------------------------

def migrate_from_xlsx(
    xlsx_path: PathLike,
    df_battles: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Convert legacy battle_winners_review.xlsx annotations into the structured,
    id-keyed store, recovering each battle id from its URL and parsing the old
    judging string into structured fields.

    Returns
    -------
    (results, review)
        results: structured DataFrame ready for save_results.
        review:  rows that could not be matched, whose winner does not match the
                 battle's emcees, or whose judging needs a human to confirm
                 (the ambiguous '(OT)' cases).
    """
    legacy = pd.read_excel(xlsx_path)
    annotated = legacy[legacy["winner"].notna()].copy()

    bk = df_battles.copy()
    bk["battle_key"] = bk["id"].map(battle_key)
    emcees = bk.set_index("battle_key")[["emcee1", "emcee2"]].to_dict("index")

    rows: list[dict] = []
    review: list[dict] = []

    for _, r in annotated.iterrows():
        first_url = str(r.get("url", "")).split("|")[0].strip()
        key = extract_video_id(first_url)
        winner = str(r["winner"]).strip()
        notes = "" if pd.isna(r.get("notes")) else str(r.get("notes")).strip()

        if key is None or key not in emcees:
            review.append({**r.to_dict(), "_reason": "no matching battle id"})
            continue

        parsed = parse_legacy_judging(r.get("judging"))
        flagged = parsed.pop("needs_review", False)

        em = emcees[key]
        if winner and not validate_winner(winner, em["emcee1"], em["emcee2"]):
            review.append({**r.to_dict(), "_reason": "winner not an emcee of this battle"})

        row = make_result_row(id=key, winner=winner, notes=notes, **parsed)
        rows.append(row)

        if flagged:
            review.append({
                **r.to_dict(),
                "_reason": f"confirm judging -> {parsed['judging_status']}, "
                           f"ot={parsed['overtime']}, votes_ot={parsed['votes_ot']}",
            })

    results = (
        pd.DataFrame(rows, columns=RESULTS_COLUMNS)
        .drop_duplicates(subset=["id"], keep="last")
    )
    return results, pd.DataFrame(review)
