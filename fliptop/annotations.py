"""
fliptop.annotations

Storage and helpers for manually-collected battle results, kept deliberately
separate from the auto-built battle metadata table.

The authoritative store is an append-only CSV keyed by battle ``id``:

    data/annotations/battle_results.csv
    columns: id, battle_type, winner,
             votes_winner, votes_loser, votes_nv, votes_ot, overtime, notes

A battle is one of two kinds, which the host announces. A draw is a judged
battle with no winner:

    battle_type      "judged" | "promo"
                     "judged" = judges decided the result (winner or draw).
                     "promo"  = exhibition/promo bout with no judging.
    winner           the winning emcee; "NA" for a draw or promo.
    votes_winner     judges who voted for the winner     (int, else "NA")
    votes_loser      judges who voted for the loser      (int, else "NA")
    votes_nv         judges who did not vote (NV)        (int, else "NA")
    votes_ot         judges who voted to go to overtime  (int, else "NA")
    overtime         did the battle go to an OT round?   "yes" | "no" | "NA"
    notes            free text, or the literal "none"

A decided battle whose score was not recorded keeps its ``winner`` but stores
"NA" in every vote column (and ``overtime``). A draw stores ``winner="NA"`` and
also leaves the structured judging fields as "NA"; unusual rulings belong in
``notes``. A promo likewise has no winner or judging fields, but is distinguished
from a draw by ``battle_type``.

The recorded vote tally is always the *final* (post-overtime) tally. Panel size
is not fixed (5 / 7 / 9 judges) and is simply the sum of the vote columns. All
values are stored as text so the CSV has no blank cells; convert the vote
columns with ``pd.to_numeric`` for analysis.

The store is never regenerated wholesale - the annotate workflow only adds rows
for battles not yet recorded. The refresh/build pipeline validates it against
the battle metadata and joins the core result fields into the final
``ft_battles.json`` output.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from . import DATA_DIR

PathLike = str | Path

ANNOTATIONS_DIR = DATA_DIR / "annotations"
RESULTS_PATH = ANNOTATIONS_DIR / "battle_results.csv"

RESULTS_COLUMNS = [
    "id",
    "battle_type",
    "winner",
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

def battle_key(id_value) -> str | None:
    """
    Canonical scalar key for a battle.

    Accepts either the published scalar id or the rich metadata layer's
    list-valued multi-part id; the key is the first id.
    """
    if isinstance(id_value, list):
        return id_value[0] if id_value else None
    if pd.isna(id_value):
        return None
    return str(id_value)


def extract_video_id(url: str) -> str | None:
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

    * promo - ``winner``, every vote column, and ``overtime`` are all NA.
    * judged draw - ``winner``, every vote column, and ``overtime`` are all NA.
    * judged decision - a real winner. The score is either fully recorded
      (every vote column an integer and ``overtime`` yes/no) or not recorded at
      all (every vote column NA and ``overtime`` NA); a half-filled tally is
      rejected.
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

    # A completed judged row without a winner is a draw. Draw tallies stay in
    # notes because the announced rulings are not consistently score-shaped.
    if winner == NA:
        for col, v in zip(VOTE_COLUMNS + ["overtime"], votes + [overtime]):
            if v != NA:
                problems.append(f"{col} must be {NA!r} for a draw")
        return problems

    if not winner:
        problems.append(f"winner must be a real emcee or {NA!r} for a judged battle")

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


def validate_results_store(
    results: pd.DataFrame,
    ft_battles: pd.DataFrame | None = None,
    *,
    require_complete: bool = True,
) -> list[str]:
    """
    Validate the results table by itself, and optionally against a battles table.

    This is the gate used before publishing result-enriched ``ft_battles``. It
    checks the CSV-level contract (schema, unique ids, no blank cells, valid row
    structure), then, when ``ft_battles`` is supplied, checks id alignment and
    verifies that each non-NA winner is one of the two emcees in that battle.
    """
    problems: list[str] = []

    actual = list(results.columns)
    if actual != RESULTS_COLUMNS:
        missing = [col for col in RESULTS_COLUMNS if col not in actual]
        unexpected = [col for col in actual if col not in RESULTS_COLUMNS]
        details = []
        if missing:
            details.append(f"missing: {', '.join(missing)}")
        if unexpected:
            details.append(f"unexpected: {', '.join(unexpected)}")
        if not missing and not unexpected:
            details.append("columns are out of order")
        problems.append("results schema mismatch (" + "; ".join(details) + ")")
        return problems

    if results.empty:
        problems.append("battle_results is empty")

    blank = results.map(lambda v: str(v).strip() == "")
    if bool(blank.any().any()):
        n_blank = int(blank.sum().sum())
        problems.append(f"battle_results has {n_blank} blank cell(s)")

    ids = results["id"].astype("string").str.strip()
    n_missing = int((ids.isna() | (ids == "")).sum())
    if n_missing:
        problems.append(f"{n_missing} result row(s) have a blank id")
    dupes = ids[ids.notna() & ids.duplicated()].dropna().unique().tolist()
    if dupes:
        shown = ", ".join(map(str, dupes[:5]))
        more = "" if len(dupes) <= 5 else f" (+{len(dupes) - 5} more)"
        problems.append(f"{len(dupes)} duplicate result id(s): {shown}{more}")

    for idx, row in results.iterrows():
        row_problems = validate_result_row(row.to_dict())
        for problem in row_problems:
            problems.append(f"row {idx}: {problem}")

    if ft_battles is None or "id" not in ft_battles.columns:
        return problems

    work = ft_battles.copy()
    work["_battle_key"] = work["id"].map(battle_key)
    battle_keys = set(work["_battle_key"].dropna().astype(str))
    result_ids = set(ids.dropna().astype(str))

    orphan = sorted(result_ids - battle_keys)
    if orphan:
        shown = ", ".join(orphan[:5])
        more = "" if len(orphan) <= 5 else f" (+{len(orphan) - 5} more)"
        problems.append(f"{len(orphan)} result id(s) do not match a battle: {shown}{more}")

    missing = sorted(battle_keys - result_ids)
    if require_complete and missing:
        shown = ", ".join(missing[:5])
        more = "" if len(missing) <= 5 else f" (+{len(missing) - 5} more)"
        problems.append(f"{len(missing)} battle(s) are missing results: {shown}{more}")

    if {"emcee1", "emcee2"} <= set(work.columns):
        lookup = work.dropna(subset=["_battle_key"]).set_index("_battle_key")
        for idx, row in results.iterrows():
            result_id = str(row["id"]).strip()
            winner = str(row["winner"]).strip()
            if winner == NA or result_id not in lookup.index:
                continue
            battle = lookup.loc[result_id]
            if isinstance(battle, pd.DataFrame):
                battle = battle.iloc[0]
            if not validate_winner(winner, battle["emcee1"], battle["emcee2"]):
                problems.append(
                    f"row {idx}: winner {winner!r} is not one of "
                    f"{battle['emcee1']!r} / {battle['emcee2']!r}"
                )

    return problems


# ---------------------------------------------------------------------------
# Store I/O
# ---------------------------------------------------------------------------

def _require_results_schema(results: pd.DataFrame, source: PathLike) -> None:
    """Raise clearly rather than silently dropping or inventing result columns."""
    actual = list(results.columns)
    if actual == RESULTS_COLUMNS:
        return

    missing = [col for col in RESULTS_COLUMNS if col not in actual]
    unexpected = [col for col in actual if col not in RESULTS_COLUMNS]
    details = []
    if missing:
        details.append(f"missing: {', '.join(missing)}")
    if unexpected:
        details.append(f"unexpected: {', '.join(unexpected)}")
    if not missing and not unexpected:
        details.append("columns are out of order")
    detail = "; ".join(details)
    raise ValueError(
        f"{source}: results schema does not match the current format ({detail}). "
        f"Expected columns, in order: {', '.join(RESULTS_COLUMNS)}"
    )


def load_results(path: PathLike = RESULTS_PATH) -> pd.DataFrame:
    """Load the results store as text, or an empty frame with the right schema."""
    path = Path(path)
    if not path.exists():
        return pd.DataFrame(columns=RESULTS_COLUMNS)
    # keep_default_na=False so the literal "NA"/"none" markers stay as text.
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    _require_results_schema(df, path)
    return df


def save_results(results: pd.DataFrame, path: PathLike = RESULTS_PATH) -> Path:
    """Write the results store as CSV (sorted by id for stable diffs)."""
    path = Path(path)
    _require_results_schema(results, path)
    path.parent.mkdir(parents=True, exist_ok=True)
    out = results.sort_values("id")
    out.to_csv(path, index=False)
    return path


def make_result_row(
    *,
    id: str,
    battle_type: str = "judged",
    winner: str = NA,
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
        "battle_type": battle_type,
        "winner": winner if winner else NA,
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
    ft_battles: pd.DataFrame,
    results: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Battles from ft_battles not yet in the results store, newest upload first.

    Adds a scalar ``battle_key`` column for joining/recording. A battle is
    considered done once it has a row in the store.
    """
    if results is None:
        results = load_results()

    done = set(results["id"])

    work = ft_battles.copy()
    work["battle_key"] = work["id"].map(battle_key)
    pending = work[~work["battle_key"].isin(done)]

    if "upload_date" in pending.columns:
        pending = pending.sort_values("upload_date", ascending=False)
    return pending.reset_index(drop=True)


def merge_results(
    ft_battles: pd.DataFrame,
    results: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Left-join the results store onto ft_battles by battle key (analysis helper).

    Returns a new frame with the result columns added and does not mutate the
    input. Vote columns remain text (with 'NA' where not applicable); convert
    with pd.to_numeric for analysis.
    """
    if results is None:
        results = load_results()

    out = ft_battles.copy()
    out["battle_key"] = out["id"].map(battle_key)
    merged = out.merge(
        results.rename(columns={"id": "battle_key"}),
        on="battle_key",
        how="left",
    )
    return merged
