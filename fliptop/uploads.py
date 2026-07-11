"""
fliptop.uploads

Upload-side cleaning and filtering helpers.

This module owns Stage 1 of the pipeline: raw YouTube upload rows become a
candidate table of parseable 1v1 battle uploads. Event metadata attachment,
date imputation, consolidation, and publishing stay in ``fliptop.battles``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from pathlib import Path

import isodate
import pandas as pd

from .overrides import load_manual_matchups, load_upload_decisions
from .rename_map import load_rename_map
from .rules import compile_exclusion_pattern, load_title_exclusion_rules

PathLike = str | Path
RenameMap = Mapping[str, str]
ManualMatchupMap = Mapping[str, Mapping[str, str | None]]
UploadDecisionMap = Mapping[str, Mapping[str, str]]


def clean_titles(df: pd.DataFrame, title_col: str = "title") -> pd.DataFrame:
    """Trim whitespace and remove wrapping double quotes in the title column."""
    if title_col not in df:
        return df

    return df.assign(
        **{
            title_col: df[title_col]
            .astype("string")
            .str.strip()
            .str.replace(r'^"(.*)"$', r"\1", regex=True)
        }
    )


def parse_upload_date(
    df: pd.DataFrame,
    upload_date_col: str = "upload_date",
    new_col: str = "upload_date",
) -> pd.DataFrame:
    """Parse YouTube UTC timestamps into timezone-naive ``datetime64[ns]``."""
    if upload_date_col not in df:
        return df

    dt = pd.to_datetime(df[upload_date_col], utc=True, errors="coerce")
    dt = dt.dt.tz_convert(None)
    return df.assign(**{new_col: dt})


def add_duration_columns(
    df: pd.DataFrame,
    duration_col: str = "duration",
) -> pd.DataFrame:
    """Parse ISO-8601 durations into seconds and ``HH:MM:SS`` display strings."""
    if duration_col not in df:
        return df

    def to_seconds(x):
        if pd.isna(x):
            return pd.NA
        try:
            return isodate.parse_duration(x).total_seconds()
        except Exception:
            return pd.NA

    seconds = df[duration_col].map(to_seconds)
    seconds = pd.to_numeric(seconds, errors="coerce")
    hms = pd.to_datetime(seconds, unit="s", errors="coerce").dt.strftime("%H:%M:%S")
    hms = hms.where(seconds.notna(), other=pd.NA)

    return df.assign(
        duration_seconds=seconds,
        duration_hms=hms,
    )


def convert_video_metrics_to_numeric(
    df: pd.DataFrame,
    cols: Iterable[str] = ("view_count", "likeCount", "commentCount"),
) -> pd.DataFrame:
    """Convert view/like/comment count columns from strings to numeric."""
    target_cols = list(cols)
    present_cols = [col for col in target_cols if col in df.columns]
    if not present_cols:
        return df

    return df.assign(
        **{col: pd.to_numeric(df[col], errors="coerce") for col in present_cols}
    )


def filter_titles_with_vs(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows whose ``title`` contains the token ``vs``."""
    if "title" not in df:
        return df
    return df[df["title"].str.contains(r"\bvs\b", case=False, regex=True, na=False)]


def drop_non_battles(
    df: pd.DataFrame,
    exclusion_rules=None,
) -> pd.DataFrame:
    """Drop rows whose ``title`` matches active title exclusion rules."""
    if "title" not in df:
        return df
    if exclusion_rules is None:
        exclusion_rules = load_title_exclusion_rules()
    pattern = compile_exclusion_pattern(list(exclusion_rules))
    return df[~df["title"].str.contains(pattern, na=False)]


def keep_1v1(df: pd.DataFrame) -> pd.DataFrame:
    """Keep rows that look like 1v1 battles based on simple title heuristics."""
    if "title" not in df:
        return df

    is_str = df["title"].apply(lambda x: isinstance(x, str))
    s = df["title"].where(is_str, None).fillna("").str.lower()

    vs_count = s.str.count(r"\bvs\b")
    and_count = s.str.count(r"\band\b")
    has_slash = s.str.contains(r"/", na=False)
    has_plus = s.str.contains(r"\+", na=False)
    and_both = s.str.contains(r"\band\b.*\bvs\b.*\band\b", na=False)
    n_on_m = s.str.contains(r"\b\d+\s*on\s*\d+\b", na=False)

    not_1v1 = (
        (~is_str)
        | (vs_count > 1)
        | (and_count > 1)
        | has_slash
        | has_plus
        | and_both
        | n_on_m
    )

    return df.loc[~not_1v1]


def _resolved_manual_matchup_ids(
    manual_matchups: ManualMatchupMap | None,
) -> set[str]:
    if not manual_matchups:
        return set()
    return {
        str(battle_id)
        for battle_id, row in manual_matchups.items()
        if row.get("emcee1") and row.get("emcee2")
    }


def _pending_manual_matchup_ids(
    manual_matchups: ManualMatchupMap | None,
) -> set[str]:
    if not manual_matchups:
        return set()
    return {
        str(battle_id)
        for battle_id, row in manual_matchups.items()
        if not (row.get("emcee1") and row.get("emcee2"))
    }


def _manual_matchup_notes(manual_matchups: ManualMatchupMap | None) -> dict[str, object]:
    if not manual_matchups:
        return {}
    return {
        str(battle_id): row.get("note") or pd.NA
        for battle_id, row in manual_matchups.items()
    }


def _manual_matchup_audit_fields(
    manual_matchups: ManualMatchupMap | None,
    rename_map: RenameMap | None = None,
) -> pd.DataFrame:
    if not manual_matchups:
        return pd.DataFrame(
            columns=[
                "battle_key",
                "helper_emcee",
                "emcee1_status",
                "emcee2_status",
                "helper_status",
                "manual_note",
            ]
        )
    if rename_map is None:
        rename_map = load_rename_map()

    rows = []
    for battle_id, row in manual_matchups.items():
        helper = row.get("helper_emcee")
        if helper:
            helper = rename_map.get(str(helper).strip(), str(helper).strip())
        rows.append(
            {
                "battle_key": str(battle_id),
                "helper_emcee": helper or pd.NA,
                "emcee1_status": row.get("emcee1_status") or pd.NA,
                "emcee2_status": row.get("emcee2_status") or pd.NA,
                "helper_status": row.get("helper_status") or pd.NA,
                "manual_note": row.get("note") or pd.NA,
            }
        )
    return pd.DataFrame(rows)


def _upload_decision_ids(
    upload_decisions: UploadDecisionMap | None,
    decision: str,
) -> set[str]:
    if not upload_decisions:
        return set()
    return {
        str(upload_id)
        for upload_id, row in upload_decisions.items()
        if row.get("decision") == decision
    }


def _upload_decision_audit_fields(
    upload_decisions: UploadDecisionMap | None,
) -> pd.DataFrame:
    if not upload_decisions:
        return pd.DataFrame(
            columns=[
                "id",
                "upload_decision",
                "upload_decision_reason",
                "upload_decision_note",
            ]
        )

    rows = []
    for upload_id, row in upload_decisions.items():
        rows.append(
            {
                "id": str(upload_id),
                "upload_decision": row.get("decision") or pd.NA,
                "upload_decision_reason": row.get("reason") or pd.NA,
                "upload_decision_note": row.get("note") or pd.NA,
            }
        )
    return pd.DataFrame(rows)


def _apply_upload_decision_fields(
    df: pd.DataFrame,
    upload_decisions: UploadDecisionMap | None,
) -> pd.DataFrame:
    if df.empty or "id" not in df.columns or not upload_decisions:
        return df

    fields = _upload_decision_audit_fields(upload_decisions)
    if fields.empty:
        return df

    out = df.copy()
    if any(col in out.columns for col in fields.columns if col != "id"):
        out = out.drop(columns=[c for c in fields.columns if c != "id" and c in out.columns])
    out["id"] = out["id"].astype(str)
    return out.merge(fields, on="id", how="left")


def _hold_upload_decision_rows(
    df: pd.DataFrame,
    upload_decisions: UploadDecisionMap | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Remove exact exclude/review decisions from the flow and return their rows.

    Returns ``(kept, excluded, review)``. Exact ``include`` decisions do not exit
    here; they protect rows from broad filters later in the pipeline.
    """
    if "id" not in df.columns or not upload_decisions:
        empty = df.iloc[0:0].copy()
        return df, empty, empty

    ids = df["id"].astype(str)
    exclude_ids = _upload_decision_ids(upload_decisions, "exclude")
    review_ids = _upload_decision_ids(upload_decisions, "review")

    excluded = df.loc[ids.isin(exclude_ids)].copy()
    if not excluded.empty:
        excluded = _apply_upload_decision_fields(excluded, upload_decisions)
        excluded["pipeline_status"] = "excluded"
        excluded["stage"] = "upload_decision_override"
        excluded["excluded_reason"] = "manual upload decision"
        excluded["exit_category"] = excluded["upload_decision_reason"]

    review = df.loc[ids.isin(review_ids)].copy()
    if not review.empty:
        review = _apply_upload_decision_fields(review, upload_decisions)
        review["pipeline_status"] = "needs_upload_review"
        review["stage"] = "upload_decision_review"
        review["exit_category"] = review["upload_decision_reason"]

    held_ids = exclude_ids | review_ids
    kept = df.loc[~ids.isin(held_ids)].copy()
    return kept, excluded, review


def _keep_upload_decision_includes(
    before: pd.DataFrame,
    after: pd.DataFrame,
    upload_decisions: UploadDecisionMap | None,
) -> pd.DataFrame:
    """Add exact ``include`` ids back after a broad filter removes them."""
    if "id" not in before.columns or not upload_decisions:
        return after

    include_ids = _upload_decision_ids(upload_decisions, "include")
    if not include_ids:
        return after

    after_ids = set(after["id"].astype(str)) if "id" in after.columns else set()
    restore_ids = include_ids - after_ids
    if not restore_ids:
        return after

    restored = before.loc[before["id"].astype(str).isin(restore_ids)].copy()
    if restored.empty:
        return after
    return pd.concat([after, restored], ignore_index=True)


def keep_1v1_or_manual_matchup(
    df: pd.DataFrame,
    manual_matchups: ManualMatchupMap | None = None,
) -> pd.DataFrame:
    """Keep normal 1v1-looking titles plus explicitly resolved manual matchups."""
    kept = keep_1v1(df)
    resolved_ids = _resolved_manual_matchup_ids(manual_matchups)
    if not resolved_ids or "id" not in df.columns:
        return kept

    already_kept = set(kept["id"].astype(str)) if "id" in kept.columns else set()
    manual = df[
        df["id"].astype(str).isin(resolved_ids - already_kept)
    ]
    if manual.empty:
        return kept
    return pd.concat([kept, manual], ignore_index=True)


def copy_yt_title(df: pd.DataFrame) -> pd.DataFrame:
    """Preserve the cleaned YouTube title, including any ``pt. N`` suffix."""
    if "title" not in df:
        return df
    return df.assign(yt_raw_title=df["title"])


_PT_RE = re.compile(r"\s*pt\.?\s*(\d+)$", flags=re.IGNORECASE)
_PT_SUFFIX = re.compile(r"\s*pt\.?\s*\d+$", flags=re.IGNORECASE)


def _base_title(s):
    """Strip trailing ``pt. N`` from a working title."""
    if not isinstance(s, str):
        return s
    return _PT_SUFFIX.sub("", s.strip()).strip()


def strip_pt_suffix_from_title(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with trailing ``pt. N`` removed from ``title``."""
    if "title" not in df:
        return df
    return df.assign(title=df["title"].map(_base_title))


def _base_raw_title(s: str) -> str:
    """Strip trailing ``pt. N`` from a raw title."""
    cleaned = _base_title(s)
    return "" if not isinstance(cleaned, str) else cleaned


def _part_num(s: str):
    """Extract the part number N from ``... pt. N``, or ``None``."""
    if not isinstance(s, str):
        return None
    m = _PT_RE.search(s)
    return int(m.group(1)) if m else None


_VS_SPLIT = re.compile(r"\s+vs\s+", flags=re.IGNORECASE)
_PREFIX = re.compile(r"^FlipTop(?: [^–-]+)?\s*[-–]\s*", flags=re.IGNORECASE)
_POST2 = re.compile(r"\s*[@|(*].*$")
_TRAIL_NUM = re.compile(r"\s+\d+$")


def extract_matchup_from_title(title: str) -> str | None:
    """Extract a clean ``Emcee A vs Emcee B`` string from a FlipTop title."""
    if not isinstance(title, str):
        return None

    t = title.strip()
    t = _PREFIX.sub("", t)

    parts = _VS_SPLIT.split(t, maxsplit=1)
    if len(parts) != 2:
        return None

    emcee1 = parts[0].strip()
    emcee2 = _POST2.sub("", parts[1].strip())
    emcee2 = _TRAIL_NUM.sub("", emcee2).strip()

    if not emcee1 or not emcee2:
        return None

    return f"{emcee1} vs {emcee2}"


def add_matchup_and_split(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``matchup``, ``emcee1``, and ``emcee2`` inferred from ``title``."""
    if "title" not in df:
        return df

    out = df.copy()
    out["matchup"] = out["title"].map(extract_matchup_from_title)
    out["matchup"] = out["matchup"].str.replace(r"\s-\s.*$", "", regex=True)

    split = out["matchup"].str.split(_VS_SPLIT, n=1, expand=True)
    out[["emcee1", "emcee2"]] = split

    out["emcee1"] = out["emcee1"].str.strip()
    out["emcee2"] = out["emcee2"].str.strip()

    return out


def apply_manual_matchup_overrides(
    df: pd.DataFrame,
    manual_matchups: ManualMatchupMap | None = None,
) -> pd.DataFrame:
    """Override matchup fields for resolved manual rows."""
    if not manual_matchups or "id" not in df.columns:
        return df

    out = df.copy()
    ids = out["id"].astype(str)
    for battle_id, row in manual_matchups.items():
        emcee1 = row.get("emcee1")
        emcee2 = row.get("emcee2")
        if not emcee1 or not emcee2:
            continue

        mask = ids == str(battle_id)
        if not bool(mask.any()):
            continue
        out.loc[mask, "matchup"] = f"{emcee1} vs {emcee2}"
        out.loc[mask, "emcee1"] = emcee1
        out.loc[mask, "emcee2"] = emcee2
    return out


def apply_emcee_rename(
    df: pd.DataFrame,
    rename_map: RenameMap | None = None,
) -> pd.DataFrame:
    """Canonicalize ``emcee1`` and ``emcee2`` using the alias map."""
    if rename_map is None:
        return df

    if not {"emcee1", "emcee2"} <= set(df.columns):
        return df

    out = df.copy()
    out["emcee1"] = out["emcee1"].astype("string").str.strip().replace(rename_map)
    out["emcee2"] = out["emcee2"].astype("string").str.strip().replace(rename_map)
    return out


def add_matchup_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Build ``matchup_clean`` from canonicalized emcee columns."""
    if not {"emcee1", "emcee2"} <= set(df.columns):
        return df

    return df.assign(
        matchup_clean=(
            df["emcee1"].astype("string").str.strip()
            + " vs "
            + df["emcee2"].astype("string").str.strip()
        )
    )


def prepare_uploads(df_yt: pd.DataFrame) -> pd.DataFrame:
    """Apply pre-filter transforms shared by the build and audit traces."""
    return (
        df_yt
        .pipe(clean_titles)
        .pipe(parse_upload_date)
        .pipe(add_duration_columns)
        .pipe(convert_video_metrics_to_numeric)
        .pipe(copy_yt_title)
        .pipe(strip_pt_suffix_from_title)
    )


def make_df_1v1_uploads(
    df_yt: pd.DataFrame,
    rename_map: RenameMap | None = None,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
    title_exclusion_rules=None,
) -> pd.DataFrame:
    """Build a clean table of 1v1 battle uploads from raw YouTube uploads."""
    if rename_map is None:
        rename_map = load_rename_map()
    if manual_matchups is None:
        manual_matchups = load_manual_matchups()
    if upload_decisions is None:
        upload_decisions = load_upload_decisions()

    df = prepare_uploads(df_yt)
    df, _, _ = _hold_upload_decision_rows(df, upload_decisions)
    df = _keep_upload_decision_includes(
        df,
        filter_titles_with_vs(df),
        upload_decisions,
    )
    df = _keep_upload_decision_includes(
        df,
        drop_non_battles(df, exclusion_rules=title_exclusion_rules),
        upload_decisions,
    )
    df = _keep_upload_decision_includes(
        df,
        keep_1v1_or_manual_matchup(df, manual_matchups=manual_matchups),
        upload_decisions,
    )
    df = (
        df.pipe(add_matchup_and_split)
        .pipe(apply_manual_matchup_overrides, manual_matchups=manual_matchups)
        .pipe(apply_emcee_rename, rename_map=rename_map)
        .pipe(add_matchup_clean)
    )

    if "upload_date" in df.columns:
        df = df.sort_values("upload_date").reset_index(drop=True)

    return df
