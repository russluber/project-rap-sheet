"""Current YouTube engagement metrics and battle-level analysis helpers.

``data/raw/youtube_video_metrics.csv`` is a current-state table: one row per
known YouTube upload. Successful refreshes overwrite the stored counts while
retaining an observation timestamp. The table deliberately stays outside
``PipelineInputs`` so changing telemetry does not invalidate the stable
``ft_battles`` release.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from . import RAW_DATA_DIR
from .annotations import battle_key, extract_video_id
from .contracts import (
    YOUTUBE_VIDEO_METRICS,
    YOUTUBE_VIDEO_METRICS_COLUMNS,
    ContractViolation,
)
from .io import atomic_output_path

PathLike = str | Path

DEFAULT_METRICS_PATH = RAW_DATA_DIR / "youtube_video_metrics.csv"
FETCHED_METRIC_COLUMNS = ("video_id", "view_count", "like_count", "comment_count")
ATTACHED_METRIC_COLUMNS = (
    "youtube_view_count",
    "youtube_like_count",
    "youtube_comment_count",
    "youtube_metrics_observed_at",
    "youtube_video_count",
    "youtube_metrics_complete",
)


def _coerce_metric_dtypes(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with stable nullable count and UTC timestamp dtypes."""
    work = frame.copy()
    for column in ("view_count", "like_count", "comment_count"):
        work[column] = pd.to_numeric(work[column], errors="coerce").astype("Int64")
    for column in ("observed_at", "checked_at"):
        work[column] = pd.to_datetime(work[column], errors="coerce", utc=True)
    work["video_id"] = work["video_id"].astype("string")
    work["fetch_status"] = work["fetch_status"].astype("string")
    return work.loc[:, list(YOUTUBE_VIDEO_METRICS_COLUMNS)]


def validate_youtube_video_metrics(frame: pd.DataFrame) -> list[str]:
    """Return structural and semantic problems with a metrics table."""
    problems = YOUTUBE_VIDEO_METRICS.problems(frame)
    if any(column not in frame.columns for column in YOUTUBE_VIDEO_METRICS_COLUMNS):
        return problems

    work = frame.copy()
    counts: dict[str, pd.Series] = {}
    for column in ("view_count", "like_count", "comment_count"):
        numeric = pd.to_numeric(work[column], errors="coerce")
        counts[column] = numeric
        present = numeric.notna()
        negative = present & numeric.lt(0)
        fractional = present & numeric.mod(1).ne(0)
        if bool(negative.any()):
            problems.append(f"{column} has {int(negative.sum())} negative value(s)")
        if bool(fractional.any()):
            problems.append(f"{column} has {int(fractional.sum())} non-integer value(s)")

    observed = pd.to_datetime(work["observed_at"], errors="coerce", utc=True)
    checked = pd.to_datetime(work["checked_at"], errors="coerce", utc=True)
    status = work["fetch_status"].astype("string").str.strip()
    ok = status.eq("ok")

    missing_ok_views = ok & counts["view_count"].isna()
    if bool(missing_ok_views.any()):
        problems.append(
            f"{int(missing_ok_views.sum())} ok row(s) are missing view_count"
        )
    missing_ok_observed = ok & observed.isna()
    if bool(missing_ok_observed.any()):
        problems.append(
            f"{int(missing_ok_observed.sum())} ok row(s) are missing observed_at"
        )
    after_check = observed.notna() & checked.notna() & observed.gt(checked)
    if bool(after_check.any()):
        problems.append(
            f"{int(after_check.sum())} row(s) have observed_at after checked_at"
        )
    return problems


def require_youtube_video_metrics(
    frame: pd.DataFrame,
    *,
    source: PathLike | None = None,
) -> pd.DataFrame:
    """Validate and return a consistently typed metrics table."""
    problems = validate_youtube_video_metrics(frame)
    if problems:
        raise ContractViolation(YOUTUBE_VIDEO_METRICS.name, problems, source=source)
    return _coerce_metric_dtypes(frame)


def load_youtube_video_metrics(
    path: PathLike = DEFAULT_METRICS_PATH,
) -> pd.DataFrame:
    """Load and validate the current one-row-per-video metrics store."""
    path = Path(path)
    frame = pd.read_csv(
        path,
        dtype={"video_id": "string", "fetch_status": "string"},
    )
    return require_youtube_video_metrics(frame, source=path)


def save_youtube_video_metrics(
    frame: pd.DataFrame,
    path: PathLike = DEFAULT_METRICS_PATH,
) -> Path:
    """Atomically validate and replace the current metrics CSV."""
    path = Path(path)
    work = require_youtube_video_metrics(frame, source=path)
    work = work.sort_values("video_id", kind="stable").reset_index(drop=True)
    with atomic_output_path(path) as temporary:
        work.to_csv(
            temporary,
            index=False,
            date_format="%Y-%m-%dT%H:%M:%SZ",
        )
    return path


def empty_youtube_video_metrics() -> pd.DataFrame:
    """Return an empty, typed frame suitable as the first upsert input."""
    frame = pd.DataFrame(columns=YOUTUBE_VIDEO_METRICS_COLUMNS)
    for column in ("view_count", "like_count", "comment_count"):
        frame[column] = frame[column].astype("Int64")
    for column in ("observed_at", "checked_at"):
        frame[column] = pd.to_datetime(frame[column], utc=True)
    frame["video_id"] = frame["video_id"].astype("string")
    frame["fetch_status"] = frame["fetch_status"].astype("string")
    return frame


def merge_youtube_metric_refresh(
    existing: pd.DataFrame | None,
    fetched: pd.DataFrame,
    requested_ids: Iterable[str],
    *,
    checked_at,
) -> pd.DataFrame:
    """Merge one complete API check into the latest-value metrics store.

    Returned videos replace their previous counts, even when a count decreased.
    Requested IDs omitted by the API retain their last successful counts and
    ``observed_at`` while ``checked_at`` advances and ``fetch_status`` becomes
    ``not_returned``.
    """
    requested = [str(video_id).strip() for video_id in requested_ids]
    if any(not video_id for video_id in requested):
        raise ValueError("requested video IDs must be nonblank")
    if len(requested) != len(set(requested)):
        raise ValueError("requested video IDs must be unique")

    checked = pd.Timestamp(checked_at)
    checked = (
        checked.tz_localize("UTC")
        if checked.tzinfo is None
        else checked.tz_convert("UTC")
    )

    missing_fetched_columns = [
        column for column in FETCHED_METRIC_COLUMNS if column not in fetched.columns
    ]
    if missing_fetched_columns:
        raise ValueError(
            "fetched metrics are missing column(s): "
            + ", ".join(missing_fetched_columns)
        )
    fresh = fetched.loc[:, list(FETCHED_METRIC_COLUMNS)].copy()
    fresh["video_id"] = fresh["video_id"].astype("string").str.strip()
    if bool(fresh["video_id"].isna().any() | fresh["video_id"].eq("").any()):
        raise ValueError("fetched metrics contain a blank video_id")
    if bool(fresh["video_id"].duplicated().any()):
        raise ValueError("fetched metrics contain duplicate video IDs")
    unexpected = sorted(set(fresh["video_id"].astype(str)) - set(requested))
    if unexpected:
        raise ValueError(
            "API returned unrequested video ID(s): " + ", ".join(unexpected[:5])
        )
    for column in ("view_count", "like_count", "comment_count"):
        fresh[column] = pd.to_numeric(fresh[column], errors="coerce").astype("Int64")
    fresh_by_id = fresh.set_index("video_id")

    if existing is None or existing.empty:
        prior = empty_youtube_video_metrics().set_index("video_id")
    else:
        prior_work = require_youtube_video_metrics(existing)
        orphaned = sorted(set(prior_work["video_id"].astype(str)) - set(requested))
        if orphaned:
            raise ValueError(
                "existing metrics contain ID(s) absent from the upload inventory: "
                + ", ".join(orphaned[:5])
            )
        prior = prior_work.set_index("video_id")

    rows: list[dict[str, object]] = []
    for video_id in requested:
        if video_id in fresh_by_id.index:
            row = fresh_by_id.loc[video_id]
            rows.append(
                {
                    "video_id": video_id,
                    "view_count": row["view_count"],
                    "like_count": row["like_count"],
                    "comment_count": row["comment_count"],
                    "observed_at": checked,
                    "checked_at": checked,
                    "fetch_status": "ok",
                }
            )
            continue

        old = prior.loc[video_id] if video_id in prior.index else None
        rows.append(
            {
                "video_id": video_id,
                "view_count": pd.NA if old is None else old["view_count"],
                "like_count": pd.NA if old is None else old["like_count"],
                "comment_count": pd.NA if old is None else old["comment_count"],
                "observed_at": pd.NaT if old is None else old["observed_at"],
                "checked_at": checked,
                "fetch_status": "not_returned",
            }
        )

    result = pd.DataFrame(rows, columns=YOUTUBE_VIDEO_METRICS_COLUMNS)
    return require_youtube_video_metrics(result)


def build_battle_video_map(ft_battles: pd.DataFrame) -> pd.DataFrame:
    """Return one ordered ``battle_id, video_id, part_number`` row per upload."""
    if "id" not in ft_battles.columns:
        raise ValueError("battles table is missing required column: id")

    records: list[dict[str, object]] = []
    for _, row in ft_battles.iterrows():
        raw_id = row["id"]
        battle_id = battle_key(raw_id)
        if battle_id is None or not str(battle_id).strip():
            raise ValueError("battles table contains a row with no usable id")

        if isinstance(raw_id, list):
            video_ids = [str(value).strip() for value in raw_id if str(value).strip()]
        elif isinstance(row.get("url"), list):
            video_ids = []
            for url in row["url"]:
                video_id = extract_video_id(url)
                if video_id is None:
                    raise ValueError(
                        f"cannot extract a YouTube video ID from multipart URL: {url!r}"
                    )
                video_ids.append(video_id)
        else:
            video_ids = [str(battle_id)]

        if not video_ids:
            raise ValueError(f"battle {battle_id!r} has no video IDs")
        if video_ids[0] != str(battle_id):
            raise ValueError(
                f"battle {battle_id!r} does not match first video ID {video_ids[0]!r}"
            )
        if len(video_ids) != len(set(video_ids)):
            raise ValueError(f"battle {battle_id!r} contains duplicate video IDs")

        records.extend(
            {
                "battle_id": str(battle_id),
                "video_id": video_id,
                "part_number": part_number,
            }
            for part_number, video_id in enumerate(video_ids, start=1)
        )

    mapping = pd.DataFrame(
        records,
        columns=["battle_id", "video_id", "part_number"],
    )
    if bool(mapping["video_id"].duplicated().any()):
        duplicates = mapping.loc[mapping["video_id"].duplicated(), "video_id"].tolist()
        raise ValueError(
            "YouTube video ID(s) map to multiple battles: "
            + ", ".join(map(str, duplicates[:5]))
        )
    return mapping


def _sum_complete(group: pd.DataFrame, column: str):
    usable = group["fetch_status"].eq("ok") & group[column].notna()
    if not bool(usable.all()):
        return pd.NA
    return int(group[column].sum())


def attach_youtube_metrics(
    ft_battles: pd.DataFrame,
    metrics: pd.DataFrame | None = None,
    *,
    metrics_path: PathLike = DEFAULT_METRICS_PATH,
) -> pd.DataFrame:
    """Attach current YouTube totals without changing the stable battle schema.

    Multipart battles sum each metric across their ordered video parts. A total
    is missing rather than partial when any part lacks a current usable value.
    """
    work_metrics = (
        load_youtube_video_metrics(metrics_path)
        if metrics is None
        else require_youtube_video_metrics(metrics)
    )
    mapping = build_battle_video_map(ft_battles)
    joined = mapping.merge(
        work_metrics,
        on="video_id",
        how="left",
        validate="many_to_one",
    )

    aggregates: list[dict[str, object]] = []
    for battle_id, group in joined.groupby("battle_id", sort=False):
        complete = bool(
            (
                group["fetch_status"].eq("ok")
                & group["view_count"].notna()
                & group["observed_at"].notna()
            ).all()
        )
        aggregates.append(
            {
                "battle_id": battle_id,
                "youtube_view_count": _sum_complete(group, "view_count"),
                "youtube_like_count": _sum_complete(group, "like_count"),
                "youtube_comment_count": _sum_complete(group, "comment_count"),
                "youtube_metrics_observed_at": (
                    group["observed_at"].min() if complete else pd.NaT
                ),
                "youtube_video_count": len(group),
                "youtube_metrics_complete": complete,
            }
        )
    aggregate_frame = pd.DataFrame(
        aggregates,
        columns=["battle_id", *ATTACHED_METRIC_COLUMNS],
    ).set_index("battle_id")

    out = ft_battles.copy()
    keys = out["id"].map(battle_key).astype("string")
    if bool(keys.duplicated().any()):
        raise ValueError("battles table contains duplicate battle IDs")
    for column in ATTACHED_METRIC_COLUMNS:
        out[column] = keys.map(aggregate_frame[column])
    for column in (
        "youtube_view_count",
        "youtube_like_count",
        "youtube_comment_count",
        "youtube_video_count",
    ):
        out[column] = pd.to_numeric(out[column], errors="coerce").astype("Int64")
    out["youtube_metrics_observed_at"] = pd.to_datetime(
        out["youtube_metrics_observed_at"],
        errors="coerce",
        utc=True,
    )
    out["youtube_metrics_complete"] = out["youtube_metrics_complete"].astype("boolean")
    return out
