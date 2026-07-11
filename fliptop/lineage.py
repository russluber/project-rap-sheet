"""
fliptop.lineage

Audit helpers for the FlipTop wrangling pipeline.

This module owns the explainability surfaces for the raw-to-output pipeline:
which YouTube uploads were included, which were excluded, which were held for
manual review, and where row counts change across the pipeline.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pandas as pd

from .battles import (
    build_battle_metadata,
    load_event_metadata,
    load_youtube_uploads,
)
from .events import (
    EVENT_EXCLUSION_RE,
    EVENT_EXCLUSION_RULES,
    attach_event_metadata,
    drop_excluded_events,
)
from .io import atomic_output_path
from .overrides import load_manual_matchups, load_upload_decisions
from .publish import build_ft_battles_from_metadata
from .rules import first_matching_rule
from .uploads import (
    TITLE_EXCLUSION_RULES,
    ManualMatchupMap,
    PathLike,
    RenameMap,
    UploadDecisionMap,
    _hold_upload_decision_rows,
    _keep_upload_decision_includes,
    _manual_matchup_audit_fields,
    _manual_matchup_notes,
    _part_num,
    _pending_manual_matchup_ids,
    _upload_decision_audit_fields,
    drop_non_battles,
    filter_titles_with_vs,
    keep_1v1_or_manual_matchup,
    prepare_uploads,
)


def _event_name_lookup(df_events: pd.DataFrame) -> pd.DataFrame:
    """Return a small ``id -> event_name`` lookup from raw event metadata."""
    event_key = "video_id" if "video_id" in df_events.columns else "id"
    if event_key not in df_events.columns or "event_name" not in df_events.columns:
        return pd.DataFrame(columns=["id", "_event_name_lookup"])
    return (
        df_events[[event_key, "event_name"]]
        .drop_duplicates(subset=[event_key])
        .rename(columns={event_key: "id", "event_name": "_event_name_lookup"})
    )


def _rule_audit_fields(row) -> dict[str, object]:
    """Structured rule metadata responsible for a row exit, if any."""
    if row["excluded_reason"] == "non-battle keyword":
        match = first_matching_rule(row.get("title"), TITLE_EXCLUSION_RULES)
    elif row["excluded_reason"] == "excluded event":
        match = first_matching_rule(row.get("event_name"), EVENT_EXCLUSION_RULES)
    else:
        match = None

    if match is None:
        return {
            "matched_keyword": pd.NA,
            "rule_id": pd.NA,
            "rule_note": pd.NA,
            "exit_category": row.get("exit_category", pd.NA),
        }

    rule, matched_keyword = match
    return {
        "matched_keyword": matched_keyword,
        "rule_id": rule.rule_id,
        "rule_note": rule.note,
        "exit_category": rule.exit_category,
    }


def _upload_stage_trace(
    df_yt: pd.DataFrame,
    df_events: pd.DataFrame,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, pd.DataFrame]:
    """
    Run the upload filters once and return stage frames plus row exits.

    ``excluded`` records the first filter that removed each upload. ``pending``
    records known battle uploads held for manual matchup resolution. This is
    the shared spine for the exclusion audit, lineage audit, and stage summary.
    """
    if manual_matchups is None:
        manual_matchups = load_manual_matchups()
    if upload_decisions is None:
        upload_decisions = load_upload_decisions()

    pre = prepare_uploads(df_yt)
    after_decisions, decision_excluded, needs_upload_review = _hold_upload_decision_rows(
        pre,
        upload_decisions,
    )

    def _dropped(
        before: pd.DataFrame,
        after: pd.DataFrame,
        reason: str,
        stage: str,
        exit_category: str,
    ) -> pd.DataFrame:
        out = before[~before["id"].isin(after["id"])].copy()
        out["pipeline_status"] = "excluded"
        out["stage"] = stage
        out["excluded_reason"] = reason
        out["exit_category"] = exit_category
        return out

    after_vs = _keep_upload_decision_includes(
        after_decisions,
        filter_titles_with_vs(after_decisions),
        upload_decisions,
    )
    after_nonbattle = _keep_upload_decision_includes(
        after_vs,
        drop_non_battles(after_vs),
        upload_decisions,
    )

    pending_ids = _pending_manual_matchup_ids(manual_matchups)
    is_pending_manual = (
        after_nonbattle["id"].astype(str).isin(pending_ids)
        if "id" in after_nonbattle.columns
        else pd.Series(False, index=after_nonbattle.index)
    )
    pending_manual = after_nonbattle.loc[is_pending_manual].copy()
    if not pending_manual.empty:
        event_lookup = _event_name_lookup(df_events)
        if not event_lookup.empty:
            pending_manual = pending_manual.merge(event_lookup, on="id", how="left")
            pending_manual["event_name"] = pending_manual["_event_name_lookup"]
            pending_manual = pending_manual.drop(columns=["_event_name_lookup"])

    if "event_name" in pending_manual.columns:
        pending_excluded_event = pending_manual["event_name"].astype("string").str.contains(
            EVENT_EXCLUSION_RE,
            na=False,
        )
    else:
        pending_excluded_event = pd.Series(False, index=pending_manual.index)

    needs_manual = pending_manual.loc[~pending_excluded_event].copy()
    if not needs_manual.empty:
        needs_manual["pipeline_status"] = "needs_manual_matchup"
        needs_manual["stage"] = "manual_matchup_override"
        needs_manual["exit_category"] = "manual_review_required"
        needs_manual["manual_note"] = needs_manual["id"].astype(str).map(
            _manual_matchup_notes(manual_matchups)
        )
    pending_event_excluded = pending_manual.loc[pending_excluded_event].copy()
    if not pending_event_excluded.empty:
        pending_event_excluded["pipeline_status"] = "excluded"
        pending_event_excluded["stage"] = "drop_excluded_events"
        pending_event_excluded["excluded_reason"] = "excluded event"
        pending_event_excluded["exit_category"] = "out_of_scope_event"
        pending_event_excluded["manual_note"] = pending_event_excluded["id"].astype(str).map(
            _manual_matchup_notes(manual_matchups)
        )

    not_pending = after_nonbattle.loc[~is_pending_manual].copy()
    after_1v1 = _keep_upload_decision_includes(
        not_pending,
        keep_1v1_or_manual_matchup(
            not_pending,
            manual_matchups=manual_matchups,
        ),
        upload_decisions,
    )
    with_event_meta = attach_event_metadata(after_1v1, df_events)
    after_event_filter = _keep_upload_decision_includes(
        with_event_meta,
        drop_excluded_events(with_event_meta),
        upload_decisions,
    )

    excluded = pd.concat(
        [
            decision_excluded,
            _dropped(
                after_decisions,
                after_vs,
                "no 'vs' token",
                "filter_titles_with_vs",
                "not_battle",
            ),
            _dropped(
                after_vs,
                after_nonbattle,
                "non-battle keyword",
                "drop_non_battles",
                pd.NA,
            ),
            _dropped(
                not_pending,
                after_1v1,
                "not 1v1",
                "keep_1v1",
                "format_not_supported",
            ),
            _dropped(
                with_event_meta,
                after_event_filter,
                "excluded event",
                "drop_excluded_events",
                "out_of_scope_event",
            ),
            pending_event_excluded,
        ],
        ignore_index=True,
    )

    # Add event names to early-stage drops too, without changing which filter
    # receives credit for excluding them.
    event_lookup = _event_name_lookup(df_events)
    if not event_lookup.empty:
        excluded = excluded.merge(event_lookup, on="id", how="left")
        if "event_name" in excluded.columns:
            excluded["event_name"] = excluded["event_name"].fillna(
                excluded["_event_name_lookup"]
            )
        else:
            excluded["event_name"] = excluded["_event_name_lookup"]
        excluded = excluded.drop(columns=["_event_name_lookup"])

        if not needs_manual.empty:
            needs_manual = needs_manual.merge(event_lookup, on="id", how="left")
            if "event_name" in needs_manual.columns:
                needs_manual["event_name"] = needs_manual["event_name"].fillna(
                    needs_manual["_event_name_lookup"]
                )
            else:
                needs_manual["event_name"] = needs_manual["_event_name_lookup"]
            needs_manual = needs_manual.drop(columns=["_event_name_lookup"])

        if not needs_upload_review.empty:
            needs_upload_review = needs_upload_review.merge(event_lookup, on="id", how="left")
            if "event_name" in needs_upload_review.columns:
                needs_upload_review["event_name"] = needs_upload_review[
                    "event_name"
                ].fillna(needs_upload_review["_event_name_lookup"])
            else:
                needs_upload_review["event_name"] = needs_upload_review[
                    "_event_name_lookup"
                ]
            needs_upload_review = needs_upload_review.drop(columns=["_event_name_lookup"])

    needs_review = pd.concat([needs_upload_review, needs_manual], ignore_index=True)

    rule_cols = ["matched_keyword", "rule_id", "rule_note", "exit_category"]
    if excluded.empty:
        for col in rule_cols:
            excluded[col] = pd.Series(dtype="object")
    else:
        rule_fields = excluded.apply(_rule_audit_fields, axis=1, result_type="expand")
        for col in rule_cols:
            if col not in excluded.columns:
                excluded[col] = pd.NA
            if col in rule_fields.columns:
                excluded[col] = excluded[col].combine_first(rule_fields[col])
    trace = {
        "raw_youtube": df_yt.copy(),
        "prepare_uploads": pre,
        "apply_upload_decisions": after_decisions,
        "filter_titles_with_vs": after_vs,
        "drop_non_battles": after_nonbattle,
        "manual_matchup_review_split": not_pending,
        "keep_1v1_or_manual_matchup": after_1v1,
        "attach_event_metadata": with_event_meta,
        "drop_excluded_events": after_event_filter,
    }
    return trace, excluded, needs_review


def _filter_upload_stages(
    df_yt: pd.DataFrame,
    df_events: pd.DataFrame,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Run the upload filters once and return ``(prepared, kept, excluded, pending)``.

    ``excluded`` records the first stage that removed each upload. This is the
    compatibility API shared by the older audit helpers.
    """
    trace, excluded, needs_manual = _upload_stage_trace(
        df_yt,
        df_events,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )
    return (
        trace["prepare_uploads"],
        trace["drop_excluded_events"],
        excluded,
        needs_manual,
    )


def build_excluded_uploads(
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
) -> pd.DataFrame:
    """
    Return the raw uploads that the pipeline drops, tagged with the reason.

    Audit helper: lets you eyeball everything the pipeline excludes so real
    battles are not silently filtered out. It reruns the title/format filters
    in the same order, then attaches event metadata and applies the event-name
    exclusions, recording the first stage that removed each video:

        - "no 'vs' token"      (filter_titles_with_vs)
        - "non-battle keyword" (drop_non_battles; rule metadata is recorded)
        - "not 1v1"            (keep_1v1)
        - "excluded event"     (drop_excluded_events; event keyword recorded)
        - "manual upload decision" (exact exclude row in upload_decisions.csv)

    Returns
    -------
    pd.DataFrame
        One row per excluded upload, with id, both titles, upload_date, url,
        `excluded_reason`, `exit_category`, and rule metadata where applicable.
    """
    raw_dir = Path(raw_dir)
    df_yt = load_youtube_uploads(raw_dir / youtube_json_name)
    df_events = load_event_metadata(raw_dir / events_csv_name)
    _, _, excluded, _ = _filter_upload_stages(
        df_yt,
        df_events,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )

    cols = [
        "id",
        "yt_raw_title",
        "title",
        "event_name",
        "upload_date",
        "url",
        "excluded_reason",
        "exit_category",
        "matched_keyword",
        "rule_id",
        "rule_note",
        "upload_decision",
        "upload_decision_reason",
        "upload_decision_note",
    ]
    excluded = excluded[[c for c in cols if c in excluded.columns]]
    if "upload_date" in excluded.columns:
        excluded = excluded.sort_values("upload_date").reset_index(drop=True)
    return excluded


UPLOAD_LINEAGE_COLUMNS = [
    "id",
    "yt_raw_title",
    "title",
    "upload_date",
    "url",
    "pipeline_status",
    "stage",
    "excluded_reason",
    "exit_category",
    "matched_keyword",
    "rule_id",
    "rule_note",
    "upload_decision",
    "upload_decision_reason",
    "upload_decision_note",
    "manual_note",
    "event_name",
    "event_date",
    "event_date_source",
    "battle_key",
    "final_title",
    "final_matchup",
    "emcee1",
    "emcee2",
    "helper_emcee",
    "emcee1_status",
    "emcee2_status",
    "helper_status",
    "source_part_number",
    "annotation_status",
    "battle_type",
    "winner",
    "votes_winner",
    "votes_loser",
]

PIPELINE_SUMMARY_COLUMNS = [
    "stage_order",
    "stage",
    "input_rows",
    "output_rows",
    "delta_rows",
    "exit_rows",
    "exit_status",
    "note",
]

PIPELINE_STAGE_DROP_COLUMNS = [
    "stage_order",
    "id",
    "pipeline_status",
    "stage",
    "excluded_reason",
    "exit_category",
    "matched_keyword",
    "rule_id",
    "rule_note",
    "upload_decision",
    "upload_decision_reason",
    "upload_decision_note",
    "manual_note",
    "yt_raw_title",
    "title",
    "event_name",
    "upload_date",
    "url",
]


def build_upload_lineage(
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    versetracker_csv_name: str = "versetracker_event_dates.csv",
    rename_map: RenameMap | None = None,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
    vt_event_dates: Mapping[str, pd.Timestamp] | None = None,
    battle_metadata: pd.DataFrame | None = None,
    results: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Build a one-row-per-YouTube-upload audit table for the wrangling pipeline.

    The lineage table answers "what happened to every raw upload?" Each raw
    ``youtube_videos.json`` row is tagged as:

    - ``excluded``: removed by the first recorded filter stage;
    - ``needs_manual_matchup``: explicitly known battle whose title needs a
      hand-entered 1v1 matchup before it can enter the final output;
    - ``included``: source upload is the published battle key;
    - ``consolidated_part``: source upload was folded into a multi-part battle
      whose key is another source id.

    For included rows it also records the final battle key, canonical matchup,
    event/date provenance, and annotation status. This is an audit surface only;
    it does not change the published ``ft_battles`` build.
    """
    from .annotations import battle_key, load_results

    raw_dir = Path(raw_dir)
    df_yt = load_youtube_uploads(raw_dir / youtube_json_name)
    df_events = load_event_metadata(raw_dir / events_csv_name)
    if manual_matchups is None:
        manual_matchups = load_manual_matchups()
    if upload_decisions is None:
        upload_decisions = load_upload_decisions()
    prepared, _kept, excluded, needs_manual = _filter_upload_stages(
        df_yt,
        df_events,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )

    base_cols = ["id", "yt_raw_title", "title", "upload_date", "url"]
    lineage = prepared[[c for c in base_cols if c in prepared.columns]].copy()
    lineage["id"] = lineage["id"].astype(str)

    event_lookup = _event_name_lookup(df_events)
    if not event_lookup.empty:
        lineage = lineage.merge(event_lookup, on="id", how="left")
        lineage["event_name"] = lineage["_event_name_lookup"]
        lineage = lineage.drop(columns=["_event_name_lookup"])

    for col in UPLOAD_LINEAGE_COLUMNS:
        if col not in lineage.columns:
            lineage[col] = pd.NA

    lineage["source_part_number"] = lineage["yt_raw_title"].map(_part_num)

    upload_decision_fields = _upload_decision_audit_fields(upload_decisions)
    if not upload_decision_fields.empty:
        decision_lookup = upload_decision_fields.set_index("id")
        is_decision = lineage["id"].isin(decision_lookup.index)
        for col in [
            "upload_decision",
            "upload_decision_reason",
            "upload_decision_note",
        ]:
            lineage.loc[is_decision, col] = lineage.loc[is_decision, "id"].map(
                decision_lookup[col]
            )

    if not excluded.empty:
        excluded_lookup = excluded.drop_duplicates(subset=["id"]).copy()
        excluded_lookup["id"] = excluded_lookup["id"].astype(str)
        excluded_lookup = excluded_lookup.set_index("id")
        is_excluded = lineage["id"].isin(excluded_lookup.index)
        for col in [
            "pipeline_status",
            "stage",
            "excluded_reason",
            "exit_category",
            "matched_keyword",
            "rule_id",
            "rule_note",
            "upload_decision",
            "upload_decision_reason",
            "upload_decision_note",
            "manual_note",
        ]:
            if col not in excluded_lookup.columns:
                continue
            lineage.loc[is_excluded, col] = lineage.loc[is_excluded, "id"].map(
                excluded_lookup[col]
            )
        if "event_name" in excluded_lookup.columns:
            lineage.loc[is_excluded, "event_name"] = lineage.loc[is_excluded, "id"].map(
                excluded_lookup["event_name"]
            )

    if not needs_manual.empty:
        manual_lookup = needs_manual.drop_duplicates(subset=["id"]).copy()
        manual_lookup["id"] = manual_lookup["id"].astype(str)
        manual_lookup = manual_lookup.set_index("id")
        is_manual = lineage["id"].isin(manual_lookup.index)
        for col in [
            "pipeline_status",
            "stage",
            "exit_category",
            "upload_decision",
            "upload_decision_reason",
            "upload_decision_note",
            "manual_note",
        ]:
            if col not in manual_lookup.columns:
                continue
            lineage.loc[is_manual, col] = lineage.loc[is_manual, "id"].map(
                manual_lookup[col]
            )
        if "event_name" in manual_lookup.columns:
            lineage.loc[is_manual, "event_name"] = lineage.loc[is_manual, "id"].map(
                manual_lookup["event_name"]
            )

    if battle_metadata is None:
        battle_metadata = build_battle_metadata(
            raw_dir=raw_dir,
            youtube_json_name=youtube_json_name,
            events_csv_name=events_csv_name,
            versetracker_csv_name=versetracker_csv_name,
            rename_map=rename_map,
            manual_matchups=manual_matchups,
            upload_decisions=upload_decisions,
            vt_event_dates=vt_event_dates,
        )

    final_rows: list[dict[str, object]] = []
    for _, battle in battle_metadata.iterrows():
        key = battle_key(battle["id"])
        if key is None:
            continue
        source_ids = battle["id"] if isinstance(battle["id"], list) else [battle["id"]]
        for source_id in source_ids:
            source_id = str(source_id)
            final_rows.append(
                {
                    "id": source_id,
                    "pipeline_status": (
                        "included" if source_id == str(key) else "consolidated_part"
                    ),
                    "stage": (
                        "final" if source_id == str(key) else "consolidate_battle_parts"
                    ),
                    "event_name": battle.get("event_name", pd.NA),
                    "event_date": battle.get("event_date", pd.NaT),
                    "event_date_source": battle.get("event_date_source", pd.NA),
                    "battle_key": str(key),
                    "final_title": battle.get("title", pd.NA),
                    "final_matchup": battle.get("matchup", pd.NA),
                    "emcee1": battle.get("emcee1", pd.NA),
                    "emcee2": battle.get("emcee2", pd.NA),
                }
            )

    if final_rows:
        final_lookup = pd.DataFrame(final_rows).drop_duplicates(subset=["id"]).set_index("id")
        is_final_source = lineage["id"].isin(final_lookup.index)
        for col in [
            "pipeline_status",
            "stage",
            "event_name",
            "event_date",
            "event_date_source",
            "battle_key",
            "final_title",
            "final_matchup",
            "emcee1",
            "emcee2",
        ]:
            lineage.loc[is_final_source, col] = lineage.loc[is_final_source, "id"].map(
                final_lookup[col]
            )

    manual_fields = _manual_matchup_audit_fields(manual_matchups, rename_map=rename_map)
    if not manual_fields.empty:
        manual_lookup = manual_fields.set_index("battle_key")
        lineage_keys = lineage["battle_key"].where(lineage["battle_key"].notna(), lineage["id"])
        is_manual = lineage_keys.isin(manual_lookup.index)
        for col in [
            "helper_emcee",
            "emcee1_status",
            "emcee2_status",
            "helper_status",
            "manual_note",
        ]:
            lineage.loc[is_manual, col] = lineage_keys.loc[is_manual].map(
                manual_lookup[col]
            )

    if results is None:
        results = load_results()

    if not results.empty and "battle_key" in lineage.columns:
        result_lookup = results.copy()
        result_lookup["id"] = result_lookup["id"].astype(str)
        result_lookup = result_lookup.drop_duplicates(subset=["id"]).set_index("id")
        has_battle_key = lineage["battle_key"].notna()
        lineage.loc[has_battle_key, "annotation_status"] = "missing"
        annotated = has_battle_key & lineage["battle_key"].isin(result_lookup.index)
        lineage.loc[annotated, "annotation_status"] = "annotated"
        for col in ["battle_type", "winner", "votes_winner", "votes_loser"]:
            lineage.loc[has_battle_key, col] = lineage.loc[has_battle_key, "battle_key"].map(
                result_lookup[col]
            )
    else:
        lineage.loc[lineage["battle_key"].notna(), "annotation_status"] = "missing"

    unclassified = lineage["pipeline_status"].isna()
    lineage.loc[unclassified, "pipeline_status"] = "unclassified"

    lineage = lineage[UPLOAD_LINEAGE_COLUMNS]
    if "upload_date" in lineage.columns:
        lineage = lineage.sort_values("upload_date").reset_index(drop=True)
    return lineage


def build_manual_matchup_review_uploads(
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
) -> pd.DataFrame:
    """
    Return known battle uploads awaiting a hand-entered 1v1 matchup.

    These rows are listed in ``data/overrides/manual_matchups.csv`` with
    ``emcee1=NA`` and ``emcee2=NA``. They are intentionally removed from the
    generic ``filtered_out.csv`` audit, but are not included in final battle
    metadata until the two emcee columns are resolved.
    """
    raw_dir = Path(raw_dir)
    df_yt = load_youtube_uploads(raw_dir / youtube_json_name)
    df_events = load_event_metadata(raw_dir / events_csv_name)
    _, _, _, needs_manual = _filter_upload_stages(
        df_yt,
        df_events,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )
    if not needs_manual.empty and "stage" in needs_manual.columns:
        needs_manual = needs_manual[needs_manual["stage"] == "manual_matchup_override"].copy()

    cols = [
        "id",
        "yt_raw_title",
        "title",
        "event_name",
        "upload_date",
        "url",
        "stage",
        "exit_category",
        "manual_note",
    ]
    needs_manual = needs_manual[[c for c in cols if c in needs_manual.columns]]
    if "upload_date" in needs_manual.columns:
        needs_manual = needs_manual.sort_values("upload_date").reset_index(drop=True)
    return needs_manual


def _summary_delta(input_rows: object, output_rows: object) -> object:
    if input_rows is pd.NA or output_rows is pd.NA:
        return pd.NA
    return int(output_rows) - int(input_rows)


def _pipeline_summary_row(
    *,
    stage_order: int,
    stage: str,
    input_rows: object,
    output_rows: object,
    exit_rows: int = 0,
    exit_status: str = "",
    note: str = "",
) -> dict[str, object]:
    return {
        "stage_order": stage_order,
        "stage": stage,
        "input_rows": input_rows,
        "output_rows": output_rows,
        "delta_rows": _summary_delta(input_rows, output_rows),
        "exit_rows": int(exit_rows),
        "exit_status": exit_status,
        "note": note,
    }


def build_pipeline_stage_summary(
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    versetracker_csv_name: str = "versetracker_event_dates.csv",
    rename_map: RenameMap | None = None,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
    vt_event_dates: Mapping[str, pd.Timestamp] | None = None,
    battle_metadata: pd.DataFrame | None = None,
    ft_battles: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Summarize raw-to-output row counts at each major wrangling stage.

    This is the compact companion to ``upload_lineage.csv``: it explains the
    row-count changes step by step, while ``build_pipeline_stage_drops`` lists
    the exact upload ids that exited at filter/manual-review stages.
    """
    raw_dir = Path(raw_dir)
    df_yt = load_youtube_uploads(raw_dir / youtube_json_name)
    df_events = load_event_metadata(raw_dir / events_csv_name)
    if manual_matchups is None:
        manual_matchups = load_manual_matchups()
    if upload_decisions is None:
        upload_decisions = load_upload_decisions()

    trace, excluded, needs_manual = _upload_stage_trace(
        df_yt,
        df_events,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )

    needs_manual_matchup = (
        needs_manual[needs_manual["stage"] == "manual_matchup_override"]
        if "stage" in needs_manual.columns
        else needs_manual.iloc[0:0]
    )
    needs_upload_review = (
        needs_manual[needs_manual["stage"] == "upload_decision_review"]
        if "stage" in needs_manual.columns
        else needs_manual.iloc[0:0]
    )

    event_drops = excluded[excluded["stage"] == "drop_excluded_events"]
    event_stage_ids = (
        set(trace["attach_event_metadata"]["id"].astype(str))
        if "id" in trace["attach_event_metadata"].columns
        else set()
    )
    pending_event_drops = event_drops[
        ~event_drops["id"].astype(str).isin(event_stage_ids)
    ]

    if battle_metadata is None:
        battle_metadata = build_battle_metadata(
            raw_dir=raw_dir,
            youtube_json_name=youtube_json_name,
            events_csv_name=events_csv_name,
            versetracker_csv_name=versetracker_csv_name,
            rename_map=rename_map,
            manual_matchups=manual_matchups,
            upload_decisions=upload_decisions,
            vt_event_dates=vt_event_dates,
        )
    if ft_battles is None:
        ft_battles = build_ft_battles_from_metadata(
            battle_metadata,
            require_results=False,
        )

    def drop_count(stage: str) -> int:
        return int((excluded["stage"] == stage).sum())

    raw_n = len(trace["raw_youtube"])
    prepared_n = len(trace["prepare_uploads"])
    decision_n = len(trace["apply_upload_decisions"])
    with_vs_n = len(trace["filter_titles_with_vs"])
    nonbattle_n = len(trace["drop_non_battles"])
    manual_flow_n = len(trace["manual_matchup_review_split"])
    manual_output_n = manual_flow_n + len(pending_event_drops)
    one_v_one_n = len(trace["keep_1v1_or_manual_matchup"])
    with_event_n = len(trace["attach_event_metadata"])
    event_input_n = with_event_n + len(pending_event_drops)
    event_output_n = len(trace["drop_excluded_events"])
    metadata_n = len(battle_metadata)
    final_n = len(ft_battles)

    rows = [
        _pipeline_summary_row(
            stage_order=1,
            stage="raw_youtube",
            input_rows=pd.NA,
            output_rows=raw_n,
            note="Rows loaded from youtube_videos.json.",
        ),
        _pipeline_summary_row(
            stage_order=2,
            stage="prepare_uploads",
            input_rows=raw_n,
            output_rows=prepared_n,
            note="Clean titles, parse dates/durations, numeric metrics, and preserve yt_raw_title.",
        ),
        _pipeline_summary_row(
            stage_order=3,
            stage="apply_upload_decisions",
            input_rows=prepared_n,
            output_rows=decision_n,
            exit_rows=(
                drop_count("upload_decision_override") + len(needs_upload_review)
            ),
            exit_status="manual_upload_decision",
            note="Apply exact include/exclude/review decisions from data/overrides/upload_decisions.csv.",
        ),
        _pipeline_summary_row(
            stage_order=4,
            stage="filter_titles_with_vs",
            input_rows=decision_n,
            output_rows=with_vs_n,
            exit_rows=drop_count("filter_titles_with_vs"),
            exit_status="excluded",
            note="Keep uploads whose working title contains a standalone 'vs' token.",
        ),
        _pipeline_summary_row(
            stage_order=5,
            stage="drop_non_battles",
            input_rows=with_vs_n,
            output_rows=nonbattle_n,
            exit_rows=drop_count("drop_non_battles"),
            exit_status="excluded",
            note="Remove title-keyword matches such as flyers, trailers, interviews, and other non-battles.",
        ),
        _pipeline_summary_row(
            stage_order=6,
            stage="manual_matchup_review_split",
            input_rows=nonbattle_n,
            output_rows=manual_output_n,
            exit_rows=len(needs_manual_matchup),
            exit_status="needs_manual_matchup",
            note=(
                "Hold unresolved manual matchups for review; resolved manual rows continue. "
                "Pending rows in excluded event categories are credited to drop_excluded_events."
            ),
        ),
        _pipeline_summary_row(
            stage_order=7,
            stage="keep_1v1_or_manual_matchup",
            input_rows=manual_flow_n,
            output_rows=one_v_one_n,
            exit_rows=drop_count("keep_1v1"),
            exit_status="excluded",
            note="Keep normal 1v1-looking titles plus explicitly resolved manual matchups.",
        ),
        _pipeline_summary_row(
            stage_order=8,
            stage="attach_event_metadata",
            input_rows=one_v_one_n,
            output_rows=with_event_n,
            note="Merge scraped event name, date, and location metadata by upload id.",
        ),
        _pipeline_summary_row(
            stage_order=9,
            stage="drop_excluded_events",
            input_rows=event_input_n,
            output_rows=event_output_n,
            exit_rows=drop_count("drop_excluded_events"),
            exit_status="excluded",
            note="Remove excluded event categories such as Process of Illumination and tryouts.",
        ),
        _pipeline_summary_row(
            stage_order=10,
            stage="finalize_battle_metadata",
            input_rows=event_output_n,
            output_rows=metadata_n,
            exit_rows=max(event_output_n - metadata_n, 0),
            exit_status="row_count_change",
            note="Consolidate multi-part uploads, apply date/location fixes, and select metadata columns.",
        ),
        _pipeline_summary_row(
            stage_order=11,
            stage="publish_ft_battles",
            input_rows=metadata_n,
            output_rows=final_n,
            exit_rows=max(metadata_n - final_n, 0),
            exit_status="row_count_change",
            note="Join annotation results by battle key and select the final analysis columns.",
        ),
    ]
    return pd.DataFrame(rows, columns=PIPELINE_SUMMARY_COLUMNS)


def build_pipeline_stage_drops(
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
) -> pd.DataFrame:
    """
    Return the exact raw upload rows that exit at filter/manual-review stages.

    ``filtered_out.csv`` remains the narrower compatibility view of excluded
    rows. This table keeps the stage/status columns, so manual-review holds are
    visible alongside true exclusions.
    """
    raw_dir = Path(raw_dir)
    df_yt = load_youtube_uploads(raw_dir / youtube_json_name)
    df_events = load_event_metadata(raw_dir / events_csv_name)
    if manual_matchups is None:
        manual_matchups = load_manual_matchups()
    if upload_decisions is None:
        upload_decisions = load_upload_decisions()

    _, excluded, needs_manual = _upload_stage_trace(
        df_yt,
        df_events,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )

    exits = pd.concat([excluded, needs_manual], ignore_index=True)
    if exits.empty:
        return pd.DataFrame(columns=PIPELINE_STAGE_DROP_COLUMNS)

    stage_order = {
        "upload_decision_override": 3,
        "upload_decision_review": 3,
        "filter_titles_with_vs": 4,
        "drop_non_battles": 5,
        "manual_matchup_override": 6,
        "keep_1v1": 7,
        "drop_excluded_events": 9,
    }
    exits["stage_order"] = exits["stage"].map(stage_order).fillna(99).astype(int)

    for col in PIPELINE_STAGE_DROP_COLUMNS:
        if col not in exits.columns:
            exits[col] = pd.NA

    exits = exits[PIPELINE_STAGE_DROP_COLUMNS]
    sort_cols = ["stage_order"]
    if "upload_date" in exits.columns:
        sort_cols.append("upload_date")
    sort_cols.append("id")
    return exits.sort_values(sort_cols, na_position="last").reset_index(drop=True)


def write_audit_outputs(
    raw_dir: PathLike,
    debug_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
) -> tuple[Path, Path, Path, Path, Path]:
    """
    Write the reproducible debug audit files and return their paths.

    Outputs:
    - ``filtered_out.csv``: compatibility view from ``build_excluded_uploads``;
    - ``upload_lineage.csv``: one row per raw YouTube upload.
    - ``manual_matchup_needed.csv``: known battles awaiting manual matchup rows.
    - ``pipeline_summary.csv``: row counts at each major pipeline stage.
    - ``pipeline_stage_drops.csv``: exact ids exiting at filter/manual stages.
    """
    debug_dir = Path(debug_dir)
    debug_dir.mkdir(parents=True, exist_ok=True)

    excluded = build_excluded_uploads(
        raw_dir=raw_dir,
        youtube_json_name=youtube_json_name,
        events_csv_name=events_csv_name,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )
    lineage = build_upload_lineage(
        raw_dir=raw_dir,
        youtube_json_name=youtube_json_name,
        events_csv_name=events_csv_name,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )
    manual_needed = build_manual_matchup_review_uploads(
        raw_dir=raw_dir,
        youtube_json_name=youtube_json_name,
        events_csv_name=events_csv_name,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )
    pipeline_summary = build_pipeline_stage_summary(
        raw_dir=raw_dir,
        youtube_json_name=youtube_json_name,
        events_csv_name=events_csv_name,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )
    pipeline_drops = build_pipeline_stage_drops(
        raw_dir=raw_dir,
        youtube_json_name=youtube_json_name,
        events_csv_name=events_csv_name,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )

    excluded_path = debug_dir / "filtered_out.csv"
    lineage_path = debug_dir / "upload_lineage.csv"
    manual_path = debug_dir / "manual_matchup_needed.csv"
    summary_path = debug_dir / "pipeline_summary.csv"
    drops_path = debug_dir / "pipeline_stage_drops.csv"
    outputs = [
        (excluded, excluded_path),
        (lineage, lineage_path),
        (manual_needed, manual_path),
        (pipeline_summary, summary_path),
        (pipeline_drops, drops_path),
    ]
    for frame, path in outputs:
        with atomic_output_path(path) as temporary:
            frame.to_csv(temporary, index=False)
    return excluded_path, lineage_path, manual_path, summary_path, drops_path


