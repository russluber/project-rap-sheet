"""Single-execution orchestration for the raw-to-metadata pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from . import DATA_DIR
from .battles import finalize_battles
from .contracts import PIPELINE_STAGE_CONTRACTS
from .events import attach_event_metadata, drop_excluded_events
from .inputs import PipelineInputs, load_pipeline_inputs
from .rules import ExclusionRule, compile_exclusion_pattern, first_matching_rule
from .uploads import (
    ManualMatchupMap,
    PathLike,
    RenameMap,
    UploadDecisionMap,
    _hold_upload_decision_rows,
    _keep_upload_decision_includes,
    _manual_matchup_notes,
    _pending_manual_matchup_ids,
    add_matchup_and_split,
    add_matchup_clean,
    apply_emcee_rename,
    apply_manual_matchup_overrides,
    drop_non_battles,
    filter_titles_with_vs,
    keep_1v1_or_manual_matchup,
    prepare_uploads,
)


@dataclass
class PipelineRun:
    """All reusable products from one execution of the metadata pipeline."""

    inputs: PipelineInputs
    stages: dict[str, pd.DataFrame]
    excluded_uploads: pd.DataFrame
    review_uploads: pd.DataFrame
    battle_metadata: pd.DataFrame

    @property
    def raw_dir(self) -> Path:
        return self.inputs.raw_dir

    @property
    def rename_map(self) -> Mapping[str, str]:
        return self.inputs.rename_map

    @property
    def manual_matchups(self) -> ManualMatchupMap:
        return self.inputs.manual_matchups

    @property
    def upload_decisions(self) -> UploadDecisionMap:
        return self.inputs.upload_decisions

    @property
    def vt_event_dates(self) -> Mapping[str, pd.Timestamp]:
        return self.inputs.vt_event_dates

    @property
    def raw_uploads(self) -> pd.DataFrame:
        return self.inputs.raw_uploads

    @property
    def raw_events(self) -> pd.DataFrame:
        return self.inputs.raw_events


def _require_stage_contract(stage: str, frame: pd.DataFrame) -> pd.DataFrame:
    """Validate a named major stage and return the same frame for composition."""
    contract = PIPELINE_STAGE_CONTRACTS[stage]
    return contract.require(frame, source=f"pipeline stage {stage}")


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


def _rule_audit_fields(
    row,
    *,
    title_rules: tuple[ExclusionRule, ...],
    event_rules: tuple[ExclusionRule, ...],
) -> dict[str, object]:
    """Structured rule metadata responsible for a row exit, if any."""
    if row["excluded_reason"] == "non-battle keyword":
        match = first_matching_rule(row.get("title"), list(title_rules))
    elif row["excluded_reason"] == "excluded event":
        match = first_matching_rule(row.get("event_name"), list(event_rules))
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
    inputs: PipelineInputs,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, pd.DataFrame]:
    """Execute upload stages once and record every filter/review exit."""
    df_yt = inputs.raw_uploads
    df_events = inputs.raw_events
    rename_map = inputs.rename_map
    manual_matchups = inputs.manual_matchups
    upload_decisions = inputs.upload_decisions
    event_exclusion_re = compile_exclusion_pattern(list(inputs.event_exclusion_rules))
    pre = _require_stage_contract("prepare_uploads", prepare_uploads(df_yt))
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
        drop_non_battles(after_vs, exclusion_rules=inputs.title_exclusion_rules),
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
            event_exclusion_re,
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
        keep_1v1_or_manual_matchup(not_pending, manual_matchups=manual_matchups),
        upload_decisions,
    )
    parsed_1v1 = (
        after_1v1.pipe(add_matchup_and_split)
        .pipe(apply_manual_matchup_overrides, manual_matchups=manual_matchups)
        .pipe(apply_emcee_rename, rename_map=rename_map)
        .pipe(add_matchup_clean)
    )
    parsed_1v1 = _require_stage_contract(
        "parse_and_canonicalize_matchups",
        parsed_1v1,
    )
    if "upload_date" in parsed_1v1.columns:
        parsed_1v1 = parsed_1v1.sort_values("upload_date").reset_index(drop=True)

    with_event_meta = _require_stage_contract(
        "attach_event_metadata",
        attach_event_metadata(
            parsed_1v1,
            df_events,
            location_aliases=inputs.location_aliases,
        ),
    )
    after_event_filter = _keep_upload_decision_includes(
        with_event_meta,
        drop_excluded_events(
            with_event_meta,
            exclusion_rules=inputs.event_exclusion_rules,
        ),
        upload_decisions,
    )
    after_event_filter = _require_stage_contract(
        "drop_excluded_events",
        after_event_filter,
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

    # Enrich early exits with event names without changing which stage receives
    # credit for the exit.
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
                needs_upload_review["event_name"] = needs_upload_review["event_name"].fillna(
                    needs_upload_review["_event_name_lookup"]
                )
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
        rule_fields = excluded.apply(
            _rule_audit_fields,
            axis=1,
            result_type="expand",
            title_rules=inputs.title_exclusion_rules,
            event_rules=inputs.event_exclusion_rules,
        )
        for col in rule_cols:
            if col not in excluded.columns:
                excluded[col] = pd.NA
            if col in rule_fields.columns:
                excluded[col] = excluded[col].combine_first(rule_fields[col])

    stages = {
        "raw_youtube": df_yt.copy(),
        "prepare_uploads": pre,
        "apply_upload_decisions": after_decisions,
        "filter_titles_with_vs": after_vs,
        "drop_non_battles": after_nonbattle,
        "manual_matchup_review_split": not_pending,
        "keep_1v1_or_manual_matchup": after_1v1,
        "parse_and_canonicalize_matchups": parsed_1v1,
        "attach_event_metadata": with_event_meta,
        "drop_excluded_events": after_event_filter,
    }
    return stages, excluded, needs_review


def build_pipeline_run(
    raw_dir: PathLike | None = None,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    versetracker_csv_name: str = "versetracker_event_dates.csv",
    rename_map: RenameMap | None = None,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
    vt_event_dates: dict[str, pd.Timestamp] | None = None,
    *,
    inputs: PipelineInputs | None = None,
    data_dir: PathLike = DATA_DIR,
) -> PipelineRun:
    """Execute filtering, metadata attachment, and finalization exactly once."""
    if inputs is None:
        if raw_dir is None:
            raise TypeError("raw_dir is required when inputs is not provided")
        inputs = load_pipeline_inputs(
            raw_dir,
            data_dir=data_dir,
            youtube_json_name=youtube_json_name,
            events_csv_name=events_csv_name,
            versetracker_csv_name=versetracker_csv_name,
            rename_map=rename_map,
            manual_matchups=manual_matchups,
            upload_decisions=upload_decisions,
            vt_event_dates=vt_event_dates,
        )
    elif any(
        value is not None
        for value in (rename_map, manual_matchups, upload_decisions, vt_event_dates)
    ):
        raise TypeError("pass either inputs or individual input overrides, not both")

    stages, excluded, review = _upload_stage_trace(inputs)

    filtered = stages["drop_excluded_events"]
    battle_metadata = _require_stage_contract(
        "finalize_battle_metadata",
        finalize_battles(
            filtered,
            vt_event_dates=inputs.vt_event_dates,
            event_location_overrides=inputs.event_location_overrides,
            event_location_patterns=inputs.event_location_patterns,
            event_date_overrides=inputs.event_date_overrides,
        ),
    )
    stages = dict(stages)
    stages["finalize_battle_metadata"] = battle_metadata

    return PipelineRun(
        inputs=inputs,
        stages=stages,
        excluded_uploads=excluded,
        review_uploads=review,
        battle_metadata=battle_metadata,
    )
