# FlipTop

The core Python package for Project Rap Sheet. It implements the reproducible
pipeline that turns raw FlipTop data into a rich **one-row-per-battle** metadata
table, then publishes the final result-enriched `ft_battles` table used for
analysis. It also includes helpers that build structures from that table, record
battle results, and refresh everything with one command.

The code lives here (not in notebooks) so the pipeline is modular, reusable,
testable, and reproducible. Notebooks *import* this package; they don't
reimplement it.

---

## Contents

- [Architecture at a glance](#architecture-at-a-glance)
- [Package layout](#package-layout)
- [The pipeline: raw → `ft_battles`](#the-pipeline-raw--ft_battles) (`uploads.py`, `events.py`, `battles.py`, `publish.py`)
- [Pipeline map](#pipeline-map)
- [`ft_battles` schema](#ft_battles-schema)
- [Auditing upload lineage](#auditing-upload-lineage)
- [Derived structures](#derived-structures) (`structures.py`)
- [Output validation gate](#output-validation-gate) (`validate.py`)
- [Canonical emcee names](#canonical-emcee-names) (`rename_map.py`)
- [Battle results / annotations](#battle-results--annotations) (`annotations.py`, `annotate.py`)
- [Refreshing the datasets](#refreshing-the-datasets) (`refresh.py`)
- [Public API & typical usage](#public-api--typical-usage)
- [Testing](#testing)

---

## Architecture at a glance

Everything flows one way toward `ft_battles` as the hub: the pipeline builds
metadata, `release.py` assembles and checks a complete candidate, and only a
passing candidate replaces the three processed tables that other modules use.

```
 data/raw/youtube_videos.json ─┐
                               ├─► pipeline.py ─► battles.py ─► publish.py ─► ft_battles ──┬─► structures.py
 data/raw/matchup_events_      ─┘   (+ emcee_aliases.csv)      │
   metadata.csv                                                ??? annotations.py (validated result store)

 scripts/ (fetch raw) ──► data/raw/        refresh.py orchestrates: fetch (optional) → build → write
                                           lineage.py projects audits from the same run
                                           annotate.py records battle results into data/annotations/
```

**Design principles**

- **Two layers, one public output.** `build_battle_metadata()` keeps the rich
  audit table with provenance fields; `build_ft_battles()` joins validated
  annotations and emits the final analysis table.
- **One execution, many outputs.** `pipeline.py` records stage frames and row
  exits once; `battles.py` exposes metadata compatibility helpers, while
  `lineage.py` projects every audit table from that exact run.
- **One explicit input snapshot.** `inputs.py` loads raw tables, rules,
  overrides, aliases, reference dates, and annotations once. `pipeline.py`,
  audits, candidate construction, and manifests reuse that same immutable
  `PipelineInputs` object instead of reading defaults midway through a run.
- **Review before release.** `release.py` builds the final tables and human
  review queues together. It records the run and proposed changes before the
  official processed files are replaced as a rollback-safe bundle.
- **Raw collection is transactional.** `raw_snapshot.py` gives both collectors
  a staged copy of the current raw data, validates their combined result, and
  promotes the raw files together with rollback on publication failure.
- **Contracts at every boundary.** `contracts.py` declares the schema, key,
  type, blank-value, and vocabulary rules for source tables and major pipeline
  stages. Failures identify the file or transform that introduced the problem.
- **Logic is separate from CLI.** `pipeline.py`/`lineage.py` ↔ `refresh.py`
  (build/audit ↔ command) and `annotations.py` ↔ `annotate.py` (store ↔ command)
  are deliberate splits.
- **Hand-maintained data lives in `data/` files, not in code** — emcee aliases,
  battle results, etc. are CSVs you can edit like any other data.
- **Imports stay cheap.** `import fliptop` pulls in nothing heavy; the public
  entry points are imported lazily on first use (so `pandas`/`networkx` load
  only when you actually call something).

---

## Package layout

```
fliptop/
├── __init__.py         # package init: shared data-dir paths + lazy public API
├── inputs.py           # one immutable snapshot of all file-backed run inputs
├── pipeline.py         # one execution: stages, exits, reviews, and battle metadata
├── battles.py          # battle finalization + compatibility metadata entry point
├── uploads.py          # upload-side cleaning, title filters, and matchup parsing
├── events.py           # event metadata parsing, date/location fixes, event joins
├── publish.py          # final ft_battles schema, annotation join, and file writes
├── lineage.py          # audit tables explaining raw upload inclusion/exclusion
├── rename_map.py       # loads/validates the alias map from data/emcee_aliases.csv
├── overrides.py        # loads/validates the correction tables in data/overrides/
├── rules.py            # loads/validates reviewable exclusion rules in data/rules/
├── structures.py       # structures derived from ft_battles (emcee table + battle network)
├── validate.py         # output data-quality gate for ft_battles
├── annotations.py      # battle-results store (winner/judging/notes) + helpers
├── annotate.py         # fliptop-annotate CLI: interactively record battle results
└── refresh.py          # fliptop-refresh CLI: rebuild (and optionally re-fetch) the datasets
```

---

## The pipeline: raw → `ft_battles`

`pipeline.py` orchestrates the metadata build and returns a `PipelineRun` that
retains its `PipelineInputs` snapshot plus stage frames, recorded exits/reviews, and final battle
metadata. Upload-side transforms live in `uploads.py`; event metadata parsing
and date/location fixes live in `events.py`; final publishing lives in
`publish.py`; `lineage.py` derives row-level audit tables from the same run;
`release.py` combines those products into a candidate and controls publication.
`contracts.py` supplies the shared rulebook enforced by loaders and by the
major stage transitions in `pipeline.py`.
`build_battle_metadata(raw_dir=...)` runs three cleaning stages and returns the
rich metadata table. `build_ft_battles` then joins validated battle results and
returns the final analysis table.

```
build_pipeline_run
  ├─ PipelineInputs (all file-backed dependencies loaded once)
  ├─ upload stages + recorded exits/reviews
  ├─ attach/filter event metadata
  └─ finalize_battles -> PipelineRun.battle_metadata

build_ft_battles_from_metadata
  └─ join annotations and select final columns
```

Each stage is a thin orchestration over small, single-purpose transforms (most
take a DataFrame and return a new one), which keeps them easy to read and test.
The contracts after preparation, matchup parsing, event enrichment/filtering,
and metadata finalization make each module's output assumptions explicit.

## Pipeline Map

This is the raw-to-output path at a glance. The public package-root functions
(`build_battle_metadata`, `build_ft_battles`, `build_upload_lineage`, etc.) are
lazy re-exports; the owning module below is where the behavior lives.

| order | stage | owner | input | output | exits / notes |
| ----- | ----- | ----- | ----- | ------ | ------------- |
| 1 | load raw sources | `pipeline.py` | `data/raw/youtube_videos.json`, `data/raw/matchup_events_metadata.csv`, `data/raw/versetracker_event_dates.csv` | raw upload/event frames plus VerseTracker date map | File loading only; no filtering. |
| 2 | clean and filter uploads | `uploads.py` | raw YouTube uploads, alias map, `manual_matchups.csv`, `upload_decisions.csv`, `title_exclusions.csv` | parseable 1v1 upload candidates with canonical `emcee1`, `emcee2`, `matchup_clean` | Exact upload decisions can include/exclude/review rows; title/format filters remove non-battles and unsupported multi-emcee titles unless manually resolved. |
| 3 | attach and filter event metadata | `events.py` | 1v1 uploads plus scraped event metadata and event/location/date overrides | uploads with `event_name`, `event_date`, `event_date_source`, `event_location_clean` | COVID-era website dates are masked, post-COVID descriptions can fill missing metadata, and `event_exclusions.csv` removes out-of-scope event categories. |
| 4 | finalize battle metadata | `battles.py` | event-enriched upload rows plus VerseTracker dates | rich one-row-per-battle metadata with `METADATA_COLUMNS` | Multi-part uploads are consolidated; event locations/dates are overridden or imputed; provenance/debug fields remain available. |
| 5 | publish final output | `publish.py` | rich battle metadata plus `battle_results.csv` | standalone `ft_battles` table with `FINAL_COLUMNS` | Annotation results are validated and joined; list-valued multipart ids are scalarized; provenance/debug-only columns are intentionally excluded. |
| 6 | derive analysis structures | `structures.py` | final `ft_battles` | `battle_participants.csv`, `emcees.csv`, battle network | No-show/helper participation is modeled in the participant table. |
| audit | explain every raw upload | `lineage.py` | the completed `PipelineRun` | `upload_lineage.csv`, `filtered_out.csv`, `manual_matchup_needed.csv`, `pipeline_summary.csv`, `pipeline_stage_drops.csv` | Projects the exits and stage frames recorded by the actual build; no second filter execution. |

### Stage 1 — clean YouTube uploads → 1v1 uploads

`uploads.make_df_1v1_uploads(df_yt)` first runs the shared
**`prepare_uploads`** step:

| transform | what it does |
| --------- | ------------ |
| `clean_titles` | trim whitespace, strip wrapping quotes |
| `parse_upload_date` | ISO-8601 → tz-naive `datetime64` |
| `add_duration_columns` | ISO-8601 duration → `duration_seconds` + `duration_hms` |
| `convert_video_metrics_to_numeric` | view/like/comment counts → numeric |
| `copy_yt_title` | preserve the original title (with `pt. N`) as `yt_raw_title` |
| `strip_pt_suffix_from_title` | drop the `pt. N` suffix from the working title |

then the **three title/format filters** that make the first decision about what
counts as a battle:

| filter | keeps / drops |
| ------ | ------------- |
| `upload_decisions.csv` | exact `include`/`exclude`/`review` decisions from `data/overrides/upload_decisions.csv`; `include` can rescue parseable one-off uploads from broad filters, while `exclude`/`review` hold exact ids out of final outputs |
| `filter_titles_with_vs` | keep only titles containing the token `vs` |
| `drop_non_battles` | drop titles matching active rules in `data/rules/title_exclusions.csv` (beatbox, tryout, flyer, `[LIVE]`, ...) — currently matched as **substrings**, case-insensitive. Title-labeled promo battles are kept and classified later via annotations. |
| `keep_1v1_or_manual_matchup` | drop multi-emcee formats (`>1 vs`, `/`, `+`, `N on M`, `and ... vs ... and`) unless a resolved row in `data/overrides/manual_matchups.csv` supplies the actual 1v1 matchup |

and finally extracts the matchup:

- `add_matchup_and_split` → `matchup`, `emcee1`, `emcee2` (also strips a trailing
  `- Finals`-style annotation)
- `apply_manual_matchup_overrides` → replace ambiguous no-show titles with a
  hand-entered `emcee1 vs emcee2` when available
- `apply_emcee_rename` → canonicalize names via the alias map (**exact, case-sensitive** match)
- `add_matchup_clean` → `matchup_clean` from the canonical names

### Stage 2 — attach event metadata

`events.attach_event_metadata(df_1v1, df_events_raw)` cleans the scraped event
side (`split_event_description` → `parse_event_date` →
`clean_event_location`), then joins it onto the uploads by **YouTube video id**.
Two special behaviors:

- **COVID-era mask.** Event dates inside `2020-05-01 … 2022-04-27` are set to
  `NaT` on purpose — FlipTop obfuscated those dates, so the scraped values are
  wrong. These are recovered later in Stage 3 from VerseTracker (see
  [COVID-era date imputation](#covid-era-date-imputation-versetracker)).
- **Post-COVID fallback.** For battles after the COVID window whose event date
  is still missing, `fill_metadata_from_yt_description` recovers event name /
  date / location from the YouTube description.

This stage also seeds **`event_date_source`**, a provenance tag that records
where each date came from — `website` initially, cleared inside the COVID window,
`description` for the post-COVID fallback. Stage 3 then tags `versetracker` and
`manual` as it imputes/overrides.

After metadata attachment, `drop_excluded_events` removes rows whose
`event_name` matches active rules in `data/rules/event_exclusions.csv`, such as
`Process of Illumination` or `tryout` (case-insensitive). These categories
cannot be filtered reliably during Stage 1 because many of their YouTube titles
contain neither phrase. The event exclusions are separate from
`title_exclusions.csv`, which remains title-only. Event exclusions still win over
manual no-show handling: a known no-show battle in a POI/tryout event stays
excluded with the rest of that event category.

`clean_event_location` also normalizes known messiness: prefers the text after
`@`, fixes the country separator (`City. Philippines` / `Metro Manila
Philippines` → `, Philippines`), collapses doubled words, and applies known
location aliases (e.g. Davao variants) from `data/overrides/location_aliases.csv`.

### Stage 3 — consolidate, tidy, finalize

`finalize_battles` drops helper columns, renames `matchup_clean → matchup` and
`event_location_clean → event_location`, then:

- **`consolidate_battle_parts`** collapses multi-part uploads (`pt. 1`, `pt. 2`,
  …) into one row: `id` and `url` become **ordered lists**, `duration_seconds`
  is summed, `upload_date` is the earliest part, `duration_hms` is recomputed.
- sorts newest-upload-first,
- applies `apply_manual_event_location_overrides` (hand-fixed locations from
  `data/overrides/`, for events the scrape got wrong),
- **`impute_event_dates_from_versetracker`** fills the COVID-masked `event_date`s
  (see [below](#covid-era-date-imputation-versetracker)) — before
  `normalize_event_day`, so the `(Day N)` suffix is still available,
- **`normalize_event_day`** standardizes multi-day events (see below),
- **`apply_manual_event_date_overrides`** pins specific battles whose YouTube
  description mis-dates them (from `data/overrides/event_dates.csv`, keyed by
  video id; runs last, so a hand-pin always wins),
- selects and orders the final columns.

#### Multi-day events (`normalize_event_day`)

FlipTop events that run over several days appear in the source as separate
`event_name`s with a day suffix — `"Ahon 16 (Day 1)"`, `"Ahon 16 (Day 2)"` (or
the comma form `"Gubat 12, Day 1"` from YouTube descriptions). This step:

- **Strips the day suffix** so the name standardizes to the event itself
  (`"Ahon 16"`), collapsing the per-day duplicates.
- **Resolves the per-day date.** The source often carries the *date range*
  (`"December 13-14, 2025"`) on every day's entry, which left both days pinned to
  the range's first day. When an `event_date` still equals the range start, it is
  moved to the N-th day (`start + (N-1)`, clamped to the range end), so Day 1 →
  Dec 13 and Day 2 → Dec 14. Dates that already differ from the start (correctly
  disambiguated at the source) and missing dates (COVID-era `NaT`) are left as-is.

The day number is used only internally — to resolve the date and strip the
suffix — and is **not** kept as a column: `event_name` + `event_date` already
identify the battle, and the ordinal is recoverable as the rank of the date
within its event. This runs *after* the location overrides, which still key on
the day-suffixed names. One known limitation: cross-month ranges
(`"Nov 30 – Dec 1"`) aren't parsed as a range yet.

#### COVID-era date imputation (VerseTracker)

The COVID-era mask in Stage 2 leaves a window of battles with no `event_date`
(FlipTop obfuscated those dates). `impute_event_dates_from_versetracker` fills
them from a small reference file,
[`data/raw/versetracker_event_dates.csv`](../data/raw/), scraped from VerseTracker
by [`scripts/fetch_versetracker_event_dates.py`](../scripts/fetch_versetracker_event_dates.py)
and loaded by `load_versetracker_event_dates`.

- For each row whose `event_date` is `NaT`, it strips any `(Day N)` off the event
  name (`_split_event_day`) and, if the base name is in the reference map, sets
  the date to the mapped **first-day** date plus `(N − 1)` days. VerseTracker
  lists only the first day, so the per-day offset is applied here (Ahon 11,
  Ahon 12, Bwelta Balentong 7).
- It only fills `NaT` rows — an existing date is **never** overwritten — and tags
  each filled row `versetracker` in `event_date_source`.
- It runs **before** `normalize_event_day` (which strips the `(Day N)` suffix it
  needs), and `apply_manual_event_date_overrides` runs **after**, so any
  hand-pinned entry in `data/overrides/event_dates.csv` still wins over a
  VerseTracker value.

The reference CSV is optional: if it's absent, `load_versetracker_event_dates`
returns `{}` and the step is a no-op (those battles simply stay `NaT`). The build
auto-loads it from `raw_dir`; pass `build_ft_battles(..., vt_event_dates={})` to
disable imputation explicitly.

> ⚠️ VerseTracker's dates are accurate to ~days, not exact (it appears to use the
> flyer-post date for some events). They're flagged via `event_date_source` so the
> approximate dates can be sliced out for sensitivity analysis.

---

## `ft_battles` schema

| column | type | notes |
| ------ | ---- | ----- |
| `id` | string | scalar battle key; for multi-part battles, the first part's YouTube id |
| `title` | string | cleaned title, `pt. N` suffix removed |
| `upload_date` | datetime | earliest part for multi-part battles |
| `duration_seconds` | number | summed across parts |
| `emcee1`, `emcee2` | string | canonicalized names |
| `matchup` | string | `emcee1 vs emcee2` |
| `event_name` | string | standardized — the `(Day N)` suffix is stripped (see below) |
| `event_date` | datetime | the battle's actual day (the specific day for multi-day events); COVID-era dates are imputed from VerseTracker (see above) |
| `event_location` | string | |
| `url` | string or list | a **list** for multi-part battles |
| `battle_type` | string | `judged` or `promo`, from `battle_results.csv` |
| `winner` | string | winning emcee, or `NA` for draws/promos |
| `votes_winner`, `votes_loser` | string | final vote totals as text, or `NA` when not applicable/unknown |

Rich audit fields such as `description`, `duration_hms`, and
`event_date_source` stay in the internal `battle_metadata` table returned by
`build_battle_metadata()`.

---

## Output Layer Contract

The project has four output layers with different jobs:

| layer | created by | purpose |
| ----- | ---------- | ------- |
| `battle_metadata` | `build_battle_metadata()` | rich internal build table with provenance/debug fields such as `description`, `duration_hms`, and `event_date_source` |
| `ft_battles.json` | `build_ft_battles()` / `fliptop-refresh` | clean standalone battle-level analysis output with only `FINAL_COLUMNS` |
| `battle_participants.csv` | `build_battle_participants()` / `fliptop-refresh` | long participant table for event-history/survival-style analysis |
| `release_manifest.json` | `fliptop-refresh` | committed input/output hashes, byte sizes, row counts, contract versions, and pipeline commit for offline verification |
| `data/debug/*` | `fliptop-refresh` | regenerated lineage, human review queues, release blockers, proposed changes, and a hashed run manifest |

`ft_battles.json` intentionally excludes provenance and audit-only fields. Do
not add columns such as `event_date_source`, `description`, `duration_hms`,
`yt_raw_title`, `rule_id`, or `upload_decision_note` to the final output. Use
`build_battle_metadata()` when inspecting date provenance, and use
`data/debug/upload_lineage.csv` or `data/debug/pipeline_stage_drops.csv` when
auditing why raw uploads were included, excluded, or held for review.

---

## Auditing upload lineage

To verify the filters aren't dropping real battles, use the lineage audit:
`build_upload_lineage(raw_dir)` returns **one row per raw YouTube upload** and
records what happened to it. Rows are tagged as `included`, `excluded`, or
`consolidated_part` (for the second upload in a multi-part battle). A fourth
status, `needs_manual_matchup`, marks known no-show battles whose title names
multiple possible emcees and whose actual 1v1 matchup still needs to be filled
in `data/overrides/manual_matchups.csv`, unless the row is excluded by event
category first. The lineage includes the filter stage, exclusion reason, matched
keyword, final battle key, canonical matchup, manual note, and annotation status
where applicable. Rule-based exits also carry `rule_id`, `rule_note`, and
`exit_category` from `data/rules/`. Exact upload decisions carry
`upload_decision`, `upload_decision_reason`, and `upload_decision_note` from
`data/overrides/upload_decisions.csv`.

For the narrower compatibility view, `build_excluded_uploads(raw_dir)` returns
only the removed uploads, tagged with the reason (`no 'vs' token`,
`non-battle keyword`, `not 1v1`, `excluded event`, or `manual upload decision`)
and, for keyword drops, the matched title or event keyword. Both audit views
share the same filter trace so they cannot drift apart.

```python
from fliptop import (
    RAW_DATA_DIR,
    build_excluded_uploads,
    build_pipeline_stage_drops,
    build_pipeline_stage_summary,
    build_upload_lineage,
)

lineage = build_upload_lineage(RAW_DATA_DIR)
excluded = build_excluded_uploads(RAW_DATA_DIR)
summary = build_pipeline_stage_summary(RAW_DATA_DIR)
drops = build_pipeline_stage_drops(RAW_DATA_DIR)
lineage["pipeline_status"].value_counts()
excluded["excluded_reason"].value_counts()
```

To write the local debug artifacts, run:

```bash
uv run fliptop-refresh
```

This writes `data/debug/upload_lineage.csv`,
`data/debug/filtered_out.csv`, `data/debug/manual_matchup_needed.csv`,
`data/debug/pipeline_summary.csv`, and
`data/debug/pipeline_stage_drops.csv`, plus `missing_results.csv`,
`release_blockers.txt`, `release_changes.csv`,
`release_changes_summary.txt`, and `run_manifest.json`. The release files show
what a human must fix, what differs from the current published battles, and the
exact input hashes and Git commit used for the run. The summary file gives row counts at
each major stage. The stage-drops file lists exact ids exiting at
filter/manual-review stages, while `filtered_out.csv` remains the narrower
compatibility view of true exclusions. The `data/debug/` directory is
git-ignored; these files are regenerated audit outputs, not hand-maintained
data.

---

## Derived structures

`structures.py` holds reusable structures **derived from `ft_battles`**. They
shape the table into analysis-ready form but do **not** perform analysis itself —
that belongs in notebooks. (A per-emcee career table for survival analysis would
be a natural addition here.)

- **Emcee table** (`build_emcees_table`) — every distinct name across
  `emcee1`/`emcee2`, sorted, with a stable 1-based `emcee_id`;
  `write_emcees_table` writes it to `data/processed/emcees.csv`.
- **Battle participants** (`build_battle_participants`) — one row per emcee
  participation. Use `appearance_credit` for event-history / survival analyses;
  no-show opponents have `appearance_credit=False`, while helper emcees have
  `appearance_credit=True` and `battle_credit=False`.
- **Battle network** (`build_battle_network`) — an undirected weighted graph:
  nodes are emcees (with a `battle_count`), edges mean two emcees battled (with
  `weight` = how many times).

```python
from fliptop.structures import (
    build_battle_network,
    build_battle_participants,
    build_emcees_table,
)

participants = build_battle_participants(ft_battles)
df_emcees = build_emcees_table(ft_battles, participants=participants)
G = build_battle_network(ft_battles)   # networkx.Graph
```

---

## Canonical emcee names

Emcees appear under many spellings/aliases in YouTube titles. `rename_map.py`
loads and validates the **alias → canonical** mapping that normalizes
`emcee1`/`emcee2`. The mapping is hand-maintained data, one row per alias:

```
data/emcee_aliases.csv
columns: alias,canonical
```

`load_rename_map()` reads it into a dict and validates:

- skips blank rows and no-op self-maps; de-duplicates identical rows;
- **raises** if an alias maps to two different canonicals (a real data error);
- **resolves alias chains transitively** with cycle detection — e.g. with
  `Ghostly → Goriong Talas` already present, adding `Spade → Ghostly` yields
  `Spade → Goriong Talas` regardless of row order (a plain `df.replace` is a
  single pass and would otherwise stop at `Ghostly`).

To register a new alias, append a row to `data/emcee_aliases.csv`; the next build
picks it up.

---

## Battle results / annotations

Battle results (including draws and promos) and judges' tallies are collected
**by hand** in an append-only CSV keyed by battle `id`. The CSV remains the
manual source of truth; `ft_battles.json` publishes only the core analysis
fields from it:

```
data/annotations/battle_results.csv
columns: id, battle_type, winner,
         votes_winner, votes_loser, votes_nv, votes_ot, overtime, notes
```

Every battle is one of two kinds — which the host announces. A draw is a judged
battle with no winner, not a third battle type:

| column | meaning |
| ------ | ------- |
| `battle_type` | `judged` (decision or draw) \| `promo` (no judging) |
| `winner` | the winning emcee; `NA` for a judged draw or promo |
| `votes_winner` / `votes_loser` | judges voting for the winner / loser |
| `votes_nv` | judges who did not vote (NV) |
| `votes_ot` | judges who voted to go to overtime |
| `overtime` | `yes` \| `no` — did the battle go to an OT round? |
| `notes` | free text, or the literal `none` |

The tally is always the **final (post-overtime)** result. Panel size varies
(5 / 7 / 9 judges) and is just the sum of the vote columns. Everything is stored
as text with explicit markers (`NA` where not applicable, `none` for empty
notes), so the CSV has **no blank cells**; convert the vote columns with
`pd.to_numeric` for analysis. A judged battle whose score was never recorded
keeps its `winner` but has `NA` in every vote column — that's how "winner known,
score unknown" is represented (no separate status for it). A judged row with
`winner=NA` is a draw; a promo row also has no winner, but `battle_type`
distinguishes it. Draw rulings that do not fit the structured fields belong in
`notes`.

**Current publishing contract.** The annotation CSV stays separate as the
hand-maintained source of truth, but the published `ft_battles.json` is now
result-enriched. `build_ft_battles()` validates the store against the battle
metadata, joins the core result fields, and keeps only the final analysis
columns. If a fetch adds new battles, `fliptop-refresh` will refuse to publish
until `fliptop-annotate` records those results.

**`annotations.py`** — the store and its helpers:

- `load_results()` / `save_results()` — read/write the CSV.
- `pending_battles(ft_battles)` — battles not yet annotated (newest first).
- `merge_results(ft_battles)` — left-join the full result store onto a battle
  table when you need fields beyond the published core result columns.
- `battle_key()` — the canonical scalar key for a battle (the first id for
  consolidated multi-part battles).

**`annotate.py`** — the `fliptop-annotate` console script: an interactive tool
that walks un-annotated battles, lets you pick the winner by `1`/`2`, record a
judged draw with `d`, or record a promo with `p`. It validates input and
**writes after every entry** (crash-safe and resumable — quit any time and it
picks up where you left off). When using `--redo`, leaving notes blank preserves
the existing note.

```bash
fliptop-annotate                 # go through all pending battles
fliptop-annotate --limit 20      # do up to 20, then stop
fliptop-annotate --event Ahon    # only battles whose event matches "Ahon"
fliptop-annotate --open          # also open each battle's URL in the browser
fliptop-annotate --redo "A vs B" # re-annotate an existing battle (fix a mistake)
```

---

## Table contracts and output validation

`contracts.py` guards structure at the point data enters or crosses a major
boundary. `TableContract` checks columns and order, key uniqueness, blanks,
basic types/date parsing, and closed vocabularies. `ContractViolation` reports
all structural problems it finds and includes either the source path or the
named pipeline stage. Contract definitions carry versions; `run_manifest.json`
records all active versions with the input hashes and Git commit.

These structural contracts complement the domain-quality gate rather than
replace it. For example, a contract can prove that `event_date` is a date, while
`validate.py` decides whether that date is historically plausible; the result
store also retains its cross-field judging rules.

## Output validation gate

`validate.py` guards the built table. `build_ft_battles` is a long chain of
heuristic filters, merges, and overrides, so a change in a raw source's shape (a
re-scrape, a YouTube API tweak) can silently produce a malformed table that still
writes cleanly to disk. `validate_ft_battles(df)` returns a list of
human-readable problems (empty == ok), checking the invariants the pipeline is
supposed to guarantee:

- every expected column is present (`fliptop.battles.FINAL_COLUMNS`, the single
  source of truth for the output schema);
- no metadata/audit-only columns are present in `ft_battles` (for example
  `event_date_source`, `rule_id`, or `upload_decision_note`);
- one row per battle — the scalar battle key (first id for multi-part battles) is
  present and unique;
- every battle has two non-blank emcees;
- `event_date` is within a plausible window (>= 2010, not in the future).

`validate_battle_metadata(df)` separately guards the rich metadata table,
including the `event_date_source` vocabulary (`website` | `description` |
`versetracker` | `manual`; missing is allowed for undated battles).

`fliptop-refresh` builds a candidate and writes its review files first, then
runs the gate and **aborts before changing processed data** if anything fails.
A passing candidate is serialized into a temporary bundle and reloaded before
all three processed tables are promoted; a promotion error restores the old
bundle. `summarize_ft_battles(df)` produces the
one-line build summary (battle count + `battle_type` breakdown) printed on each
refresh.

---

## Refreshing the datasets

`refresh.py` implements **`fliptop-refresh`** (a console script), the one-command
way to regenerate the processed datasets.

```bash
uv run fliptop-refresh                        # rebuild ft_battles.json + emcees.csv from existing raw data
uv run fliptop-refresh --fetch                # re-fetch raw data (YouTube + web) first, then rebuild
uv run fliptop-refresh --fetch --events-since 2025   # incremental: only re-scrape recent events, then rebuild
uv run fliptop-refresh --no-audit             # rebuild without writing data/debug audit files
uv run fliptop-verify-release                 # verify the committed processed release offline
```

- `rebuild_processed()` receives one candidate, runs the
  [validation gate](#output-validation-gate), and (if it passes) stages,
  reloads, and publishes all three processed tables plus their official manifest
  as one rollback-safe bundle.
- `fetch_raw()` (only with `--fetch`) runs the two `scripts/` collectors against
  a temporary copy of `data/raw/`; this needs a YouTube API key (see
  `data/README.md`). The combined candidate is contract-validated and promoted
  with rollback protection. The
  YouTube fetch is always incremental; the website events scrape is a **full
  overwrite** (2010 → now) by default.
- `--events-since YEAR` makes the events scrape incremental — only YEAR → now is
  scraped and **merged** into the existing CSV (much faster for routine updates).
  Run a plain `--fetch` periodically for a clean full reconcile. See
  [`scripts/README.md`](../scripts/README.md) for the overwrite-vs-merge
  trade-off.
- By default, refresh writes the audit and release-review files **before** it
  tries to publish. These include `data/debug/filtered_out.csv`,
  `data/debug/upload_lineage.csv`, `data/debug/manual_matchup_needed.csv`,
  `data/debug/pipeline_summary.csv`, and
  `data/debug/pipeline_stage_drops.csv`, plus `missing_results.csv`,
  `release_blockers.txt`, `release_changes.csv`,
  `release_changes_summary.txt`, and `run_manifest.json`. A blocked run still
  leaves these instructions behind while keeping the current processed files
  untouched. Use `--no-audit` only when you intentionally want to skip them.

This is the recommended way to regenerate data. The Python API below is for when
you want the table in memory or finer control. For the step-by-step maintainer
routine to catch up after new uploads, see
[`docs/workflows.md`](../docs/workflows.md).

---

## Public API & typical usage

The main entry points are re-exported from the package root and imported lazily,
so `import fliptop` stays cheap:

```python
from fliptop import (
    RAW_DATA_DIR, PROCESSED_DATA_DIR, DATA_DIR,   # shared paths
    build_battle_metadata,   # raw -> rich metadata
    build_ft_battles,        # raw + annotations -> final ft_battles
    build_excluded_uploads,  # audit of dropped uploads
    build_upload_lineage,    # audit of every raw YouTube upload
    build_manual_matchup_review_uploads,  # pending manual matchup queue
    build_battle_participants,  # ft_battles -> long participant table
    write_battle_participants_table,
    build_emcees_table,      # ft_battles -> emcee table
    write_emcees_table,
    build_battle_network,    # ft_battles -> networkx graph
    merge_results,           # join full battle results onto a battle table
    validate_battle_metadata,
    validate_ft_battles,     # data-quality gate for final ft_battles
)
```

Build the dataset in memory:

```python
from fliptop import RAW_DATA_DIR, build_battle_metadata, build_ft_battles

metadata = build_battle_metadata(raw_dir=RAW_DATA_DIR)
ft_battles = build_ft_battles(raw_dir=RAW_DATA_DIR)
```

Build and write it to disk. The committed output is newline-delimited JSON, so
pass `fmt="json"` (the function also supports `fmt="csv"`):

```python
from fliptop import RAW_DATA_DIR, PROCESSED_DATA_DIR
from fliptop.publish import write_ft_battles

write_ft_battles(
    out_path=PROCESSED_DATA_DIR / "ft_battles.json",
    raw_dir=RAW_DATA_DIR,
    fmt="json",
)
```

Reload the written dataset. It's newline-delimited (`lines=True`), and dates are
stored as epoch-ms, so name the date columns to restore the datetime dtype:

```python
import pandas as pd

ft_battles = pd.read_json(
    "data/processed/ft_battles.json",
    lines=True,
    convert_dates=["upload_date", "event_date"],
)
```

(Or just call `build_ft_battles(...)` again — building in memory keeps the
datetime dtypes and skips the JSON round-trip entirely.)

---

## Testing

The pipeline's transforms are covered by a `pytest` suite at the repo root
(`tests/`). Run it through the locked uv environment:

```bash
uv run pytest -q --basetemp .pytest-tmp
```

Tests use small hand-built DataFrames for the unit transforms, plus end-to-end
invariant checks against the committed raw data (schema, no null emcees,
multi-part `id`/`url` are lists, the COVID window is masked before imputation and
fully dated after it, metadata `event_date_source` is tagged from a known vocabulary,
excluded ids disjoint from final battles, …). See `tests/README`-style headers
in each `tests/test_*.py` for what each file covers.
