# fliptop

The core Python package for Project Rap Sheet. It implements the reproducible
pipeline that turns raw FlipTop data into a clean, **one-row-per-battle** table
(`df_battles`), plus the helpers that build things from that table, record
battle results, and refresh everything with one command.

The code lives here (not in notebooks) so the pipeline is modular, reusable,
testable, and reproducible. Notebooks *import* this package; they don't
reimplement it.

---

## Contents

- [Architecture at a glance](#architecture-at-a-glance)
- [Package layout](#package-layout)
- [The pipeline: raw → `df_battles`](#the-pipeline-raw--df_battles) (`battles.py`)
- [`df_battles` schema](#df_battles-schema)
- [Auditing what gets filtered out](#auditing-what-gets-filtered-out)
- [Derived structures](#derived-structures) (`structures.py`)
- [Canonical emcee names](#canonical-emcee-names) (`rename_map.py`)
- [Battle results / annotations](#battle-results--annotations) (`annotations.py`, `annotate.py`)
- [Refreshing the datasets](#refreshing-the-datasets) (`refresh.py`)
- [Public API & typical usage](#public-api--typical-usage)
- [Testing](#testing)

---

## Architecture at a glance

Everything flows one way toward `df_battles` as the hub: the pipeline *builds*
it, and other modules *consume* it.

```
 data/raw/youtube_videos.json ─┐
                               ├─► battles.py ──► df_battles ──┬─► structures.py  (emcee table, network)
 data/raw/matchup_events_      ─┘   (+ emcee_aliases.csv)      │
   metadata.csv                                                └─► annotations.py (winner/judging, joined on demand)

 scripts/ (fetch raw) ──► data/raw/        refresh.py orchestrates: fetch (optional) → build → write
                                           annotate.py records battle results into data/annotations/
```

**Design principles**

- **`df_battles` is the hub.** `battles.py` produces the one canonical table;
  `structures.py` and `annotations.py` only read from it. No module downstream
  of `df_battles` feeds back into building it.
- **Logic is separate from CLI.** `battles.py` ↔ `refresh.py` (build ↔ command)
  and `annotations.py` ↔ `annotate.py` (store ↔ command) are deliberate splits.
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
├── battles.py          # the pipeline: raw sources -> df_battles
├── rename_map.py       # loads/validates the alias map from data/emcee_aliases.csv
├── structures.py       # structures derived from df_battles (emcee table + battle network)
├── annotations.py      # battle-results store (winner/judging/notes) + helpers
├── annotate.py         # fliptop-annotate CLI: interactively record battle results
└── refresh.py          # fliptop-refresh CLI: rebuild (and optionally re-fetch) the datasets
```

---

## The pipeline: raw → `df_battles`

`battles.py` is the heart of the package. `build_df_battles(raw_dir=...)` runs
three stages and returns the final table.

```
build_df_battles
  ├─ make_df_1v1_uploads      Stage 1: raw YouTube uploads -> clean 1v1 uploads
  ├─ attach_event_metadata    Stage 2: merge event name / date / location
  └─ finalize_battles         Stage 3: consolidate parts, tidy, override, order
```

Each stage is a thin orchestration over small, single-purpose transforms (most
take a DataFrame and return a new one), which keeps them easy to read and test.

### Stage 1 — clean YouTube uploads → 1v1 uploads

`make_df_1v1_uploads(df_yt)` first runs the shared **`prepare_uploads`** step:

| transform | what it does |
| --------- | ------------ |
| `clean_titles` | trim whitespace, strip wrapping quotes |
| `parse_upload_date` | ISO-8601 → tz-naive `datetime64` |
| `add_duration_columns` | ISO-8601 duration → `duration_seconds` + `duration_hms` |
| `convert_video_metrics_to_numeric` | view/like/comment counts → numeric |
| `copy_yt_title` | preserve the original title (with `pt. N`) as `yt_raw_title` |
| `strip_pt_suffix_from_title` | drop the `pt. N` suffix from the working title |

then the **three filters** that decide what counts as a battle (this is the only
place rows are dropped):

| filter | keeps / drops |
| ------ | ------------- |
| `filter_titles_with_vs` | keep only titles containing the token `vs` |
| `drop_non_battles` | drop titles matching `EXCLUDE_KEYWORDS` (beatbox, tryout, promo, `[LIVE]`, …) — matched as **substrings**, case-insensitive |
| `keep_1v1` | drop multi-emcee formats: `>1 vs`, `/`, `+`, `N on M`, `and … vs … and` |

and finally extracts the matchup:

- `add_matchup_and_split` → `matchup`, `emcee1`, `emcee2` (also strips a trailing
  `- Finals`-style annotation)
- `apply_emcee_rename` → canonicalize names via the alias map (**exact, case-sensitive** match)
- `add_matchup_clean` → `matchup_clean` from the canonical names

### Stage 2 — attach event metadata

`attach_event_metadata(df_1v1, df_events_raw)` cleans the scraped event side
(`split_event_description` → `parse_event_date` → `clean_event_location`), then
joins it onto the uploads by **YouTube video id**. Two special behaviors:

- **COVID-era mask.** Event dates inside `2020-05-01 … 2022-04-27` are set to
  `NaT` on purpose — FlipTop obfuscated those dates, so recording them would be
  worse than leaving them blank. (Recovering the real dates is a known open
  task.)
- **Post-COVID fallback.** For battles after the COVID window whose event date
  is still missing, `fill_metadata_from_yt_description` recovers event name /
  date / location from the YouTube description.

`clean_event_location` also normalizes known messiness: prefers the text after
`@`, fixes the country separator (`City. Philippines` / `Metro Manila
Philippines` → `, Philippines`), collapses doubled words, and applies a few
Davao variants.

### Stage 3 — consolidate, tidy, finalize

`finalize_battles` drops helper columns, renames `matchup_clean → matchup` and
`event_location_clean → event_location`, then:

- **`consolidate_battle_parts`** collapses multi-part uploads (`pt. 1`, `pt. 2`,
  …) into one row: `id` and `url` become **ordered lists**, `duration_seconds`
  is summed, `upload_date` is the earliest part, `duration_hms` is recomputed.
- sorts newest-upload-first,
- applies `apply_manual_event_location_overrides` (hand-fixed locations for a
  handful of events the scrape got wrong, keyed by event name),
- selects and orders the final columns.

---

## `df_battles` schema

| column | type | notes |
| ------ | ---- | ----- |
| `id` | string or list | YouTube video id; a **list** of part ids for multi-part battles |
| `title` | string | cleaned title, `pt. N` suffix removed |
| `description` | string | raw YouTube description (left verbatim) |
| `upload_date` | datetime | earliest part for multi-part battles |
| `duration_seconds` | number | summed across parts |
| `duration_hms` | string | `HH:MM:SS` |
| `emcee1`, `emcee2` | string | canonicalized names |
| `matchup` | string | `emcee1 vs emcee2` |
| `event_name` | string | |
| `event_date` | datetime | **null for COVID-era battles** (intentional) |
| `event_location` | string | |
| `url` | string or list | a **list** for multi-part battles |

> Win/loss data is **not** in `df_battles` — it lives separately and is joined on
> demand (see [Battle results](#battle-results--annotations)).

---

## Auditing what gets filtered out

To verify the filters aren't dropping real battles, `build_excluded_uploads(raw_dir)`
returns every upload the Stage-1 filters removed, tagged with the reason
(`no 'vs' token`, `non-battle keyword`, `not 1v1`) and, for keyword drops, the
matched keyword. It reruns the *exact same* filter functions as the pipeline
(via `prepare_uploads`), so the audit can never drift from real behavior.

```python
from fliptop import RAW_DATA_DIR, build_excluded_uploads, DATA_DIR

excluded = build_excluded_uploads(RAW_DATA_DIR)
excluded.to_csv(DATA_DIR / "debug" / "filtered_out.csv", index=False)
excluded["excluded_reason"].value_counts()
```

---

## Derived structures

`structures.py` holds reusable structures **derived from `df_battles`**. They
shape the table into analysis-ready form but do **not** perform analysis itself —
that belongs in notebooks. (A per-emcee career table for survival analysis would
be a natural addition here.)

- **Emcee table** (`build_emcees_table`) — every distinct name across
  `emcee1`/`emcee2`, sorted, with a stable 1-based `emcee_id`;
  `write_emcees_table` writes it to `data/processed/emcees.csv`.
- **Battle network** (`build_battle_network`) — an undirected weighted graph:
  nodes are emcees (with a `battle_count`), edges mean two emcees battled (with
  `weight` = how many times).

```python
from fliptop.structures import build_emcees_table, build_battle_network

df_emcees = build_emcees_table(df_battles)
G = build_battle_network(df_battles)   # networkx.Graph
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

Who won each battle (and the judges' tally) is collected **by hand** and kept
**separate** from `df_battles`, in an append-only CSV keyed by battle `id`:

```
data/annotations/battle_results.csv
columns: id, winner, judging_status,
         votes_winner, votes_loser, votes_nv, votes_ot, overtime, notes
```

Judging is recorded as explicit, structured fields rather than one ambiguous
string:

| column | meaning |
| ------ | ------- |
| `judging_status` | `scored` \| `no_decision` \| `unknown` |
| `votes_winner` / `votes_loser` | judges voting for the winner / loser |
| `votes_nv` | judges who did not vote (NV) |
| `votes_ot` | judges who voted to go to overtime |
| `overtime` | `yes` \| `no` — did the battle go to an OT round? |
| `notes` | free text, or the literal `none` |

The tally is always the **final (post-overtime)** result. Panel size varies
(5 / 7 / 9 judges) and is just the sum of the vote columns. Everything is stored
as text with explicit markers (`NA` where not applicable, `none` for empty
notes), so the CSV has **no blank cells**; convert the vote columns with
`pd.to_numeric` for analysis.

**`annotations.py`** — the store and its helpers:

- `load_results()` / `save_results()` — read/write the CSV.
- `pending_battles(df_battles)` — battles not yet annotated (newest first).
- `merge_results(df_battles)` — left-join results onto `df_battles` **on demand**.
  `df_battles.json` itself stays clean (no partially-filled result columns); you
  opt into the join only when analyzing.
- `battle_key()` — the canonical scalar key for a battle (the first id for
  consolidated multi-part battles).
- `migrate_from_xlsx()` — one-time import of the legacy `battle_winners_review.xlsx`.

**`annotate.py`** — the `fliptop-annotate` console script: an interactive tool
that walks un-annotated battles, lets you pick the winner by `1`/`2`, validates
input, and **writes after every entry** (crash-safe and resumable — quit any
time and it picks up where you left off).

```bash
fliptop-annotate                 # go through all pending battles
fliptop-annotate --limit 20      # do up to 20, then stop
fliptop-annotate --event Ahon    # only battles whose event matches "Ahon"
fliptop-annotate --open          # also open each battle's URL in the browser
fliptop-annotate --redo "A vs B" # re-annotate an existing battle (fix a mistake)
```

---

## Refreshing the datasets

`refresh.py` implements **`fliptop-refresh`** (a console script), the one-command
way to regenerate the processed datasets.

```bash
fliptop-refresh            # rebuild df_battles.json + emcees.csv from existing raw data
fliptop-refresh --fetch    # re-fetch raw data (YouTube + web) first, then rebuild
```

- `rebuild_processed()` builds `df_battles` once and writes **both** processed
  outputs from that single frame (fast, deterministic, no network).
- `fetch_raw()` (only with `--fetch`) runs the two `scripts/` collectors as
  subprocesses first; this needs a YouTube API key (see `data/README.md`).

This is the recommended way to regenerate data. The Python API below is for when
you want the table in memory or finer control.

---

## Public API & typical usage

The main entry points are re-exported from the package root and imported lazily,
so `import fliptop` stays cheap:

```python
from fliptop import (
    RAW_DATA_DIR, PROCESSED_DATA_DIR, DATA_DIR,   # shared paths
    build_df_battles,        # raw -> df_battles
    build_excluded_uploads,  # audit of dropped uploads
    build_emcees_table,      # df_battles -> emcee table
    write_emcees_table,
    build_battle_network,    # df_battles -> networkx graph
    merge_results,           # join battle results onto df_battles
)
```

Build the dataset in memory:

```python
from fliptop import RAW_DATA_DIR, build_df_battles

df_battles = build_df_battles(raw_dir=RAW_DATA_DIR)
```

Build and write it to disk. The committed output is newline-delimited JSON, so
pass `fmt="json"` (the function also supports `fmt="csv"`):

```python
from fliptop import RAW_DATA_DIR, PROCESSED_DATA_DIR
from fliptop.battles import write_df_battles

write_df_battles(
    out_path=PROCESSED_DATA_DIR / "df_battles.json",
    raw_dir=RAW_DATA_DIR,
    fmt="json",
)
```

Reload the written dataset. It's newline-delimited (`lines=True`), and dates are
stored as epoch-ms, so name the date columns to restore the datetime dtype:

```python
import pandas as pd

df_battles = pd.read_json(
    "data/processed/df_battles.json",
    lines=True,
    convert_dates=["upload_date", "event_date"],
)
```

(Or just call `build_df_battles(...)` again — building in memory keeps the
datetime dtypes and skips the JSON round-trip entirely.)

---

## Testing

The pipeline's transforms are covered by a `pytest` suite at the repo root
(`tests/`). Run it from the activated environment:

```bash
pytest
```

Tests use small hand-built DataFrames for the unit transforms, plus end-to-end
invariant checks against the committed raw data (schema, no null emcees,
multi-part `id`/`url` are lists, `event_date` null across the COVID window,
excluded ids disjoint from final battles, …). See `tests/README`-style headers
in each `tests/test_*.py` for what each file covers.
