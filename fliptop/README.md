# fliptop

This directory contains the core Python package used by the project.  
It implements the reusable data pipeline that converts raw FlipTop data sources into a clean, battle-level dataset.

The code here is intentionally separated from notebooks so that the data pipeline is:

- modular
- reusable
- easier to maintain
- reproducible outside of notebooks

The main output produced by this package is the **`df_battles`** table.

---

# Package Structure
```
fliptop/
├── __init__.py         # package init + shared data-dir paths
├── battles.py    # raw sources -> df_battles pipeline
├── rename_map.py       # loads/validates the alias map from data/emcee_aliases.csv
├── structures.py       # structures derived from df_battles (emcee table + battle network)
├── refresh.py          # fliptop-refresh CLI: rebuild (and optionally re-fetch) the datasets
├── annotations.py      # battle-results store (winner/judging/notes) + helpers
└── annotate.py         # fliptop-annotate CLI: interactively record battle results
```


---

# Module Overview

## `battles.py`

This module contains the full data pipeline used to construct the final dataset.

It converts raw data from:

- `youtube_videos.json`
- `matchup_events_metadata.csv`

into a cleaned **battle-level dataset** where each row represents one FlipTop battle.

### Main Pipeline Stages

The pipeline follows three main stages:

### 1. Clean YouTube Upload Data

Raw YouTube metadata is processed to produce a table of **1v1 battle uploads**.

Steps include:
- cleaning video titles
- parsing upload dates
- parsing video durations
- converting engagement metrics to numeric values
- filtering non-battle uploads
- extracting emcee names from titles
- canonicalizing emcee names using a rename map

The result is a dataset of likely **1v1 battle videos**.

---

### 2. Attach Event Metadata

Event metadata scraped from the FlipTop website is merged onto the YouTube data.

This stage:
- parses event descriptions
- extracts event dates
- cleans event location strings
- joins event metadata to videos using YouTube video IDs

For newer battles where event metadata is missing from the site scrape, the pipeline attempts to recover metadata directly from the **YouTube video description**.

---

### 3. Consolidate Multi-Part Battles

Some battles were uploaded in multiple parts (e.g. `pt. 1`, `pt. 2`).

These uploads are consolidated into a **single battle row** by:
- grouping parts using the base title
- combining video IDs and URLs
- summing durations
- retaining consistent metadata

---

### Final Output

The final dataset (`df_battles`) contains columns such as:
- `id`
- `title`
- `description`
- `upload_date`
- `duration_seconds`
- `duration_hms`
- `emcee1`
- `emcee2`
- `matchup`
- `event_name`
- `event_date`
- `event_location`
- `url`

Each row represents **one battle**.

---

## `structures.py`

Reusable structures **derived from `df_battles`**. These shape the battle table
into analysis-ready form but do not perform analysis themselves — that lives in
notebooks. Two structures live here today (a per-emcee career table for survival
analysis would be a natural third).

**Emcee table** — every distinct name across `emcee1`/`emcee2`, sorted, with a
stable 1-based `emcee_id`. Written to `data/processed/emcees.csv`.

**Battle network** — an undirected weighted graph: nodes are emcees (with a
`battle_count` attribute), edges mean two emcees battled (with a `weight` equal
to how many times).

Typical usage:

```python
from fliptop.structures import build_emcees_table, write_emcees_table, build_battle_network

df_emcees = build_emcees_table(df_battles)
write_emcees_table(df_battles, "data/processed/emcees.csv")
G = build_battle_network(df_battles)
```

---

## `rename_map.py`

Loads and validates the **canonical emcee name mapping** used by the pipeline to
normalize the `emcee1` / `emcee2` columns.

The mapping itself is hand-maintained reference data in a CSV (one row per
alias), so you can edit it like the project's other data:

```
data/emcee_aliases.csv
columns: alias,canonical
```

`load_rename_map()` reads that file into an `alias -> canonical` dict and
validates it:

- skips blank rows and no-op self-maps; de-duplicates identical rows;
- **raises** if an alias maps to two different canonicals (a real data error);
- **resolves alias chains transitively** with cycle detection — e.g. if the file
  has `Ghostly -> Goriong Talas` and you later add `Spade -> Ghostly`, the result
  is `Spade -> Goriong Talas` regardless of row order. (`df.replace` is a single
  pass and would otherwise leave `Spade -> Ghostly`.)

To add an alias, append a row to `data/emcee_aliases.csv`; the next build picks
it up.

---

## `refresh.py`

Implements the **`fliptop-refresh`** command (registered as a console script in
`pyproject.toml`), the one-command way to regenerate the processed datasets.

```bash
fliptop-refresh            # rebuild df_battles.json + emcees.csv from existing raw data
fliptop-refresh --fetch    # re-fetch raw data (YouTube + web) first, then rebuild
```

- `rebuild_processed()` builds `df_battles` once and writes both processed
  outputs from that single frame.
- `fetch_raw()` (used by `--fetch`) runs the two collection scripts in
  `scripts/` as subprocesses.

This is the recommended entry point; the Python API below is for when you want
the table in memory or finer control.

---

## `annotations.py` & `annotate.py`

Manually-collected battle results are stored separately from `df_battles`, in
an append-only CSV keyed by battle `id`:

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

The recorded tally is always the **final (post-overtime)** result. Panel size
varies (5 / 7 / 9 judges) and is just the sum of the vote columns. Everything is
stored as text with explicit markers (`NA` where not applicable, `none` for
empty notes) so the CSV has **no blank cells**; convert the vote columns with
`pd.to_numeric` for analysis.

`annotations.py` holds the store helpers:
- `load_results()` / `save_results()` — read/write the CSV.
- `pending_battles(df_battles)` — battles not yet annotated (newest first).
- `merge_results(df_battles)` — left-join results onto df_battles **on demand**
  for analysis. `df_battles.json` itself stays clean (no partially-filled
  result columns); you opt into the join when you want it.
- `battle_key()` — the canonical scalar key for a battle (first id for
  consolidated multi-part battles).
- `migrate_from_xlsx()` — one-time import of the legacy `battle_winners_review.xlsx`.

`annotate.py` implements the **`fliptop-annotate`** console script: an
interactive tool that walks through un-annotated battles, lets you pick the
winner by `1`/`2`, validates input, and writes after every entry (resumable —
quit any time and it resumes where you left off).

```bash
fliptop-annotate                 # go through all pending battles
fliptop-annotate --limit 20      # do up to 20, then stop
fliptop-annotate --event Ahon    # only battles whose event matches "Ahon"
fliptop-annotate --open          # also open each battle's URL in the browser
```

---

## `__init__.py`

Initializes the `fliptop` package.

This file allows modules within the folder to be imported as:

```python
from fliptop.battles import build_df_battles
```

## Typical Usage

> For a plain dataset refresh, prefer the `fliptop-refresh` command above. The
> Python API here is for working with the table in memory or building it as part
> of other code.

The package uses shared path constants from `fliptop.__init__`, so you don't
have to hard-code `data/` paths.

Build the dataset in memory:
```python
from fliptop import RAW_DATA_DIR
from fliptop.battles import build_df_battles

df_battles = build_df_battles(raw_dir=RAW_DATA_DIR)
```

Build and write the dataset to disk. The project's committed output is
newline-delimited JSON, so pass `fmt="json"` (the function also supports
`fmt="csv"`):
```python
from fliptop import RAW_DATA_DIR, PROCESSED_DATA_DIR
from fliptop.battles import write_df_battles

write_df_battles(
    out_path=PROCESSED_DATA_DIR / "df_battles.json",
    raw_dir=RAW_DATA_DIR,
    fmt="json",
)
```

Reload the written dataset (note `lines=True` for the JSON output):
```python
import pandas as pd

df_battles = pd.read_json("data/processed/df_battles.json", lines=True)
```
