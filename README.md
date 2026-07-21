# Project Rap Sheet

A reproducible pipeline for cleaning and organizing [FlipTop](https://www.fliptop.com.ph/about) rap battle data.

## Overview

The first objective of this project is to create a database for FlipTop rap battles. To this end, this project builds a clean battle-level dataset from three raw sources:

1. `youtube_videos.json` - raw YouTube upload metadata collected from the [FlipTop channel](https://www.youtube.com/@fliptopbattles).

2. `matchup_events_metadata.csv` - raw event and matchup metadata scraped from the [FlipTop website](https://www.fliptop.com.ph/videos/battle).

3. `versetracker_event_dates.csv` - raw event and event date data scraped from [Verse Tracker](https://versetracker.com/battles/fliptop).

The main output is a cleaned, result-enriched `ft_battles` table with one row
per battle. It is built from the battle metadata plus the hand-collected result
store in `data/annotations/battle_results.csv`, and includes:


| Variable | Type | Description |
| ------------- | ------------- | ----------- |
| `id` | string | Scalar battle key / YouTube video ID. For multi-part uploads, this is the first part ID. |
| `title` | string | The title of the YouTube video (with any `pt. N` suffix stripped) |
| `upload_date` | datetime | When the video of the battle was uploaded to YouTube (earliest part for multi-part battles) |
| `duration_seconds` | number | Duration of the battle's video in seconds (summed across parts) |
| `emcee1` | string | Name of the first emcee in the battle (canonicalized) |
| `emcee2` | string | Name of the second emcee in the battle (canonicalized) |
| `matchup` | string | Cleaned and standardized `emcee1 vs emcee2` |
| `event_name` | string | Name of the FlipTop event the battle took place in (standardized — a `(Day N)` suffix is stripped) |
| `event_date` | datetime | The day the battle actually took place (for multi-day events, the specific day). COVID-era dates are imputed from VerseTracker — see note below. |
| `event_location` | string | Location of where the battle took place |
| `battle_type` | string | `judged` or `promo` |
| `winner` | string | Winning emcee, or `NA` for promos and judged draws |
| `votes_winner` | string | Judges voting for the winner, or `NA` |
| `votes_loser` | string | Judges voting for the loser, or `NA` |
| `url`| string or list[string] | Link to the battle. A list of URLs for multi-part uploads. This is intentionally the final field in the public schema. |

The richer internal metadata table, available from
`fliptop.build_battle_metadata`, keeps audit/provenance fields such as
`description`, `duration_hms`, and `event_date_source`.
Those provenance/debug fields are intentionally excluded from `ft_battles`,
which is the standalone final analysis output.


> **Note on COVID-era events.** FlipTop obfuscated the real dates and locations
> of events held during the pandemic (roughly mid-2020 to early-2022), so the
> YouTube/website metadata for those battles carries placeholder values. The
> pipeline blanks those out, then **imputes** the real dates from
> [VerseTracker](https://versetracker.com/battles/fliptop) (a third-party FlipTop
> database) via [`scripts/fetch_versetracker_event_dates.py`](scripts/) and the
> date-imputation step in the metadata build. These imputed dates are accurate to within
> ~days (VerseTracker appears to use the event flyer-post date for some events),
> so they're flagged in the metadata table's `event_date_source` column (`versetracker`). See the
> [imputation notebook journal](notebooks/README.md#covid-era-event-dates--resolved).

The second objective is to analyze data about FlipTop rap battles. 

In particular, this project aims to model emcee career histories and build a FlipTop rap battle network.

## Project Structure

```
project-rap-sheet/
├── data/
|   ├── README.md
│   ├── emcee_aliases.csv
│   ├── raw/
│   │   ├── youtube_videos.json
│   │   ├── matchup_events_metadata.csv
│   │   └── versetracker_event_dates.csv
│   ├── overrides/
│   │   ├── event_locations.csv
│   │   ├── event_location_patterns.csv
│   │   ├── location_aliases.csv
│   │   ├── event_dates.csv
│   │   ├── manual_matchups.csv
│   │   └── upload_decisions.csv
│   ├── rules/
│   │   ├── title_exclusions.csv
│   │   └── event_exclusions.csv
│   ├── processed/
│   │   ├── ft_battles.json
│   │   ├── battle_participants.csv
│   │   ├── emcees.csv
│   │   └── release_manifest.json
│   ├── annotations/
│   │   └── battle_results.csv
│   ├── debug/                  # regenerated, git-ignored audit/review files
│   └── secret/
│       └── secret.json
├── fliptop/
│   ├── README.md
│   ├── __init__.py
│   ├── inputs.py
│   ├── pipeline.py
│   ├── battles.py
│   ├── uploads.py
│   ├── events.py
│   ├── publish.py
│   ├── release.py
│   ├── raw_snapshot.py
│   ├── contracts.py
│   ├── integrity.py
│   ├── verify_release.py
│   ├── io.py
│   ├── lineage.py
│   ├── rename_map.py
│   ├── overrides.py
│   ├── rules.py
│   ├── structures.py
│   ├── validate.py
│   ├── annotations.py
│   ├── annotate.py
│   ├── spotcheck.py
│   └── refresh.py
├── notebooks/
│   ├── README.md
│   ├── wrangling.ipynb
│   └── imputation.ipynb
├── scripts/
│   ├── README.md
│   ├── fetch_youtube_channel_uploads.py
│   ├── fetch_events_metadata_from_fliptop_web.py
│   └── fetch_versetracker_event_dates.py
├── tests/
├── docs/
│   └── workflows.md
├── .github/workflows/ci.yml
├── README.md
├── LICENSE
├── pyproject.toml
├── uv.lock
├── .python-version
└── .gitignore
```

## Setup

`uv` is the canonical environment manager for this project. The repository pins
Python 3.12 in `.python-version` and locks the resolved dependencies in
`uv.lock`.

```bash
uv sync --locked
```

That creates a project-local `.venv/`, installs the `fliptop` package in
editable mode, and includes the default developer tooling (`pytest` and
`ruff`). Run project commands through `uv run` so they use the locked
environment:

```bash
uv run python --version
uv run fliptop-refresh
```

For notebooks and analysis packages:

```bash
uv sync --locked --extra analysis
uv run python -m ipykernel install --user --name project-rap-sheet --display-name "Project Rap Sheet"
```

## Typical Workflow

For a routine update that fetches new battles, annotates them, and publishes a
new processed release:

```powershell
# Fetch new raw data and attempt a rebuild.
uv run fliptop-refresh --fetch --events-since 2026

# Record results for the new battles.
uv run fliptop-annotate

# Rebuild again so the new annotations reach the processed tables and manifest.
uv run fliptop-refresh

# Verify the complete release before committing it.
uv run fliptop-verify-release

# Review and commit the raw inputs, annotations, processed outputs, and manifest.
git status
git diff --stat

# Add, commit, and push
git add .
git commit -m "commit details"
git push
```

The first refresh may report that the release is blocked when newly fetched
battles still need annotations. This is expected: it leaves the previous
processed release intact and writes the exact to-do list to
`data/debug/missing_results.csv`. After annotation, the second refresh is
required to publish updated processed tables and a matching
`data/processed/release_manifest.json`.

Run `fliptop-verify-release` **before** committing. It is an offline, read-only
integrity check that compares the current release inputs and outputs with the
hashes, canonical byte sizes, and row counts recorded in the release manifest;
it does not require a clean or committed working tree. If it reports hash or
byte-size mismatches after annotation, rerun `fliptop-refresh` rather than
committing the mismatched files.

For matchup review, overrides, and other less common cases, see the
[detailed maintainer workflow](docs/workflows.md).

## Development

Run the checks that CI runs:

```bash
uv run pytest -q --basetemp .pytest-tmp
uv run ruff check .
uv run fliptop-verify-release
uv lock --check
```

Use `uv run ruff check --fix .` when you intentionally want Ruff to apply safe
automatic fixes. Every code or data push and pull request to `main` runs the
lint, test, release-integrity, and lockfile gates on Python 3.12 via GitHub
Actions (see `.github/workflows/ci.yml`). Documentation-only changes skip CI.

Optionally, install the git hooks so lint runs before each commit:

```bash
uv tool install pre-commit
pre-commit install
```

## Usage

The whole pipeline is wrapped in a single command, `fliptop-refresh`:

```bash
uv run fliptop-refresh                        # rebuild processed outputs from existing raw data
uv run fliptop-refresh --fetch                # fetch fresh raw data (YouTube + web) first, then rebuild
uv run fliptop-refresh --fetch --events-since 2026   # only re-scrape recent events (faster), then rebuild
uv run fliptop-refresh --no-audit             # rebuild processed outputs without local audit files
uv run fliptop-verify-release                 # verify committed inputs and processed outputs
```

- The default (no flags) is fast and needs no network or API key. The three data
  tables are deterministic for a fixed input snapshot; the release manifest
  deliberately adds the build time and pipeline commit. The command loads every
  raw table, rule, override, alias, reference date, and
  annotation exactly once into a `PipelineInputs` snapshot. It then builds a
  candidate in memory, writes the review files, and only
  then replaces `data/processed/ft_battles.json`,
  `data/processed/battle_participants.csv`, and `data/processed/emcees.csv`
  together with `data/processed/release_manifest.json`
  from the raw files already in `data/raw/` plus
  `data/annotations/battle_results.csv`, and writes the local audit files in
  `data/debug/`. If the candidate has missing results, unresolved uploads, or
  invalid data, the command exits without changing any processed file.
- Before cleaning starts, shared table contracts check the raw files and every
  hand-maintained CSV for the expected columns, types, keys, and allowed values.
  The same checks run again at the major in-memory pipeline boundaries. A
  contract failure names the source file or exact stage that broke, and no
  processed file is changed.
- `--fetch` first runs the two collection scripts in `scripts/` to refresh the
  raw data (this needs a YouTube API key — see `data/README.md`), then rebuilds.
  Both collectors write into a temporary snapshot first. The snapshot is
  contract-validated and all raw files are promoted together; a collection,
  validation, or publication failure leaves the previous raw snapshot intact.
  Override the channel or scrape years with `--channel`, `--start`, `--end`.
- `--fetch` re-scrapes the whole FlipTop site (2010 → now) by default;
  `--events-since YEAR` instead scrapes only recent events and **merges** them
  into the existing data — much faster for routine top-ups. See
  `scripts/README.md` for the trade-off.
- The audit step writes regenerated local debug files:
  `data/debug/filtered_out.csv`, `data/debug/upload_lineage.csv`,
  `data/debug/manual_matchup_needed.csv`, `data/debug/pipeline_summary.csv`,
  `data/debug/pipeline_stage_drops.csv`, `data/debug/missing_results.csv`,
  `data/debug/release_blockers.txt`, `data/debug/release_changes.csv`,
  `data/debug/release_changes_summary.txt`, and `data/debug/run_manifest.json`.
  The release files tell you what needs human attention, what would change from
  the current published dataset, and which exact inputs produced the run. The
  manifest also records the version of every active table contract. The
  lineage file has one row per
  raw YouTube upload and records whether each upload was included, excluded,
  folded into a multi-part battle, or held for manual matchup resolution.
  `pipeline_summary.csv` gives the row counts stage by stage, while
  `pipeline_stage_drops.csv` lists the exact ids exiting at filter/manual-review
  stages. Use `--no-audit` only when you intentionally want to skip these local
  debug outputs. The build and every audit file share one `PipelineRun`, so the
  filters execute once and the audit describes the exact rows that were built.
  That run retains its `PipelineInputs`, so later candidate, audit, and manifest
  steps cannot silently reread a changed file midway through the refresh.
- A successful candidate is written to a temporary bundle and reloaded before
  publication. If any processed file fails while being replaced,
  the three old tables and their release manifest are restored together. The
  manifest records cross-platform input/output hashes, canonical byte sizes, row
  counts, contract versions, and the pipeline commit;
  `fliptop-verify-release` checks it offline.

Under the hood, optional fetching transactionally refreshes the YouTube and
FlipTop website sources. The build then loads one input snapshot, executes the
cleaning stages once, validates a complete candidate, and transactionally
publishes the three tables plus their release manifest. You can also drive the
collection and in-memory APIs directly; see `scripts/README.md` and
`fliptop/README.md`. The notebooks are exploratory consumers of the package,
not a second implementation of the pipeline.

For the conversational maintainer routine to catch up after a few weeks of new
uploads, see [`docs/workflows.md`](docs/workflows.md).

### Recording battle results

Wins, judged draws, and promos are collected by hand in an id-keyed store
(`data/annotations/battle_results.csv`). `fliptop-refresh` validates that store
against the battle metadata and joins the core result fields into the published
`ft_battles.json`. Use the interactive tool to record results:

```bash
uv run fliptop-annotate            # walk through battles that aren't annotated yet
uv run fliptop-annotate --limit 20 # do a batch of 20
```

It is incremental and resumable. After fetching new raw data you only annotate
*new* battles, and quitting mid-session loses nothing. At the result prompt, `d`
records a judged draw and `p` records a promo with no judging. The final refresh
will refuse to publish if any battle is missing a result row.
See `fliptop/README.md` for details.

### Spot-checking the published output

`fliptop-spotcheck` prints a fresh random sample from
`data/processed/ft_battles.json` for quick manual inspection. It is read-only:
the links are displayed for you to open yourself.

```bash
uv run fliptop-spotcheck                         # display 5 battles
uv run fliptop-spotcheck 12                      # display 12 battles
uv run fliptop-spotcheck 5 --path candidate.json # inspect another JSON-lines file
```
