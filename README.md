# Project Rap Sheet

A reproducible pipeline for cleaning and organizing FlipTop rap battle data.

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
| `url`| string | Link to the battle. A list of URLs for multi-part uploads. |
| `battle_type` | string | `judged` or `promo` |
| `winner` | string | Winning emcee, or `NA` for promos and judged draws |
| `votes_winner` | string | Judges voting for the winner, or `NA` |
| `votes_loser` | string | Judges voting for the loser, or `NA` |

The richer internal metadata table, available from
`fliptop.build_battle_metadata`, keeps audit/provenance fields such as
`description`, `duration_hms`, and `event_date_source`.


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
│   │   └── event_dates.csv
│   ├── processed/
│   │   ├── ft_battles.json
│   │   └── emcees.csv
│   ├── annotations/
│   │   └── battle_results.csv
│   └── secret/
│       └── secret.json
├── fliptop/
|   ├── README.md
│   ├── __init__.py
│   ├── battles.py
│   ├── rename_map.py
│   ├── overrides.py
│   ├── structures.py
│   ├── refresh.py
│   ├── annotations.py
│   └── annotate.py
├── notebooks/
|   ├── README.md
│   └── wrangling.ipynb
├── scripts/
|   ├── README.md
|   ├── fetch_youtube_channel_uploads.py
|   ├── fetch_events_metadata_from_fliptop_web.py
|   └── fetch_versetracker_event_dates.py
├── tests/
├── README.md
├── LICENSE
├── pyproject.toml
├── environment.yml
└── .gitignore
```

## Setup

The project ships a conda environment that installs all dependencies and the
`fliptop` package itself (in editable mode):

```bash
conda env create -f environment.yml
conda activate fliptop-analysis
```

Or, into an existing Python (3.11+) environment:

```bash
pip install -e .              # core pipeline only
pip install -e ".[analysis]"  # + notebook/analysis stack (plotting, lifelines, …)
```

## Development

Install the developer tooling (tests + linter) and run the checks that CI runs:

```bash
pip install -e ".[dev]"   # pytest + ruff

pytest                    # run the test suite
ruff check .              # lint
ruff check --fix .        # lint and auto-fix what's safe
```

Every push and pull request to `main` runs the same lint and test suite on
Python 3.11 and 3.12 via GitHub Actions (see `.github/workflows/ci.yml`).

Optionally, install the git hooks so lint runs before each commit:

```bash
pip install pre-commit
pre-commit install
```

## Usage

The whole pipeline is wrapped in a single command, `fliptop-refresh`:

```bash
fliptop-refresh                        # rebuild processed outputs from existing raw data
fliptop-refresh --fetch                # fetch fresh raw data (YouTube + web) first, then rebuild
fliptop-refresh --fetch --events-since 2025   # only re-scrape recent events (faster), then rebuild
```

- The default (no flags) is fast, deterministic, and needs no network or API
  key: it rebuilds `data/processed/ft_battles.json` and `data/processed/emcees.csv`
  from the raw files already in `data/raw/` plus
  `data/annotations/battle_results.csv`.
- `--fetch` first runs the two collection scripts in `scripts/` to refresh the
  raw data (this needs a YouTube API key — see `data/README.md`), then rebuilds.
  Override the channel or scrape years with `--channel`, `--start`, `--end`.
- `--fetch` re-scrapes the whole FlipTop site (2010 → now) by default;
  `--events-since YEAR` instead scrapes only recent events and **merges** them
  into the existing data — much faster for routine top-ups. See
  `scripts/README.md` for the trade-off.

Under the hood the command runs three stages — fetch YouTube uploads, scrape
FlipTop event metadata, then build the cleaned tables. You can also drive these
stages directly; see `scripts/README.md` (collection) and `fliptop/README.md`
(building in Python). The `notebooks/wrangling.ipynb` notebook walks through the
build interactively.

### Recording battle results

Wins, judged draws, and promos are collected by hand in an id-keyed store
(`data/annotations/battle_results.csv`). `fliptop-refresh` validates that store
against the battle metadata and joins the core result fields into the published
`ft_battles.json`. Use the interactive tool to record results:

```bash
fliptop-annotate            # walk through battles that aren't annotated yet
fliptop-annotate --limit 20 # do a batch of 20
```

It is incremental and resumable. After fetching new raw data you only annotate
*new* battles, and quitting mid-session loses nothing. At the result prompt, `d`
records a judged draw and `p` records a promo with no judging. The final refresh
will refuse to publish if any battle is missing a result row.
See `fliptop/README.md` for details.
