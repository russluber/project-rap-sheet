# Project Rap Sheet

A reproducible data pipeline for cleaning and organizing FlipTop rap battle data.

## Overview

The first objective of this project is to create a database for FlipTop rap battles. To this end, this project builds a clean battle-level dataset from two raw sources:

1. `youtube_videos.json`  
   Raw YouTube upload metadata collected from the [FlipTop channel](https://www.youtube.com/@fliptopbattles).

2. `matchup_events_metadata.csv`  
   Raw event and matchup metadata scraped from the [FlipTop website](https://www.fliptop.com.ph/videos/battle).

The main output is a cleaned `df_battles` table with one row per battle, including:


| Variable | Type | Description |
| ------------- | ------------- | ----------- |
| `id` | string | YouTube video ID for the battle. For multi-part uploads (`pt. 1`, `pt. 2`, …) this is a list of the part IDs. |
| `title` | string | The title of the YouTube video (with any `pt. N` suffix stripped) |
| `description` | string | The text description box of the YouTube video |
| `upload_date` | datetime | When the video of the battle was uploaded to YouTube (earliest part for multi-part battles) |
| `duration_seconds` | number | Duration of the battle's video in seconds (summed across parts) |
| `duration_hms` | string | Duration of the battle's video formatted as `HH:MM:SS` |
| `emcee1` | string | Name of the first emcee in the battle (canonicalized) |
| `emcee2` | string | Name of the second emcee in the battle (canonicalized) |
| `matchup` | string | Cleaned and standardized `emcee1 vs emcee2` |
| `event_name` | string | Name of the FlipTop event the battle took place in |
| `event_date` | datetime | When the FlipTop event took place. Missing (`null`) for COVID-era events — see note below. |
| `event_location` | string | Location of where the battle took place |
| `url`| string | Link to the battle. A list of URLs for multi-part uploads. |


> **Note on COVID-era events.** FlipTop obfuscated the real dates and locations
> of events held during the pandemic (roughly mid-2020 to early-2022), and the
> YouTube/website metadata for those battles carries placeholder values. The
> pipeline therefore leaves `event_date` empty for battles in that window rather
> than record dates known to be wrong. Recovering the true dates from external
> sources is a known open task, not a bug.

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
│   │   └── matchup_events_metadata.csv
│   ├── processed/
│   │   ├── df_battles.json
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
|   └── fetch_events_metadata_from_fliptop_web.py
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

## Usage

The whole pipeline is wrapped in a single command, `fliptop-refresh`:

```bash
fliptop-refresh            # rebuild processed outputs from existing raw data
fliptop-refresh --fetch    # fetch fresh raw data (YouTube + web) first, then rebuild
```

- The default (no flags) is fast, deterministic, and needs no network or API
  key: it rebuilds `data/processed/df_battles.json` and `data/processed/emcees.csv`
  from the raw files already in `data/raw/`.
- `--fetch` first runs the two collection scripts in `scripts/` to refresh the
  raw data (this needs a YouTube API key — see `data/README.md`), then rebuilds.
  Override the channel or scrape years with `--channel`, `--start`, `--end`.

Under the hood the command runs three stages — fetch YouTube uploads, scrape
FlipTop event metadata, then build the cleaned tables. You can also drive these
stages directly; see `scripts/README.md` (collection) and `fliptop/README.md`
(building in Python). The `notebooks/wrangling.ipynb` notebook walks through the
build interactively.

### Recording battle results (who won)

Win/loss data is collected by hand and kept **separate** from `df_battles` in an
append-only, id-keyed store (`data/annotations/battle_results.csv`). Use the
interactive tool to record results:

```bash
fliptop-annotate            # walk through battles that aren't annotated yet
fliptop-annotate --limit 20 # do a batch of 20
```

It is incremental and resumable — after a refresh you only annotate the *new*
battles, and quitting mid-session loses nothing. Join the results onto
`df_battles` on demand with `fliptop.annotations.merge_results(df_battles)`;
the published `df_battles.json` is intentionally left without result columns.
See `fliptop/README.md` for details.