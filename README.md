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
│   ├── raw/
│   │   ├── youtube_videos.json
│   │   ├── battle_winners_review.xlsx
│   │   └── matchup_events_metadata.csv
│   ├── processed/
│   │   ├── df_battles.json
│   │   └── emcees.csv
│   └── secret/
│       └── secret.json
├── fliptop/
|   ├── README.md
│   ├── __init__.py
│   ├── data_cleaning.py
│   ├── rename_map.py
│   ├── battle_network.py 
│   └── emcee_table.py 
├── notebooks/
|   ├── README.md
│   └── wrangling.ipynb
├── scripts/
|   ├── README.md
|   ├── fetch_youtube_channel_uploads.py
|   └── fetch_events_metadata_from_fliptop_web.py
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

End to end, the workflow is:

1. **Fetch raw data** (see `scripts/README.md`):
   ```bash
   python scripts/fetch_youtube_channel_uploads.py --channel <CHANNEL_ID> --output data/raw/youtube_videos.json
   python scripts/fetch_events_metadata_from_fliptop_web.py --start 2010 --end 2026
   ```

2. **Build the cleaned dataset** (see `fliptop/README.md`):
   ```python
   from fliptop import RAW_DATA_DIR, PROCESSED_DATA_DIR
   from fliptop.data_cleaning import build_df_battles, write_df_battles

   df_battles = build_df_battles(raw_dir=RAW_DATA_DIR)
   write_df_battles(out_path=PROCESSED_DATA_DIR / "df_battles.json", raw_dir=RAW_DATA_DIR, fmt="json")
   ```

The `notebooks/wrangling.ipynb` notebook walks through this interactively.