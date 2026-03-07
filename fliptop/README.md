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
├── init.py
├── data_cleaning.py
└── rename_map.py
```


---

# Module Overview

## `data_cleaning.py`

This module contains the full data pipeline used to construct the final dataset.

It converts raw data from:

- `youtube_videos.json`
- `matchup_events_metadata_with_ids.csv`

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

## `rename_map.py`

This module contains the **canonical emcee rename map**.

FlipTop emcees often appear in YouTube titles under different aliases or formatting variations.  
The rename map standardizes these names so that the dataset uses **consistent canonical emcee names**.

This mapping is used during the pipeline to normalize the `emcee1` and `emcee2` columns.

---

## `__init__.py`

Initializes the `fliptop` package.

This file allows modules within the folder to be imported as:

```python
from fliptop.data_cleaning import build_df_battles
```

## Typical Usage

Build the dataset in memory:
```
from fliptop.data_cleaning import build_df_battles

df_battles = build_df_battles(raw_dir="data/raw")
```

Build and write the dataset to disk:
```
from fliptop.data_cleaning import write_df_battles

write_df_battles(
    out_path="data/processed/df_battles.csv",
    raw_dir="data/raw"
)
```

or

```
from fliptop.data_cleaning import write_df_battles

write_df_battles(
    out_path="data/processed/df_battles.csv",
    raw_dir="data/raw"
)
```