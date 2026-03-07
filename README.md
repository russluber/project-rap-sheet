# Project Rap Sheet

A reproducible data pipeline for cleaning and organizing FlipTop rap battle data.

## Overview

This project builds a clean battle-level dataset from two raw sources:

1. `youtube_videos.json`  
   Raw YouTube upload metadata collected from the [FlipTop channel](https://www.youtube.com/@fliptopbattles).

2. `matchup_events_metadata_with_ids.csv`  
   Raw event and matchup metadata scraped from the [FlipTop website](https://www.fliptop.com.ph/videos/battle).

The main output is a cleaned `df_battles` table with one row per battle, including:

- title
- emcee names
- cleaned matchup string
- upload date
- event name
- event date
- event location
- duration information
- YouTube URL data

## Project Structure

```
project-rap-sheet/
├── README.md
├── .gitignore
├── data/
|   ├── README.md
│   ├── raw/
│   │   ├── youtube_videos.json
│   │   └── matchup_events_metadata.csv
│   ├── processed/
│   │   └── df_battles.json
│   └── secret/
│       └── secret.json
├── fliptop/
|   ├── README.md
│   ├── __init__.py
│   ├── data_cleaning.py
│   └── rename_map.py
├── notebooks/
|   ├── README.md
│   └── eda.ipynb
└── scripts/
    ├── fetch_youtube_channel_uploads.py
    └── fetch_events_metadata_from_fliptop_web.py
```