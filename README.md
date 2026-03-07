# Project Rap Sheet

A reproducible data pipeline for cleaning and organizing FlipTop rap battle data.

## Overview

The first objective of this project is to create a database for FlipTop rap batles. To this end, this project builds a clean battle-level dataset from two raw sources:

1. `youtube_videos.json`  
   Raw YouTube upload metadata collected from the [FlipTop channel](https://www.youtube.com/@fliptopbattles).

2. `matchup_events_metadata.csv`  
   Raw event and matchup metadata scraped from the [FlipTop website](https://www.fliptop.com.ph/videos/battle).

The main output is a cleaned `df_battles` table with one row per battle, including:


| Variable name | Description |
| ------------- | ----------- |
| `id` | Unique identifier for the battle as a string |
| `title` | The title of the YouTube video as a string |
| `description` | The text description box of the YouTube video as a string |
| `upload_date` | Date of the video as a datetime object |
| `duration_seconds` | Duration of the battle's video in seconds as a datetime object |
| `duration_hms` | Duration of the battle's video in hours, minutes, seconds as a datetime object |
| `emcee1` | Name of emcee1 as a string |
| `emcee2` | Name of emcee2 as a string |
| `matchup` | Cleaned and standardized `emcee1` vs `emcee2` string |
| `event_name` | Name of FlipTop event the battle took place in as string |
| `event_date` | Date of when the FlipTop event took place as datetime object |
| `event_location` | Location of where the battle took place as a string |
| `url`| Link to the battle |


The second objective is to analyze data about FlipTop rap battles. 

In particular, this project aims to model emcee career histories and build a FlipTop rap battle network.

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
│   │   └── emcees.csv
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