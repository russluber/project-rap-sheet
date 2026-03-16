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
| `id` | string | Unique identifier for the battle |
| `title` | string | The title of the YouTube video |
| `description` | string | The text description box of the YouTube video |
| `upload_date` | datetime | Date of when the video of the battle was uploaded to YouTube |
| `duration_seconds` | datetime | Duration of the battle's video in seconds |
| `duration_hms` | datetime | Duration of the battle's video in hours, minutes, seconds |
| `emcee1` | string | Name of the first emcee in the battle |
| `emcee2` | string | Name of the second emcee in the battle |
| `matchup` | string | Cleaned and standardized `emcee1` vs `emcee2` |
| `event_name` | string | Name of the FlipTop event the battle took place in |
| `event_date` | datetime | Date of when the FlipTop event took place |
| `event_location` | string | Location of where the battle took place |
| `url`| string | Link to the battle |


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
│   └── eda.ipynb
├── scripts/
|   ├── fetch_youtube_channel_uploads.py
|   └── fetch_events_metadata_from_fliptop_web.py
├── README.md
├── LICENSE
├── environment.yml
└── .gitignore
```