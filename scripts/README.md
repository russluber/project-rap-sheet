# scripts

This folder contains standalone scripts used to **collect raw data** for the project.

They are responsible for populating the `data/raw/` directory.

These scripts are run **before the data cleaning pipeline** in `fliptop/`.

---

## Scripts

### `fetch_youtube_channel_uploads.py`

Uses the YouTube API to fetch all videos uploaded to the FlipTop channel.

Output:
```
data/raw/youtube_videos.json
```

Contains metadata for each video such as:
- video id
- title
- description
- upload date
- view count
- like count
- comment count
- duration
- tags

---

### `fetch_events_metadata_from_fliptop_web.py`

Scrapes the FlipTop website to collect event and matchup metadata.

Output:
```
data/raw/matchup_events_metadata.csv
```


Contains information such as:

- event name
- event date
- event location
- matchup
- YouTube video id

---

## Typical Workflow

You normally do not need to run these scripts by hand. The `fliptop-refresh`
command orchestrates both of them plus the build step:

```bash
fliptop-refresh --fetch   # runs both scripts below, then rebuilds the processed datasets
```

Run them directly only when you want finer control (for example, scraping a
single year). The end-to-end flow they fit into is:

1. Run the scripts in this folder to download raw data (or `fliptop-refresh --fetch`).
2. Raw files are saved to `data/raw/`.
3. The cleaning pipeline in `fliptop/data_cleaning.py` processes these files to produce the final dataset in `data/processed/` (this is the rebuild step `fliptop-refresh` runs by default).
