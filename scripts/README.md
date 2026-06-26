# Scripts

Standalone **data-collection** scripts — the "Extract" step of the project.
They reach out to the network (the YouTube Data API and the FlipTop website) and
populate [`data/raw/`](../data/raw/) with the two raw sources everything else is
built from.

They run *before* the cleaning pipeline in [`fliptop/`](../fliptop/): the scripts
fetch raw data, then `fliptop-refresh` cleans it into
[`data/processed/`](../data/processed/).

```
scripts/  ──fetch──►  data/raw/  ──build (fliptop)──►  data/processed/
```

---

## Contents

- [When to run these](#when-to-run-these)
- [`fetch_youtube_channel_uploads.py`](#fetch_youtube_channel_uploadspy)
- [`fetch_events_metadata_from_fliptop_web.py`](#fetch_events_metadata_from_fliptop_webpy)
- [Being a polite scraper](#being-a-polite-scraper)

---

## When to run these

You normally **don't** run these by hand. The `fliptop-refresh --fetch` command
([`fliptop/refresh.py`](../fliptop/refresh.py)) runs both of them — with the
right channel id and a `2010 → current year` scrape window — and then rebuilds
the processed datasets in one shot:

```bash
fliptop-refresh --fetch    # fetch both raw sources, then rebuild df_battles + emcees
fliptop-refresh            # rebuild only, from the raw data already on disk (no network)
```

Run a script **directly** only when you want finer control — e.g. re-scraping a
single year, pointing at a different channel, or writing to a scratch path. Both
scripts are **incremental / idempotent**, so re-running them is safe.

The end-to-end flow they fit into:

1. Fetch raw data with the scripts here (or `fliptop-refresh --fetch`).
2. Raw files land in [`data/raw/`](../data/raw/).
3. The pipeline in [`fliptop/battles.py`](../fliptop/battles.py) cleans those
   files into [`data/processed/df_battles.json`](../data/processed/) (the rebuild
   step `fliptop-refresh` runs by default).

---

## `fetch_youtube_channel_uploads.py`

Fetches metadata for **every upload** on a YouTube channel via the
[YouTube Data API v3](https://developers.google.com/youtube/v3) and writes it as
JSON.

**How it works.** It resolves the channel's "uploads" playlist, pages through it
(50 ids per request) to list every video id, then fetches `snippet` +
`contentDetails` + `statistics` for those ids in batches of 50. Ids already
present in the output file are skipped, so a re-run only fetches what's new.

**Requires an API key**, looked up in this order:

1. the `YOUTUBE_API_KEY` environment variable, then
2. a JSON file at [`data/secret/secret.json`](../data/secret/) with a
   `YT_API_KEY` field.

If neither is found the script raises with instructions.

| flag | required | default | meaning |
| ---- | -------- | ------- | ------- |
| `--channel` | ✅ | — | YouTube channel id (FlipTop is `UCBdHwFIE4AJWSa3Wxdu7bAQ`) |
| `--output` | | `data/raw/youtube_videos.json` | where to write the JSON |
| `--secret` | | `data/secret/secret.json` | path to the API-key JSON |

```bash
python scripts/fetch_youtube_channel_uploads.py \
    --channel UCBdHwFIE4AJWSa3Wxdu7bAQ \
    --output data/raw/youtube_videos.json
```

**Output** → [`data/raw/youtube_videos.json`](../data/raw/) — a JSON list with one
object per video:

| field | example | notes |
| ----- | ------- | ----- |
| `id` | `"iReYgDruGEM"` | YouTube video id |
| `title` | `"FlipTop - Hespero vs R-Zone"` | |
| `description` | `"FlipTop presents: Ahon 16 @ …"` | the box text the pipeline later mines for event name/date/location |
| `upload_date` | `"2026-02-19T12:40:15Z"` | ISO-8601 (UTC) |
| `view_count` | `"99300"` | string from the API |
| `duration` | `"PT28M1S"` | ISO-8601 duration |
| `url` | `"https://www.youtube.com/watch?v=…"` | |
| `likeCount` / `commentCount` | `"1488"` / `"444"` | strings from the API |
| `tags` | `["fliptop", …]` | list of strings |

---

## `fetch_events_metadata_from_fliptop_web.py`

Scrapes the FlipTop website's battle pages over a range of years and builds a
**one-row-per-matchup** table that ties each matchup to its YouTube video id.

**How it works.** For each year it loads `…/videos/battle?year=YYYY`, collects
the event-page links, then for each event page extracts the event name, the
description block, and the main matchup grid — pairing each `Emcee A vs Emcee B`
heading with the `data-id` of its embedded YouTube player. Emcee names can be
canonicalized on the way out via an optional rename map.

No API key needed — it's plain HTML scraping with `requests` + `BeautifulSoup`.

| flag | required | default | meaning |
| ---- | -------- | ------- | ------- |
| `--start` | ✅ | — | first year to scrape (inclusive) |
| `--end` | ✅ | — | last year to scrape (inclusive) |
| `--output` | | `data/raw/matchup_events_metadata.csv` | where to write the CSV |
| `--rename-map` | | none | optional JSON of `variant → canonical` emcee names |
| `--base` | | `https://www.fliptop.com.ph` | site root |
| `--user-agent` | | a project UA string | `User-Agent` header to send |
| `--sleep` | | `0.6` | seconds between event-page requests |
| `--retries` | | `2` | retries per request (on top of the first try) |
| `--request-sleep` | | `0.7` | base backoff inside the retry loop |
| `--timeout` | | `30` | per-request timeout (seconds) |
| `--quiet` | | off | reduce logging |

```bash
python scripts/fetch_events_metadata_from_fliptop_web.py --start 2010 --end 2026
```

**Output** → [`data/raw/matchup_events_metadata.csv`](../data/raw/):

| column | example | notes |
| ------ | ------- | ----- |
| `matchup` | `Anygma vs Dirtbag Dan` | `emcee1 vs emcee2` |
| `event_name` | `Tectonics` | |
| `event_description` | `FlipTop presents: Tectonics @ … Dec. 4, 2010. …` | free text; the pipeline parses the date/location out of this |
| `video_id` | `5BiDPaDZHzo` | YouTube id, or empty if the page had none |

> ⚠️ This scraper depends on the FlipTop site's current HTML structure (specific
> CSS classes like `div.row.my-4` and `div.youtube-player[data-id]`). If the site
> is redesigned, the selectors in `parse_event_live` will need updating.

---

## Being a polite scraper

Both scripts are written to go easy on the upstream services, and you should keep
it that way if you tweak them:

- **Incremental** — the YouTube fetch skips ids it already has; re-running adds
  only new videos.
- **Paced** — short `time.sleep` pauses between API pages and event-page
  requests.
- **Resilient** — the web scraper retries with backoff and skips a bad event
  page (logging a `[warn]`) rather than aborting the whole run.
- **Identifiable** — the scraper sends a descriptive `User-Agent`.
