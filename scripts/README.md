# Scripts

Standalone **data-collection** scripts — the "Extract" step of the project.
They reach out to the network (the YouTube Data API, the FlipTop website, and
VerseTracker) and populate [`data/raw/`](../data/raw/) with the sources
everything else is built from.

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
- [`fetch_versetracker_event_dates.py`](#fetch_versetracker_event_datespy)
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

By default `--fetch` does a **full** website re-scrape (2010 → now), which is
slow. For routine updates use the **incremental** form, which only scrapes
recent years and *merges* them into the existing events CSV:

```bash
fliptop-refresh --fetch --events-since 2025   # scrape just 2025→now, merge, rebuild
```

(The YouTube fetch is always incremental — it only pulls videos you don't have
yet — so the `--events-since` flag only changes the website scrape.)

Run a script **directly** only when you want finer control — e.g. re-scraping a
single year, pointing at a different channel, or writing to a scratch path.

The end-to-end flow they fit into:

1. Fetch raw data with the scripts here (or `fliptop-refresh --fetch`).
2. Raw files land in [`data/raw/`](../data/raw/).
3. The pipeline in [`fliptop/battles.py`](../fliptop/battles.py) cleans those
   files into [`data/processed/df_battles.json`](../data/processed/) (the rebuild
   step `fliptop-refresh` runs by default).

**`fetch_versetracker_event_dates.py` is the exception** — it is *not* bundled
into `fliptop-refresh --fetch`. The COVID-era event dates it recovers are
effectively static, so you run it **by hand, once**, to (re)build the reference
file, then commit that file. After that the build just reads the CSV. Re-run it
only if new quarantine-era battles surface (i.e. `event_date` is `NaT` again).

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
| `--merge` | | off | **upsert** scraped rows into the existing CSV (by `video_id`) instead of overwriting it |
| `--skip-known` | | off | skip event pages whose name is already in the CSV (past years only; current year always re-scraped). Requires `--merge` |

```bash
# full overwrite (clean, reproducible — the default)
python scripts/fetch_events_metadata_from_fliptop_web.py --start 2010 --end 2026

# incremental — only recent years, merged into the existing CSV
python scripts/fetch_events_metadata_from_fliptop_web.py \
    --start 2025 --end 2026 --merge --skip-known
```

**Overwrite vs. merge.** The default overwrites the CSV with exactly what the
scrape found — a clean, reproducible full rebuild, but slow over all years, and
if the site is down mid-run it can drop events you'd already captured. `--merge`
upserts instead (keyed by `video_id`), so events outside the scraped range
survive untouched and a narrowed `--start` is safe. The trade-off: merged data
is path-dependent and stale rows (a matchup removed from a page) linger — so run
a plain full overwrite periodically to reconcile. `fliptop-refresh
--events-since YEAR` bundles `--start YEAR --merge --skip-known` for you.

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

## `fetch_versetracker_event_dates.py`

Recovers the **event dates the pipeline can't get elsewhere** — the COVID-era
("quarantine") events, whose real dates FlipTop obfuscated — by scraping
[VerseTracker](https://versetracker.com/battles/fliptop), a third-party FlipTop
battle database. Output feeds the date imputation in
[`fliptop.battles`](../fliptop/battles.py) (see the
[fliptop README](../fliptop/README.md#covid-era-date-imputation-versetracker)).

**How it works.** Each event has a VerseTracker page at
`versetracker.com/events/fliptop-<slug>`, where the slug is the event name
lowercased with non-alphanumerics turned to hyphens (`Ahon 12` →
`fliptop-ahon-12`). The script builds that URL per event, fetches the page, and
reads the single date out of `div.event-date` (e.g. `December 8, 2021` →
`2021-12-08`). By **default** it targets exactly the events whose `event_date` is
currently `NaT` (it builds `df_battles` with imputation off to find them); pass
`--events` to scrape an explicit list instead. A 404 or unparseable date is
logged as a `[warn]` and skipped.

> VerseTracker lists only the **first day** of a multi-day event. The pipeline,
> not this script, applies the per-day offset (`Day N = first day + (N−1)`) using
> the `(Day N)` suffix from the FlipTop scrape — so this CSV holds one first-day
> date per event.

No API key needed — plain HTML scraping with `requests` + `BeautifulSoup`.

| flag | required | default | meaning |
| ---- | -------- | ------- | ------- |
| `--events` | | NaT events in `df_battles` | explicit event names to scrape, e.g. `--events "Ahon 12" "Zoning 10"` |
| `--output` | | `data/raw/versetracker_event_dates.csv` | where to write the CSV |
| `--base` | | `https://versetracker.com` | site root |
| `--user-agent` | | a project UA string | `User-Agent` header to send |
| `--sleep` | | `0.6` | seconds between event-page requests |
| `--retries` | | `2` | retries per request (on top of the first try) |
| `--request-sleep` | | `0.7` | base backoff inside the retry loop |
| `--timeout` | | `30` | per-request timeout (seconds) |
| `--quiet` | | off | reduce logging |

```bash
# scrape every event currently missing an event_date (the usual case)
python scripts/fetch_versetracker_event_dates.py

# or scrape a specific set
python scripts/fetch_versetracker_event_dates.py --events "Ahon 12" "Zoning 10"
```

**Output** → [`data/raw/versetracker_event_dates.csv`](../data/raw/):

| column | example | notes |
| ------ | ------- | ----- |
| `event_name` | `Ahon 12` | base name, **no** `(Day N)` suffix |
| `event_date` | `2021-12-08` | ISO first-day date |
| `source_url` | `https://versetracker.com/events/fliptop-ahon-12` | the page it came from (provenance) |

> ⚠️ VerseTracker's date is sometimes a proxy (it appears to use the event
> **flyer-post** date for some events — e.g. Bwelta Balentong 7), so treat these
> as accurate to within days, not exact. They are tagged `versetracker` in
> `df_battles`' `event_date_source` column; override a specific battle by adding
> a row to `data/overrides/event_dates.csv` if you find a better source. This
> scraper also depends on VerseTracker's current HTML (`div.event-date`); if the
> site is redesigned, update `parse_event_date`.

---

## Being a polite scraper

All three scripts are written to go easy on the upstream services, and you should
keep it that way if you tweak them:

- **Incremental / narrow** — the YouTube fetch skips ids it already has; the
  VerseTracker scraper only hits the handful of events still missing a date.
- **Paced** — short `time.sleep` pauses between API pages and event-page
  requests.
- **Resilient** — the web and VerseTracker scrapers retry with backoff and skip a
  bad/missing page (logging a `[warn]`) rather than aborting the whole run.
- **Identifiable** — the scrapers send a descriptive `User-Agent`.
