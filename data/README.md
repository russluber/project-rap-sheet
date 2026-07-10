# Data

Everything the project reads or writes lives here, organized by **how trustworthy
and how processed** it is — from raw scraped sources, through the clean
pipeline outputs, to the hand-collected annotations layered on top.

```
data/
├── emcee_aliases.csv      # hand-maintained alias → canonical name map
├── raw/                   # original scraped sources (input to the pipeline)
│   ├── youtube_videos.json
│   ├── matchup_events_metadata.csv
│   └── versetracker_event_dates.csv   # COVID-era event dates (date-imputation reference)
├── overrides/             # hand-maintained corrections applied during the build
│   ├── event_locations.csv
│   ├── event_location_patterns.csv
│   ├── location_aliases.csv
│   ├── event_dates.csv
│   └── manual_matchups.csv
├── processed/             # clean tables built by the fliptop package
│   ├── ft_battles.json
│   ├── battle_participants.csv
│   └── emcees.csv
├── annotations/           # hand-collected results, kept separate from processed
│   └── battle_results.csv
└── secret/                # API keys etc. — git-ignored
    └── secret.json
```

**Data flow.** `raw/` is produced by [`scripts/`](../scripts/); `processed/` is
built from `raw/` (plus `emcee_aliases.csv` and the `overrides/` tables) by the
[`fliptop`](../fliptop/) package; `annotations/` is filled in by hand via
`fliptop-annotate`; refresh validates those annotations and joins the core result fields into the processed `ft_battles.json`.

```
scripts/ ─► raw/ ─┐
emcee_aliases.csv ┤
overrides/ ───────┼─► fliptop (build) ─► processed/
                  │                          │
                  ????????????? annotations/ ?(validated join)??
```

---

## Contents

- [`emcee_aliases.csv`](#emcee_aliasescsv)
- [`raw/`](#raw)
- [`overrides/`](#overrides)
- [`processed/`](#processed)
- [`annotations/`](#annotations)
- [`secret/`](#secret)
- [Regenerating everything](#regenerating-everything)
- [Conventions](#conventions)

---

## `emcee_aliases.csv`

Hand-maintained mapping from name **variants** to a single **canonical** emcee
name, used by the pipeline to standardize who's who (so "Akt", "AKT", and an
alias like "1ce Water" all collapse to one person).

| column | example | notes |
| ------ | ------- | ----- |
| `alias` | `1ce Water` | the variant / alias as it appears in a title |
| `canonical` | `J-Blaque` | the single name to standardize it to |

Loaded and validated by
[`fliptop.rename_map.load_rename_map`](../fliptop/rename_map.py); matching is
**exact and case-sensitive**. To register a new alias, add a row. The reasoning
behind the canonical choices is documented in the
[notebooks journal](../notebooks/README.md#standardizing-emcee-names).

---

## `raw/`

Original data straight from the sources, before any cleaning. Produced by the
collection scripts in [`scripts/`](../scripts/) (or `fliptop-refresh --fetch`).
Treat these as **read-only inputs** — the pipeline never writes here.

### `youtube_videos.json`

A JSON list of every upload on the [FlipTop YouTube channel](https://www.youtube.com/@fliptopbattles),
one object per video. Written by
[`fetch_youtube_channel_uploads.py`](../scripts/fetch_youtube_channel_uploads.py).

Key fields: `id`, `title`, `description`, `upload_date` (ISO-8601 UTC),
`view_count`, `duration` (ISO-8601, e.g. `PT28M1S`), `url`, `likeCount`,
`commentCount`, `tags`. Counts arrive as **strings** from the API; the pipeline
coerces them to numbers. (Full field table in the
[scripts README](../scripts/README.md#fetch_youtube_channel_uploadspy).)

### `matchup_events_metadata.csv`

One row per matchup scraped from the [FlipTop website](https://www.fliptop.com.ph/videos/battle),
linking each matchup to its event and YouTube video id. Written by
[`fetch_events_metadata_from_fliptop_web.py`](../scripts/fetch_events_metadata_from_fliptop_web.py).

| column | example | notes |
| ------ | ------- | ----- |
| `matchup` | `Anygma vs Dirtbag Dan` | `emcee1 vs emcee2` |
| `event_name` | `Tectonics` | |
| `event_description` | `FlipTop presents: Tectonics @ … Dec. 4, 2010. …` | the pipeline parses event **date** and **location** out of this text |
| `video_id` | `5BiDPaDZHzo` | YouTube id (joins to `youtube_videos.json` `id`) |

### `versetracker_event_dates.csv`

A small reference table of **COVID-era event dates** recovered from
[VerseTracker](https://versetracker.com/battles/fliptop), used to fill the
`event_date`s the pipeline otherwise leaves blank for the quarantine window (see
[`processed/`](#processed) below). Written by
[`fetch_versetracker_event_dates.py`](../scripts/fetch_versetracker_event_dates.py).
Unlike the other raw files this one is **scraped on demand and committed** — the
dates are static — rather than refreshed by `fliptop-refresh --fetch`.

| column | example | notes |
| ------ | ------- | ----- |
| `event_name` | `Ahon 12` | base name, **no** `(Day N)` suffix |
| `event_date` | `2021-12-08` | ISO **first-day** date (the pipeline offsets per day for multi-day events) |
| `source_url` | `https://versetracker.com/events/fliptop-ahon-12` | the page the date came from |

> ⚠️ These dates are accurate to within ~days, not exact — VerseTracker appears to
> use the event **flyer-post** date for some events. Battles dated from this file
> are tagged `versetracker` in `ft_battles`' `event_date_source` column.

---

## `overrides/`

Small, hand-maintained **correction tables** the build applies to fix things the
raw sources get wrong — kept as CSVs (like `emcee_aliases.csv`) so they're edited
as data, not code. Each has a free-text `note` column recording *why* the row
exists; it's ignored on load. Loaded and validated by
[`fliptop.overrides`](../fliptop/overrides.py); a missing file is treated as an
empty table, so the pipeline still runs without them.

| file | key → value | match | fixes |
| ---- | ----------- | ----- | ----- |
| `event_locations.csv` | `event_name` → `event_location` | exact event name | battles whose venue couldn't be extracted (COVID-era obfuscation, or a no-`@` description that leaked the event name into the location) |
| `event_location_patterns.csv` | `contains` → `event_location` | substring of the location | a location string that carries junk around the real venue (e.g. `D' mention …`) |
| `location_aliases.csv` | `location` → `canonical` | exact value | normalize known location strings (e.g. Davao variants) |
| `event_dates.csv` | `id` → `event_date` | exact YouTube id | a battle whose own description mis-dates it, where the FlipTop site is authoritative (tagged `manual` in `ft_battles`) |
| `manual_matchups.csv` | `id` → matchup + participation roles | exact YouTube id | no-show or ambiguous titles such as `A + B vs C`; records scheduled emcees, helper emcee, and appeared/no-show status |

To register a correction, add a row (with a `note`). Matching is exact and
case-sensitive except `event_location_patterns.csv`, which matches any location
*containing* the substring.

---

## `processed/`

Clean, analysis-ready tables built by the `fliptop` package via
`fliptop-refresh`. **Regenerated, not hand-edited** — anything you change here is
overwritten on the next refresh.

### `ft_battles.json`

The project's core table: **one row per battle**, as newline-delimited JSON
(one battle per line). It is result-enriched from
`data/annotations/battle_results.csv`. Read it with
`pd.read_json(path, lines=True)`.

The full column-by-column schema is documented in the
[fliptop README](../fliptop/README.md#ft_battles-schema). A few file-format notes:

The final file keeps the analysis columns only. Rich audit fields such as
`description`, `duration_hms`, and `event_date_source` live in the internal
metadata table returned by `fliptop.build_battle_metadata()`.

- Process of Illumination and tryout events are excluded after event metadata is
  attached; they remain visible in `build_excluded_uploads` with an exclusion
  reason.
- `id` is a scalar battle key. For consolidated multi-part uploads, it is the
  first part's YouTube id; `url` may still be a list of source URLs.
- In the internal metadata layer, `id` and `url` are a **single value** for
  normal battles and a **list** for consolidated multi-part uploads.
- `event_name` is standardized (no `(Day N)` suffix); for a multi-day event,
  `event_date` carries the specific day the battle happened.
- Dates (`upload_date`, `event_date`) are stored as **epoch milliseconds** in the
  JSON; pass `convert_dates=[…]` (or `pd.to_datetime`) when loading.
- `battle_type`, `winner`, `votes_winner`, and `votes_loser` come from the
  validated annotation store. Fuller judging detail (`votes_nv`, `votes_ot`,
  `overtime`, `notes`) remains in `data/annotations/battle_results.csv`.

### `emcees.csv`

The distinct, canonicalized emcees that appear in `ft_battles`, with a stable id.

| column | example | notes |
| ------ | ------- | ----- |
| `emcee_id` | `1` | stable integer id |
| `emcee_name` | `$tep G` | canonical name |

Built by [`fliptop.structures.write_emcees_table`](../fliptop/structures.py).

### `battle_participants.csv`

Long-format participation table for event-history and survival-style analysis.
Regular battles have two scheduled participants with `appearance_credit=true`.
Manual no-show battles add the scheduled no-show with `appearance_credit=false`
and the helper emcee with `appearance_credit=true` but `battle_credit=false`.

---

## `annotations/`

Hand-collected data kept **deliberately separate** from the auto-built metadata,
so rebuilding never clobbers manual work. `fliptop-refresh` validates it and
joins the core result fields into the published `ft_battles.json`.

### `battle_results.csv`

The result of each battle (and the judges' tally), keyed by battle `id` and
written by `fliptop-annotate`. Every battle is one of two kinds — which the host
announces:

| column | values | notes |
| ------ | ------ | ----- |
| `id` | YouTube id | the battle key (first part id for multi-part battles) |
| `battle_type` | `judged` \| `promo` | `judged` includes decisions and draws; `promo` has no judging |
| `winner` | emcee name \| `NA` | `NA` for a judged draw or promo |
| `votes_winner` / `votes_loser` | int \| `NA` | judges voting for the winner / loser |
| `votes_nv` | int \| `NA` | judges who did not vote (NV) |
| `votes_ot` | int \| `NA` | judges who voted to go to overtime |
| `overtime` | `yes` \| `no` \| `NA` | did it go to an OT round? |
| `notes` | free text \| `none` | |

The tally is always the **final (post-overtime)** result; panel size varies
(5 / 7 / 9) and is just the sum of the vote columns. A judged battle whose score
wasn't recorded keeps its `winner` but has `NA` in every vote column. A judged
draw has `winner=NA`; a promo also has `winner=NA` but is distinguished by
`battle_type`. Draw details that do not fit the structured tally live in
`notes`. See the [annotations docs](../fliptop/README.md#battle-results--annotations)
for the full design and validation rules.

---

## `secret/`

Credentials and API keys (e.g. `secret.json` with a `YT_API_KEY` field used by
the YouTube fetch script). **Not committed to version control** — create it
locally. The scripts also accept keys via environment variables, so this folder
is optional if you prefer env vars.

---

## Regenerating everything

```bash
fliptop-refresh                        # rebuild processed/ from the raw files already on disk
fliptop-refresh --fetch                # re-scrape raw/ first (needs network + API key), then rebuild
fliptop-refresh --fetch --events-since 2025   # incremental events scrape (recent years only), then rebuild
fliptop-refresh --audit                # also write local data/debug audit files
```

`processed/` is fully reproducible from `raw/` + `emcee_aliases.csv` +
`overrides/` + `annotations/`. `raw/` is
reproducible from the network via [`scripts/`](../scripts/) — a full
`--fetch` overwrites the events CSV with a clean scrape, while `--events-since`
**merges** only recent events in (faster, but accumulates scrape history; see
[`scripts/README.md`](../scripts/README.md)). `annotations/` is **not**
reproducible — it's hand-entered, so it's the one thing here worth guarding.

---

`data/debug/` is git-ignored and regenerated on demand with
`fliptop-refresh --audit`. It includes `filtered_out.csv`,
`manual_matchup_needed.csv`, `pipeline_summary.csv`,
`pipeline_stage_drops.csv`, plus `upload_lineage.csv`, a one-row-per-raw-upload
audit of what the pipeline did with each source row. Use
`pipeline_summary.csv` for stage-by-stage row counts and
`pipeline_stage_drops.csv` for the exact ids that exited at filter/manual-review
stages.

## Conventions

- **No blank cells in the annotations store.** Missing/not-applicable values use
  explicit markers — `NA` for numbers/flags, `none` for empty notes — so the CSV
  never has ambiguous empties. Load with `keep_default_na=False` to preserve them.
- **Canonical names everywhere downstream.** By the time data reaches
  `processed/`, emcee names have been run through `emcee_aliases.csv`.
- **Dates** are epoch-ms in `ft_battles.json` and ISO-8601 strings in the raw
  YouTube JSON.
