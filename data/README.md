# Data

Everything the project reads or writes lives here, organized by **how trustworthy
and how processed** it is — from raw scraped sources, through the clean
pipeline outputs, to the hand-collected annotations layered on top.

```
data/
├── emcee_aliases.csv      # hand-maintained alias → canonical name map
├── raw/                   # original scraped sources (input to the pipeline)
│   ├── youtube_videos.json
│   └── matchup_events_metadata.csv
├── processed/             # clean tables built by the fliptop package
│   ├── df_battles.json
│   └── emcees.csv
├── annotations/           # hand-collected results, kept separate from processed
│   └── battle_results.csv
└── secret/                # API keys etc. — git-ignored
    └── secret.json
```

**Data flow.** `raw/` is produced by [`scripts/`](../scripts/); `processed/` is
built from `raw/` (plus `emcee_aliases.csv`) by the
[`fliptop`](../fliptop/) package; `annotations/` is filled in by hand via
`fliptop-annotate` and joined onto the processed table only on demand.

```
scripts/ ─► raw/ ─┐
                  ├─► fliptop (build) ─► processed/
emcee_aliases.csv ┘                          │
                                  annotations/ ─(join on demand)─┘
```

---

## Contents

- [`emcee_aliases.csv`](#emcee_aliasescsv)
- [`raw/`](#raw)
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

---

## `processed/`

Clean, analysis-ready tables built by the `fliptop` package via
`fliptop-refresh`. **Regenerated, not hand-edited** — anything you change here is
overwritten on the next refresh.

### `df_battles.json`

The project's core table: **one row per battle**, as newline-delimited JSON
(one battle per line). Read it with `pd.read_json(path, lines=True)`.

The full column-by-column schema is documented in the
[fliptop README](../fliptop/README.md#df_battles-schema). A few file-format notes:

- `id` and `url` are a **single value** for normal battles and a **list** for
  consolidated multi-part (`pt. 1`, `pt. 2`, …) uploads.
- `event_name` is standardized (no `(Day N)` suffix); for a multi-day event,
  `event_date` carries the specific day the battle happened.
- Dates (`upload_date`, `event_date`) are stored as **epoch milliseconds** in the
  JSON; pass `convert_dates=[…]` (or `pd.to_datetime`) when loading.
- `event_date` is **`null` for COVID-era battles** — intentional; see the note in
  the [root README](../README.md) and the
  [imputation notebook](../notebooks/README.md#covid-era-event-dates--ongoing).

### `emcees.csv`

The distinct, canonicalized emcees that appear in `df_battles`, with a stable id.

| column | example | notes |
| ------ | ------- | ----- |
| `emcee_id` | `1` | stable integer id |
| `emcee_name` | `$tep G` | canonical name |

Built by [`fliptop.structures.write_emcees_table`](../fliptop/structures.py).

---

## `annotations/`

Hand-collected data kept **deliberately separate** from the auto-built tables, so
rebuilding `processed/` never clobbers manual work. Joined onto `df_battles` on
demand via `fliptop.annotations.merge_results`.

### `battle_results.csv`

Who won each battle (and the judges' tally), keyed by battle `id` and written by
`fliptop-annotate`. Every battle is one of two kinds — which the host announces:

| column | values | notes |
| ------ | ------ | ----- |
| `id` | YouTube id | the battle key (first part id for multi-part battles) |
| `battle_type` | `judged` \| `promo` | `promo` = exhibition bout, **no winner** by design |
| `winner` | emcee name \| `NA` | the winner for a judged battle; `NA` for promo |
| `votes_winner` / `votes_loser` | int \| `NA` | judges voting for the winner / loser |
| `votes_nv` | int \| `NA` | judges who did not vote (NV) |
| `votes_ot` | int \| `NA` | judges who voted to go to overtime |
| `overtime` | `yes` \| `no` \| `NA` | did it go to an OT round? |
| `notes` | free text \| `none` | |

The tally is always the **final (post-overtime)** result; panel size varies
(5 / 7 / 9) and is just the sum of the vote columns. A judged battle whose score
wasn't recorded keeps its `winner` but has `NA` in every vote column. See the
[annotations docs](../fliptop/README.md#battle-results--annotations) for the full
design and validation rules.

---

## `secret/`

Credentials and API keys (e.g. `secret.json` with a `YT_API_KEY` field used by
the YouTube fetch script). **Not committed to version control** — create it
locally. The scripts also accept keys via environment variables, so this folder
is optional if you prefer env vars.

---

## Regenerating everything

```bash
fliptop-refresh            # rebuild processed/ from the raw files already on disk
fliptop-refresh --fetch    # re-scrape raw/ first (needs network + API key), then rebuild
```

`processed/` is fully reproducible from `raw/` + `emcee_aliases.csv`. `raw/` is
reproducible from the network via [`scripts/`](../scripts/). `annotations/` is
**not** reproducible — it's hand-entered, so it's the one thing here worth
guarding.

---

## Conventions

- **No blank cells in the annotations store.** Missing/not-applicable values use
  explicit markers — `NA` for numbers/flags, `none` for empty notes — so the CSV
  never has ambiguous empties. Load with `keep_default_na=False` to preserve them.
- **Canonical names everywhere downstream.** By the time data reaches
  `processed/`, emcee names have been run through `emcee_aliases.csv`.
- **Dates** are epoch-ms in `df_battles.json` and ISO-8601 strings in the raw
  YouTube JSON.
