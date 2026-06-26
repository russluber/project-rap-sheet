# Notebooks

This folder is part **working notebooks**, part **journal**. The journal below is
where project assumptions, decisions, and open tasks live — a running record of
*why* the data looks the way it does. The notebooks themselves are exploratory;
the reusable logic they settle on gets promoted into the
[`fliptop`](../fliptop/) package.

> Notebooks *import* `fliptop`; they don't reimplement it. If something here
> hardens into pipeline logic, it belongs in the package, not in a cell.

---

## Contents

- [Notebooks in this folder](#notebooks-in-this-folder)
- [Research questions](#research-questions)
- [What counts as a rap battle?](#what-counts-as-a-rap-battle)
- [Keywords for inclusion / exclusion](#keywords-for-inclusion--exclusion)
- [One-versus-one only](#one-versus-one-only)
- [Dealing with aliases](#dealing-with-aliases)
- [Standardizing emcee names](#standardizing-emcee-names)
- [COVID-era event dates — ONGOING](#covid-era-event-dates--ongoing)
- [Further-down-the-line questions](#further-down-the-line-questions)

---

## Notebooks in this folder

| notebook | what it's for | status |
| -------- | ------------- | ------ |
| `wrangling.ipynb` | Loads `df_battles`, explores `event_name` / `event_location`, and is where the **COVID-era date & location sleuthing** actually happens (gathering month/year estimates and venue leads from external sources). | active scratchpad |
| `imputation.ipynb` | An attempt to *infer* missing COVID-era `event_date`s from signals like `event_name` seasonality and `upload_date` lag. | abandoned — see below |

> On `imputation.ipynb`, in my own words: *"This went nowhere honestly. Just an
> artifact of something I tried to do."* The idea was to exploit the seasonality
> of recurring FlipTop events to narrow down when the quarantine events happened.
> It turned out less helpful than hoped — **external sources are still the better
> bet** for recovering these dates. Kept around as a record of what I tried.

---

## Research questions

The central question is about **FlipTop battle-rap careers and their length**:

> How long does a career in the Philippines' premier rap battle league FlipTop
> last?

Who rises to the top? What determines who stays relevant over the years? I'm
thinking **survival (time-to-event) analysis** here.

A second question is about the **shape of the rivalry network**:

> Who has battled whom? And which matchups haven't happened yet?

I'm picturing a graph, and want to understand how dense or sparse FlipTop's
battle network is:

- **Nodes** — emcees
- **Edges** — "has battled"
- **Edge weight** — how many times the two have battled

(The network is built in [`fliptop.structures`](../fliptop/structures.py).)

---

## What counts as a rap battle?

Most videos on the [FlipTop YouTube channel](https://www.youtube.com/@fliptopbattles)
are rap battles, but plenty are video flyers, announcements, behind-the-scenes
clips, reaction videos, and so on. To analyze *battles*, those have to be
filtered out first.

My criteria for what counts as a rap battle for this project:

1. The video is **a cappella** (no underlying beat behind the emcees).
2. The emcees perform **written material** (not purely off-the-top freestyle).
   - The early days of FlipTop were more about testing each other with
     off-the-top freestyle — emcees trading insults with material thought up on
     the spot.
3. To a lesser extent, the battle is **judged** at the end (there are stakes).

> **A note on the early videos — a decision that evolved.**
> By these criteria, some of FlipTop's earliest videos wouldn't qualify.
> - First I leaned toward including them all, a cappella or with a beat.
> - **Where I landed:** include the early **a cappella** freestyle battles, but
>   **not** the freestyle battles that have an accompanying beat.

This filtering is implemented in [`fliptop.battles`](../fliptop/battles.py) — see
the [pipeline write-up](../fliptop/README.md#stage-1--clean-youtube-uploads--1v1-uploads)
for exactly where rows get dropped.

---

## Keywords for inclusion / exclusion

As a long-time viewer, I know a handful of title keywords that make filtering
against the criteria above much easier. These back the `EXCLUDE_KEYWORDS` list
and the `vs` filter in [`fliptop.battles`](../fliptop/battles.py).

**Include**

| keyword | why |
| ------- | --- |
| **vs** | Nearly every a cappella battle on the channel has "vs" (short for *versus*) in the title. |

**Exclude**

| keyword | why it's not a 1v1 judged battle |
| ------- | -------------------------------- |
| **tryout** | Newcomer tryouts — often unjudged, especially in older videos. |
| **beatbox** | A separate genre from the a cappella judged battles. |
| **flyer** / **promo** | Advertisements and announcement videos for upcoming events. |
| **Anygma Machine** | Anygma (FlipTop's head) reviewing battles / previewing matches. A nod to the WW2 [Enigma machine](https://en.wikipedia.org/wiki/Enigma_machine). |
| **[LIVE]** | Live performances from the 2020 FlipTop Festival. |
| **UnggoYan** | Emcees reacting to comments on their past battles. |
| **Pre-Battle Interviews** | Self-explanatory. |
| **Salitang Ugat** | "Root word" — interviews on how emcees came up with their battle names. |
| **Trailer** | Promo trailers for upcoming events. |
| **Video Flyer** | Self-explanatory. |
| **Silip** | Recently-added behind-the-scenes videos. |
| **Sound Check** | Pre-event check-ins with FlipTop event prep. |
| **Tribute** | Tributes to rappers who've passed. |
| **Tutok** | Other behind-the-scenes videos. |
| **Abangan** | Teaser clips. |

---

## One-versus-one only

This project considers only battles **between two people**. FlipTop runs several
formats beyond the classic 1v1, for example:

- **Royal Rumble** (1v1v1v1v1)
- **5-on-5**
- **Tag-team 2-vs-2** (Dos Por Dos)

The vast majority of battles are one-on-one, and those are the focus. (The
multi-emcee formats are dropped by the `keep_1v1` filter in the pipeline.)

---

## Dealing with aliases

Some well-known emcees have battled under gimmick aliases rather than their usual
names:

| usual name | alias |
| ---------- | ----- |
| Poison13 | Markong Bungo |
| Tipsy D | Freak Sanchez |
| Goriong Talas | Ghostly |
| Emar Industriya | No. 144 |
| Sayadd | Carlito |
| GL | 1ce Water |

I've decided to **do away with these gimmick aliases**. What I care about is the
careers of the *people* behind the personas, so I count these battles under each
emcee's main name, not the alias.

---

## Standardizing emcee names

Emcee names in the video titles are sometimes inconsistent. For analysis, the
names need to be consistent across data points. How I standardized them:

- For an emcee with multiple spellings or a rename over time, I took their **most
  recent** name.
- For purely aesthetic ties, I arbitrarily picked whichever looked best to me —
  e.g. "Daddy Joe D" vs "Daddie Joe D" vs "DaddieJoe D".
- For lesser-known emcees who changed names, I **cross-referenced faces** across
  videos to confirm they were the same person.

These conventions are formalized in
[`fliptop.rename_map`](../fliptop/rename_map.py) and the mapping itself lives in
[`data/emcee_aliases.csv`](../data/emcee_aliases.csv).

---

## COVID-era event dates — ONGOING

All COVID-era ("quarantine") battles are missing `event_date`. FlipTop
**obfuscated** these — both the YouTube descriptions and the FlipTop website carry
implausible dates and made-up locations, presumably to avoid scrutiny while
[quarantine lockdowns](https://en.wikipedia.org/wiki/Enhanced_community_quarantine_in_Luzon)
were in force. Rather than record dates known to be wrong, the pipeline leaves
`event_date` as `null` for this window.

**Why bother recovering them?** Real event dates matter for estimating career
lengths (a core goal of the project) — and it's just an interesting inference
problem. That said: the **upload** to YouTube is itself part of an emcee's career
(like a music video dropping after the audio), so in a pinch `upload_date` is a
serviceable stand-in.

**The events that need real dates.** Second Sight 8 was the first COVID-era event
and Ahon 12 was the last ([context here](https://www.fliptop.com.ph/articles/an-unforgettable-second-sight-8)).
The table below collects month/year estimates I pulled from
[Wikipedia](https://en.wikipedia.org/wiki/FlipTop_Battle_League) (**unverified**),
gathered in `wrangling.ipynb`:

| # | event | estimate (unverified) |
| - | ----- | --------------------- |
| 1 | Second Sight 8 | not listed |
| 2 | Unibersikulo 8 | not listed |
| 3 | Zoning 10 | July 2020 |
| 4 | Bwelta Balentong 7 (Day 1) | October 2020 |
| 5 | Bwelta Balentong 7 (Day 2) | October 2020 |
| 6 | Ahon 11 (Day 1) | October 2020 |
| 7 | Ahon 11 (Day 2) | October 2020 |
| 8 | Grain Assault 11 | May 2021 |
| 9 | Second Sight 9 | June 2021 |
| 10 | Zoning 11 | June 2021 |
| 11 | Bwelta Balentong 8 | June 2021 |
| 12 | Zoning 12 | July 2021 |
| 13 | Unibersikulo 9 | September 2021 |
| 14 | Zoning 13 | October 2021 |
| 15 | Unibersikulo 10 | November 2021 |
| 16 | Ahon 12 (Day 1) | December 2021 |
| 17 | Ahon 12 (Day 2) | December 2021 |

Second Sight 8 and Unibersikulo 8 aren't listed anywhere. As a backstop: the
FlipTop Festival was February 7–8, 2020, and Zoning 10 lands ~July 2020 — so
those two events most likely fall between **late February and June 2020**.

**Leads being chased** (source links tracked in `wrangling.ipynb`):

- Quarantine-battle and event posters on Facebook (Second Sight 8, Unibersikulo
  8, the Quarantine Battles series).
- Emcee posts pinning specific dates — e.g. K-Ram dating Bwelta Balentong 7 to
  **October 24, 2020**.
- **Locations** are partly recovered too: the quarantine venue "Baraks" (CIFRA
  Building, Boni Ave, Mandaluyong) → normalize to *FlipTop Baraks, Mandaluyong
  City*; and **Ahon 12** at *Jenerick Resort, Tanauan City, Batangas*.

**Other avenues** if the above stalls: contacting FlipTop directly, or
COVID-era emcee posts on Facebook.

---

## Further-down-the-line questions

Ideas to revisit once the core dataset is solid:

- Per-emcee one-on-one career statistics.
- A project webpage — a battle-network explorer (maybe D3 over the graph).
- More survival-analysis angles on career length.

And some questions I'm curious about:

- Which emcees have had the **biggest comebacks**?
- The basics — e.g. most-viewed battler.
- Does **career length correlate with views** over time? Do emcees get more
  popular the longer they last?
- **Win streaks** — now that battle results are recorded
  ([`battle_results.csv`](../data/annotations/battle_results.csv) via
  `fliptop-annotate`), who has the longest?
