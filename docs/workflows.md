# Maintainer Workflows

This file is for the human maintenance routines that cut across the package,
raw data, overrides, annotations, and Git. The package README explains how the
pipeline is built; this file explains what to do when you are actually keeping
the dataset alive.

## Routine Refresh After A Few Weeks

Use this when some time has passed, FlipTop has uploaded more videos, and you
want to catch the dataset up without doing a full pipeline archaeology session.

The goal is:

- fetch new raw data;
- rebuild the processed outputs;
- inspect what changed;
- resolve any new manual decisions;
- annotate any new battles;
- rerun the build and tests before committing.

### 1. Start Clean

First make sure you are on the latest code and do not have unrelated changes in
the way.

```powershell
git pull
git status
uv sync --locked
```

`git status` does not have to be perfectly empty if you are intentionally in the
middle of work, but it should be boring enough that you can tell whether the
refresh changed data, docs, or both.

### 2. Fetch Recent Raw Data And Rebuild

For normal maintenance, use an incremental events scrape. The YouTube fetch is
already incremental; `--events-since` controls how much of the FlipTop website
event metadata to re-scrape and merge.

As of 2026, a routine top-up can usually start at the current year:

```powershell
uv run fliptop-refresh --fetch --events-since 2026
```

If you are near a year boundary, catching up after a long gap, or suspicious
that an older event page was edited, widen the window:

```powershell
uv run fliptop-refresh --fetch --events-since 2025
```

Use a full fetch only when you want a clean reconcile of the entire FlipTop
website event scrape:

```powershell
uv run fliptop-refresh --fetch
```

That is slower, but useful periodically.

All of those refresh commands write `data/debug/` audit files by default. Use
`--no-audit` only for a deliberately quiet rebuild where you do not need the
lineage/debug surfaces.

### 3. Read The Build Summary First

The terminal output should tell you the basic shape of the refresh:

```text
[validate] metadata: ... battles
[validate] final: ... battles
[build] wrote ... battles
[build] wrote ... participant rows
[done] refresh complete.
```

If the command aborts because annotations are missing, that is normal after new
battles arrive. Do the manual review steps below, then run the refresh again.

### 4. Inspect The Audit Files

Open the generated debug files before annotating. They are local artifacts and
are intentionally ignored by Git.

Most useful first:

```text
data/debug/pipeline_summary.csv
data/debug/pipeline_stage_drops.csv
data/debug/manual_matchup_needed.csv
data/debug/filtered_out.csv
data/debug/upload_lineage.csv
```

Read them in this order:

1. `pipeline_summary.csv` - confirms where row counts changed.
2. `manual_matchup_needed.csv` - tells you whether any title needs a human
   matchup decision before it can enter the dataset.
3. `filtered_out.csv` - spot-checks rows that were excluded.
4. `upload_lineage.csv` - the full one-row-per-upload audit trail when you need
   to understand a specific video id.

The quick gut check is: "Did the new rows land where I expected, or did a filter
catch something that looks like a real battle?"

### 5. Resolve Manual Matchup Rows

If `manual_matchup_needed.csv` has rows, decide whether each one should be
resolved, excluded, or left for later review.

For no-show or odd-format battle titles, edit:

```text
data/overrides/manual_matchups.csv
```

Current convention:

```text
emcee1 = the scheduled opponent who appeared
emcee2 = the no-show opponent, if this was a no-show battle
helper_emcee = the extra emcee who helped/freestyled, if applicable
emcee1_status = appeared
emcee2_status = no_show
helper_status = appeared
```

If the upload should be held out by exact id, use:

```text
data/overrides/upload_decisions.csv
```

Then rerun:

```powershell
uv run fliptop-refresh
```

Keep repeating this loop until `manual_matchup_needed.csv` is empty or contains
only rows you intentionally want to leave unresolved.

### 6. Annotate New Battle Results

Once the metadata build is happy, annotate any new battles:

```powershell
uv run fliptop-annotate
```

Useful reminders:

- `p` records a promo battle.
- `d` records a judged draw.
- The annotation store is `data/annotations/battle_results.csv`.
- The final output refuses to publish if any battle is missing an annotation.

After annotation, rebuild:

```powershell
uv run fliptop-refresh
```

### 7. Validate Before Committing

Run the checks from the locked uv environment:

```powershell
uv run pytest -q --basetemp .pytest-tmp
uv run ruff check fliptop tests
uv lock --check
```

Then look at the Git diff:

```powershell
git status
git diff --stat
```

For a normal refresh, you might expect changes in:

```text
data/raw/youtube_videos.json
data/raw/matchup_events_metadata.csv
data/processed/ft_battles.json
data/processed/battle_participants.csv
data/processed/emcees.csv
data/annotations/battle_results.csv
data/overrides/*.csv
```

You should not expect `data/debug/` files in the commit; they are regenerated
local audit outputs.

### 8. Commit The Refresh

Use a commit message that says what actually happened. Examples:

```text
Refresh FlipTop dataset
Refresh dataset and annotate new battles
Add manual matchups for new no-show battles
Update emcee aliases after refresh
```

If the refresh required several types of human decisions, separate commits are
often easier to read later:

```text
Add manual matchup overrides
Annotate new battle results
Refresh processed FlipTop outputs
```

### If Something Looks Off

A few common "pause and inspect" signals:

- A real battle appears in `filtered_out.csv`.
- `manual_matchup_needed.csv` contains a title that should be parseable.
- The final battle count drops unexpectedly.
- `battle_participants.csv` changes more than `ft_battles.json` suggests.
- A new emcee appears twice under slightly different names.

When that happens, do not force the refresh through. Use `upload_lineage.csv` to
follow the affected video id, then fix the appropriate data file:

```text
data/emcee_aliases.csv
data/overrides/manual_matchups.csv
data/overrides/upload_decisions.csv
data/rules/title_exclusions.csv
data/rules/event_exclusions.csv
data/overrides/event_dates.csv
data/overrides/event_locations.csv
```

Then rerun:

```powershell
uv run fliptop-refresh
uv run pytest -q --basetemp .pytest-tmp
```
