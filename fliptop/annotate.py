"""
fliptop.annotate

Interactive terminal tool to record battle results into the structured,
id-keyed store at data/annotations/battle_results.csv.

It walks through battles that are not yet annotated, newest upload first,
showing each matchup and its URL. It writes after every entry, so it is
crash-safe and fully resumable: quit any time and the next run picks up where
you left off, only ever showing battles you have not done.

    fliptop-annotate                 # go through all pending battles
    fliptop-annotate --limit 20      # do up to 20, then stop
    fliptop-annotate --event Ahon    # only battles whose event matches "Ahon"
    fliptop-annotate --open          # open each battle's URL in the browser

For each battle you record either the winner and the final (post-overtime)
judges' tally, or that it was a promo bout with no winner (press ``p``). The
common case is two keystrokes (winner, then a score like 5-0); the no-vote /
overtime-vote / overtime follow-ups are only asked if you opt in.
"""

from __future__ import annotations

import argparse
import re
import webbrowser

import pandas as pd

from . import RAW_DATA_DIR
from . import annotations as ann
from .battles import build_df_battles

# sentinels returned by the winner prompt
_QUIT = object()
_SKIP = object()
_PROMO = object()


def _watch_url(battle_key: str) -> str:
    """Build a canonical, clickable YouTube watch URL from a battle id."""
    return f"https://www.youtube.com/watch?v={battle_key}"


def _prompt_winner(emcee1: str, emcee2: str):
    """Return winner name, or a _QUIT / _SKIP / _PROMO sentinel."""
    while True:
        raw = input(
            f"  Winner [1={emcee1}  2={emcee2}  p=promo (no winner)  s=skip  q=quit]: "
        ).strip()
        low = raw.casefold()
        if low == "q":
            return _QUIT
        if low in ("", "s"):
            return _SKIP
        if low == "p":
            return _PROMO
        if raw == "1":
            return emcee1
        if raw == "2":
            return emcee2
        if ann.validate_winner(raw, emcee1, emcee2):
            return emcee1 if raw.casefold() == emcee1.casefold() else emcee2
        print("    ! enter 1, 2, an emcee name, p, s, or q")


def _prompt_score():
    """Return (votes_winner, votes_loser) as ints, or None if left blank."""
    while True:
        raw = input("  Final score winner-loser (e.g. 5-0; blank=unknown): ").strip()
        if not raw:
            return None
        parts = raw.split("-")
        if len(parts) == 2 and all(p.strip().isdigit() for p in parts):
            return int(parts[0]), int(parts[1])
        print("    ! use the form W-L with whole numbers, e.g. 5-0")


def _prompt_int(label: str, default: int = 0) -> int:
    while True:
        raw = input(f"    {label} [{default}]: ").strip()
        if not raw:
            return default
        if raw.isdigit():
            return int(raw)
        print("    ! enter a whole number")


def _prompt_yes_no(label: str, default: str = "no") -> str:
    while True:
        raw = input(f"    {label} [{default}]: ").strip().casefold()
        if not raw:
            return default
        if raw in ("y", "yes"):
            return "yes"
        if raw in ("n", "no"):
            return "no"
        print("    ! y or n")


def _collect_judging(winner) -> dict:
    """Prompt the structured judging fields; returns kwargs for make_result_row."""
    if winner is _PROMO:
        return {"winner": ann.NA, "battle_type": "promo"}

    score = _prompt_score()
    if score is None:
        # judged battle, winner known, score not recorded
        return {"winner": winner, "battle_type": "judged"}

    vw, vl = score
    nv, ot, overtime = 0, 0, "no"
    if _prompt_yes_no("Any no-votes / OT-votes / overtime?", "no") == "yes":
        nv = _prompt_int("no-votes (judges who didn't vote)", 0)
        ot = _prompt_int("OT-votes (judges who voted to go to overtime)", 0)
        overtime = _prompt_yes_no("did the battle go to overtime?", "no")

    return {
        "winner": winner,
        "battle_type": "judged",
        "votes_winner": vw,
        "votes_loser": vl,
        "votes_nv": nv,
        "votes_ot": ot,
        "overtime": overtime,
    }


def _summarize(row: dict) -> str:
    if row["battle_type"] == "promo":
        return "promo (no winner)"
    if str(row["votes_winner"]) == ann.NA:
        return f"{row['winner']} wins (score unknown)"
    tally = f"{row['votes_winner']}-{row['votes_loser']}"
    extra = []
    if str(row["votes_nv"]) != "0":
        extra.append(f"{row['votes_nv']} NV")
    if str(row["votes_ot"]) != "0":
        extra.append(f"{row['votes_ot']} OT-votes")
    if row["overtime"] == "yes":
        extra.append("after OT")
    suffix = f" ({', '.join(extra)})" if extra else ""
    return f"{row['winner']} wins {tally}{suffix}"


def _current_result_str(results: pd.DataFrame, key: str) -> str:
    """Human-readable summary of the currently-stored result for a battle id."""
    match = results[results["id"] == key]
    if match.empty:
        return "not yet recorded"
    return _summarize(match.iloc[0].to_dict())


def resolve_targets(df_battles: pd.DataFrame, term: str) -> pd.DataFrame:
    """
    Find battles matching a --redo term.

    The term may be a YouTube URL, an 11-char video id, or free text matched
    against the matchup (e.g. "Loonie vs Abra" or just "Abra"). Returns matching
    df_battles rows (newest first) with a ``battle_key`` column.
    """
    work = df_battles.copy()
    work["battle_key"] = work["id"].map(ann.battle_key)

    key = ann.extract_video_id(term)
    if key is None and re.fullmatch(r"[A-Za-z0-9_-]{11}", term.strip()):
        key = term.strip()
    if key is not None:
        matches = work[work["battle_key"] == key]
    else:
        matches = work[work["matchup"].astype(str).str.contains(term, case=False, na=False)]

    if "upload_date" in matches.columns:
        matches = matches.sort_values("upload_date", ascending=False)
    return matches.reset_index(drop=True)


def redo(term: str) -> None:
    """Re-annotate an existing battle, overwriting its row in the store."""
    df = build_df_battles(raw_dir=RAW_DATA_DIR)
    results = ann.load_results()

    targets = resolve_targets(df, term)
    if targets.empty:
        print(f"No battles match {term!r}.")
        return

    if len(targets) == 1:
        row = targets.iloc[0]
    else:
        print(f"{len(targets)} matches for {term!r} (newest first):")
        for i, r in targets.iterrows():
            date = (
                pd.to_datetime(r["upload_date"]).date().isoformat()
                if "upload_date" in r and pd.notna(r["upload_date"])
                else "?"
            )
            current = _current_result_str(results, r["battle_key"])
            print(f"  [{i + 1}] {r['emcee1']} vs {r['emcee2']}   uploaded {date}   {r.get('event_name', '')}")
            print(f"        currently: {current}")
            print(f"        {_watch_url(r['battle_key'])}")
        choice = input("Pick a number to redo (q to cancel): ").strip()
        if not choice.isdigit() or not (1 <= int(choice) <= len(targets)):
            print("Cancelled.")
            return
        row = targets.iloc[int(choice) - 1]

    emcee1, emcee2 = str(row["emcee1"]), str(row["emcee2"])
    key = row["battle_key"]
    print(f"\n{emcee1} vs {emcee2}   ({row.get('event_name', '')})")
    print(f"  {_watch_url(key)}")
    print(f"  current: {_current_result_str(results, key)}\n")

    winner = _prompt_winner(emcee1, emcee2)
    if winner is _QUIT or winner is _SKIP:
        print("No change made.")
        return

    fields = _collect_judging(winner)
    notes = input("  Notes (optional): ").strip()
    result_row = ann.make_result_row(id=key, notes=notes, **fields)

    results = ann.upsert_result(results, result_row)
    ann.save_results(results)
    print(f"  [saved] {_summarize(result_row)}")


def run(*, limit: int | None = None, event: str | None = None, open_urls: bool = False) -> None:
    df = build_df_battles(raw_dir=RAW_DATA_DIR)
    results = ann.load_results()

    pending = ann.pending_battles(df, results)
    if event:
        pending = pending[
            pending["event_name"].astype(str).str.contains(event, case=False, na=False)
        ].reset_index(drop=True)

    total = len(pending)
    if total == 0:
        print("Nothing to annotate - every battle already has a result.")
        return

    print(f"{total} battle(s) to annotate. Enter q any time to stop and save.\n")

    done_this_run = 0
    for i, row in pending.iterrows():
        if limit is not None and done_this_run >= limit:
            print(f"\nReached --limit {limit}. Stopping.")
            break

        emcee1, emcee2 = str(row["emcee1"]), str(row["emcee2"])
        url = _watch_url(row["battle_key"])
        event_name = row.get("event_name", "")
        date = row.get("upload_date", "")
        date_str = pd.to_datetime(date).date().isoformat() if pd.notna(date) else "?"

        print(f"Battle {i + 1} / {total}          ({event_name} - {date_str})")
        print(f"  {emcee1} vs {emcee2}")
        print(f"  {url}")
        if open_urls and url:
            webbrowser.open(url)

        winner = _prompt_winner(emcee1, emcee2)
        if winner is _QUIT:
            break
        if winner is _SKIP:
            print("  (skipped)\n")
            continue

        fields = _collect_judging(winner)
        notes = input("  Notes (optional): ").strip()
        result_row = ann.make_result_row(id=row["battle_key"], notes=notes, **fields)

        results = ann.upsert_result(results, result_row)
        ann.save_results(results)  # persist after every entry
        done_this_run += 1
        print(f"  [saved] {_summarize(result_row)}\n")

    remaining = len(ann.pending_battles(df))
    print(f"\nDone. Recorded {done_this_run} this run; {remaining} still pending.")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Interactively annotate FlipTop battle results into the id-keyed store."
    )
    parser.add_argument("--limit", type=int, default=None, help="Stop after this many entries.")
    parser.add_argument("--event", default=None, help="Only battles whose event name contains this text.")
    parser.add_argument("--open", action="store_true", dest="open_urls", help="Open each battle URL in the browser.")
    parser.add_argument(
        "--redo",
        default=None,
        metavar="MATCHUP|ID|URL",
        help="Re-annotate an existing battle (found by matchup text, video id, or URL), overwriting its result.",
    )
    args = parser.parse_args(argv)

    try:
        if args.redo:
            redo(args.redo)
        else:
            run(limit=args.limit, event=args.event, open_urls=args.open_urls)
    except (KeyboardInterrupt, EOFError):
        print("\nInterrupted - progress was saved after the last completed entry.")


if __name__ == "__main__":
    main()
