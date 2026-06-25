# data

layout:
- `emcee_aliases.csv`  
  Hand-maintained alias → canonical emcee-name mapping, used by the pipeline to
  standardize names. Loaded and validated via `fliptop.rename_map.load_rename_map`.
  Add a row to register a new alias.

- `raw/`  
  Original data files from webscraping and intermediate files for manual editing.

- `processed/`  
  Clean tables created by the `fliptop` package.  
  For example `df_battles.json`.

- `annotations/`  
  Manually-collected data kept separate from the auto-built tables.  
  `battle_results.csv` holds win/loss results keyed by battle id (append-only):
  `battle_type` (`judged` | `promo`), `winner`, the structured judging columns
  (`votes_winner`, `votes_loser`, `votes_nv`, `votes_ot`, `overtime`) and
  `notes`. Populated via `fliptop-annotate`.

- `secret/`  
  Credentials, API keys, and other private files.  
  Not committed to version control.