# data

layout:
- `raw/`  
  Original data files from webscraping and intermediate files for manual editing.

- `processed/`  
  Clean tables created by the `fliptop` package.  
  For example `df_battles.json`.

- `annotations/`  
  Manually-collected data kept separate from the auto-built tables.  
  `battle_results.csv` holds win/loss results keyed by battle id (append-only):
  `winner` plus structured judging columns (`judging_status`, `votes_winner`,
  `votes_loser`, `votes_nv`, `votes_ot`, `overtime`) and `notes`. Populated via
  `fliptop-annotate`.

- `secret/`  
  Credentials, API keys, and other private files.  
  Not committed to version control.