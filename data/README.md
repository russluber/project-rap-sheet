# data

Data layout:
- `raw/`  
  Original scraped and downloaded files. Treated as read only.

- `processed/`  
  Clean tables created by the `fliptop` package.  
  For example `df_battles.json`.

- `secret/`  
  Credentials, API keys, and other private files.  
  Not committed to version control.