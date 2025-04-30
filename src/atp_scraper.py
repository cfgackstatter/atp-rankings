import os
import pandas as pd
import glob
import difflib
import logging

logger = logging.getLogger(__name__)

def build_player_name_id_map(players_df):
    """Build a mapping from normalized full name to player_id."""
    full_names = (players_df['first_name'].astype(str).fillna('') + ' ' +
                  players_df['last_name'].astype(str).fillna('')).str.strip().str.lower()
    return pd.Series(players_df['player_id'].values, index=full_names).to_dict()

def map_and_combine_raw_files(players_df, raw_dir="data/atp_rankings_2025_raw", out_csv="data/cache/atp_rankings_2025.csv"):
    """
    Map player names to IDs for all raw 2025 files, save a single deduplicated CSV, and log unmapped players.
    """
    logger.info("Combining and mapping all raw 2025 files...")
    raw_files = sorted(glob.glob(f"{raw_dir}/atp_rankings_2025_*_raw.csv"))
    if not raw_files:
        logger.warning("No raw files found.")
        return
    all_raw = pd.concat([pd.read_csv(f) for f in raw_files], ignore_index=True)
    name_to_id = build_player_name_id_map(players_df)
    unique_names = all_raw['Player'].drop_duplicates()
    name_id_map = {}
    for name in unique_names:
        key = name.strip().lower()
        if key in name_to_id:
            name_id_map[name] = name_to_id[key]
        else:
            best = difflib.get_close_matches(key, list(name_to_id.keys()), n=1, cutoff=0.85)
            name_id_map[name] = name_to_id[best[0]] if best else ''
    all_raw['player_id'] = all_raw['Player'].map(name_id_map)
    all_raw['rank'] = pd.to_numeric(all_raw['Rank'], errors='coerce').fillna(0).astype(int)
    all_raw['points'] = pd.to_numeric(all_raw['Points'], errors='coerce').fillna(0).astype(int) if 'Points' in all_raw.columns else 0
    all_raw['player_id'] = pd.to_numeric(all_raw['player_id'], errors='coerce').apply(lambda x: int(x) if not pd.isna(x) else '').astype(str)
    all_raw['ranking_date'] = pd.to_datetime(all_raw['ranking_date'])
    out_cols = ['ranking_date', 'rank', 'player_id', 'points']
    all_raw[out_cols].to_csv(out_csv, index=False)
    logger.info(f"Saved combined 2025 rankings to {out_csv}")
    # Save unmapped players
    unmapped = [name for name, pid in name_id_map.items() if pid == '']
    if unmapped:
        pd.DataFrame({'Player': unmapped}).to_csv("data/atp_players_new.csv", index=False)
        logger.info(f"Saved {len(unmapped)} unmapped players to data/atp_players_new.csv")