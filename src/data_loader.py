import pandas as pd
import numpy as np
import requests
import os
from pathlib import Path

# Base URL for Jeff Sackmann's tennis_atp repository
BASE_URL = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master"

def ensure_cache_dir():
    """Create cache directory if it doesn't exist"""
    cache_dir = Path("data/cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir

def download_file(filename, force_download=False):
    """Download a file from the tennis_atp repository if not in cache"""
    cache_dir = ensure_cache_dir()
    local_path = cache_dir / filename
    
    # If file exists and we're not forcing a download, use cached version
    if local_path.exists() and not force_download:
        print(f"Using cached {filename}")
        return local_path
    
    # Download the file
    url = f"{BASE_URL}/{filename}"
    print(f"Downloading {url}")
    
    response = requests.get(url)
    response.raise_for_status()  # Raise exception for 4XX/5XX responses
    
    # Save to cache
    with open(local_path, "wb") as f:
        f.write(response.content)
    
    print(f"Downloaded {filename} to {local_path}")
    return local_path

def load_players():
    """Load player data from atp_players.csv"""
    players_file = download_file("atp_players.csv")
    
    # First time: load CSV and save as parquet
    parquet_path = Path("data/cache/players.parquet")
    if not parquet_path.exists():
        players = pd.read_csv(players_file, low_memory=False, parse_dates=['dob'], keep_default_na=False)
        # Standardize column names
        column_mapping = {
            'name_first': 'first_name',
            'name_last': 'last_name',
            'dob': 'birth_date',
            'ioc': 'country_code'
        }
        rename_dict = {old: new for old, new in column_mapping.items() if old in players.columns}
        if rename_dict:
            players = players.rename(columns=rename_dict)
        # Save as parquet for future use
        players.to_parquet(parquet_path)
    else:
        # Use parquet for subsequent loads (much faster)
        players = pd.read_parquet(parquet_path)
    
    return players

def load_rankings(decades=None):
    """
    Load all rankings data
      
    Returns:
        DataFrame with all rankings data
    """
    decades = ['70s', '80s', '90s', '00s', '10s', '20s', 'current']

    # Check if combined parquet file exists
    combined_path = Path("data/cache/combined_rankings.parquet")
    if combined_path.exists():
        print("Loading pre-processed rankings data...")
        return pd.read_parquet(combined_path)
    
    # Otherwise process from CSV files
    rankings_dfs = []
    
    for decade in decades:
        filename = f"atp_rankings_{decade}.csv"
        try:
            file_path = download_file(filename)
            df = pd.read_csv(file_path)
            
            # Add decade column for reference
            df['decade'] = decade
            
            # Convert ranking_date to datetime
            if 'ranking_date' in df.columns:
                df['ranking_date'] = pd.to_datetime(df['ranking_date'], format="%Y%m%d", errors='coerce')
            elif 'date' in df.columns:
                df['ranking_date'] = pd.to_datetime(df['date'], format="%Y%m%d", errors='coerce')
                df = df.rename(columns={'date': 'ranking_date'})
            
            rankings_dfs.append(df)
            print(f"Loaded {len(df)} rankings from {decade}")
            
        except Exception as e:
            print(f"Error loading {filename}: {e}")
    
    if not rankings_dfs:
        raise ValueError("No ranking data could be loaded")
    
    # Combine all dataframes
    combined_rankings = pd.concat(rankings_dfs, ignore_index=True)
    
    # Standardize column names
    if 'player' in combined_rankings.columns and 'player_id' not in combined_rankings.columns:
        combined_rankings = combined_rankings.rename(columns={'player': 'player_id'})
    
    # Save combined result as parquet
    combined_rankings.to_parquet(combined_path)
    return combined_rankings

def find_player_by_name(players_df, name):
    """Find player by name, prioritizing exact matches over partial matches"""
    name_lower = name.lower()
    
    # First, try to find an exact full name match
    if ' ' in name_lower:
        # Split the input name into first and last parts
        parts = name_lower.split(' ', 1)
        first_part = parts[0]
        last_part = parts[1]
        
        # Try exact match on first and last name
        exact_matches = players_df[
            (players_df['first_name'].str.lower() == first_part) & 
            (players_df['last_name'].str.lower() == last_part)
        ]
        
        if len(exact_matches) > 0:
            return exact_matches
    
    # If no exact full name match or no space in name, try exact match on either first or last name
    exact_first_matches = players_df[players_df['first_name'].str.lower() == name_lower]
    if len(exact_first_matches) > 0:
        return exact_first_matches
    
    exact_last_matches = players_df[players_df['last_name'].str.lower() == name_lower]
    if len(exact_last_matches) > 0:
        return exact_last_matches
    
    # If no exact matches found, fall back to partial matching
    mask = pd.Series(False, index=players_df.index)
    
    if 'first_name' in players_df.columns:
        mask |= players_df['first_name'].str.lower().str.contains(name_lower, na=False)
    
    if 'last_name' in players_df.columns:
        mask |= players_df['last_name'].str.lower().str.contains(name_lower, na=False)
    
    return players_df[mask]

def get_player_ranking_history(rankings_df, player_id):
    """Extract ranking history for a specific player"""
    # Use 'player' column instead of 'player_id'
    player_rankings = rankings_df[rankings_df['player_id'] == player_id]
    return player_rankings.sort_values('ranking_date')

def reduce_mem_usage(df):
    """Reduce memory usage of DataFrame by optimizing data types"""
    start_mem = df.memory_usage().sum() / 1024**2
    print(f"Memory usage of dataframe is {start_mem:.2f} MB")
    
    for col in df.columns:
        col_type = df[col].dtype
        
        if col_type != object and col_type != 'datetime64[ns]':
            c_min = df[col].min()
            c_max = df[col].max()
            
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
    
    end_mem = df.memory_usage().sum() / 1024**2
    print(f"Memory usage after optimization is: {end_mem:.2f} MB")
    print(f"Decreased by {100 * (start_mem - end_mem) / start_mem:.1f}%")
    
    return df