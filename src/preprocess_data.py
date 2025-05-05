import pandas as pd
import numpy as np
import os
import glob
import re
from typing import Optional, Any


def extract_dob(value: Any) -> Optional[pd.Timestamp]:
    """
    Extract date of birth from various text formats.
    Handles formats like:
    - "37 (1987/05/22)"
    - "(1987/05/22)"
    - "1987/05/22"
    - "1987-05-22"
    - "22.05.1987"

    Args:
        value: The string or value to extract date from
    
    Returns:
        pandas.Timestamp or None: Extracted date or None if no valid date found
    """
    if pd.isna(value):
        return None
        
    if not isinstance(value, str):
        return None
    
    # Try different regex patterns for date extraction
    patterns = [
        r'\((\d{4}[/-]\d{1,2}[/-]\d{1,2})\)',  # (YYYY/MM/DD) or (YYYY-MM-DD)
        r'(\d{4}[/-]\d{1,2}[/-]\d{1,2})',      # YYYY/MM/DD or YYYY-MM-DD
        r'(\d{1,2}\.\d{1,2}\.\d{4})',          # DD.MM.YYYY
    ]
    
    for pattern in patterns:
        match = re.search(pattern, value)
        if match:
            date_str = match.group(1)
            try:
                # Handle different date formats
                if '.' in date_str:  # DD.MM.YYYY
                    parts = date_str.split('.')
                    date_str = f"{parts[2]}-{parts[1]}-{parts[0]}"
                
                return pd.to_datetime(date_str)
            except:
                # If this format fails, try the next pattern
                continue
    
    return None


def insert_nan_for_gaps(rankings_df: pd.DataFrame, max_gap_days: int) -> pd.DataFrame:
    """
    Insert NaN rows in rankings_df for gaps in ranking_date greater than max_gap_days.
    
    This function identifies large gaps in a player's ranking history and inserts
    NaN values at the midpoint of these gaps to ensure proper visualization when plotting.
    
    Args:
        rankings_df: DataFrame with columns ['atp_id', 'ranking_date', 'rank']
        max_gap_days: Maximum allowed gap in days before inserting NaN row
        
    Returns:
        DataFrame with NaN rows inserted for large gaps
    """
    # Create a copy to avoid modifying the original DataFrame
    rankings_df = rankings_df.sort_values(['atp_id', 'ranking_date']).copy()
    new_rows = []

    # Process each player separately
    for atp_id, group in rankings_df.groupby('atp_id'):
        dates = group['ranking_date'].values

        # Get the atp_name if it exists in the columns
        atp_name = None
        if 'atp_name' in group.columns:
            atp_names = group['atp_name'].unique()
            if len(atp_names) > 0:
                atp_name = atp_names[0]

        # Check for gaps between consecutive ranking dates
        for i in range(1, len(dates)):
            gap = (dates[i] - dates[i-1]).astype('timedelta64[D]').astype(int)
            if gap > max_gap_days:
                # Insert a NaN row at midpoint of the gap
                mid_date = dates[i-1] + (dates[i] - dates[i-1]) / 2
                new_rows.append({
                    'atp_id': atp_id,
                    'atp_name': atp_name,
                    'ranking_date': pd.Timestamp(mid_date),
                    'rank': np.nan
                })

    # Add the new rows to the original DataFrame if any were created
    if new_rows:
        nan_df = pd.DataFrame(new_rows)
        # Ensure all columns from original DataFrame are present
        for col in rankings_df.columns:
            if col not in ['atp_id', 'atp_name', 'ranking_date', 'rank']:
                nan_df[col] = None

        # Combine original data with new NaN rows and sort
        combined = pd.concat([rankings_df, nan_df], ignore_index=True)
        combined = combined.sort_values(['atp_id', 'ranking_date']).reset_index(drop=True)
        return combined
    else:
        return rankings_df


def preprocess_all(max_gap_days: int = 180) -> None:
    """
    Preprocess raw rankings, tournaments, and players data into efficient app-ready files.
    
    This function:
    1. Concatenates raw ranking files from multiple years
    2. Processes tournament data from various sources
    3. Extracts and standardizes player information
    4. Saves all processed data as compressed parquet files
    
    Args:
        max_gap_days: Maximum allowed gap in days before inserting NaN row in ranking data
    
    Returns:
        None: Files are saved to disk in the data/ directory
    """
    # --- RANKINGS PREPROCESSING ---
    ranking_files = glob.glob("data/raw/rankings/*/atp_rankings_*_raw.csv")
    rankings_out_path = "data/atp_rankings.parquet"
    
    if ranking_files:
        # Concatenate all ranking files
        rankings = pd.concat([pd.read_csv(f) for f in ranking_files], ignore_index=True)

        # Ensure column names are consistent (rename 'Rank' to 'rank' if needed)
        if 'Rank' in rankings.columns and 'rank' not in rankings.columns:
            rankings = rankings.rename(columns={'Rank': 'rank'})
        
        # Keep only necessary columns
        keep_cols = [c for c in ['ranking_date', 'rank', 'atp_id', 'atp_name'] if c in rankings.columns]
        rankings = rankings[keep_cols].drop_duplicates()
        
        # Convert ranking_date to datetime if it's not already
        rankings['ranking_date'] = pd.to_datetime(rankings['ranking_date'])

        # Clean rank values by removing 'T' for tied ranks
        if 'rank' in rankings.columns:
            rankings['rank'] = rankings['rank'].astype(str).str.replace('T', '').astype(int)
        
        # Insert NaN for gaps greater than max_gap_days
        rankings = insert_nan_for_gaps(rankings, max_gap_days)

        # Convert rank column to Int16 (can handle both integers and NaN)
        rankings['rank'] = rankings['rank'].astype('Int16')
        
        # Save processed rankings data
        rankings.to_parquet(rankings_out_path, compression='snappy', index=False)

    # --- TOURNAMENTS PREPROCESSING ---
    tournament_files = glob.glob("data/raw/tournaments/*/tournaments_*_raw.csv")
    tournaments_out_path = "data/atp_tournaments.parquet"
    
    if tournament_files:
        # Concatenate all tournament files
        tournaments = pd.concat([pd.read_csv(f) for f in tournament_files], ignore_index=True)
        
        # Keep only necessary columns
        keep_cols = [c for c in ['tournament_name', 'start_date', 'end_date', 'tournament_type', 'singles_winner_names', 'singles_winner_urls', 'venue'] if c in tournaments.columns]
        tournaments = tournaments[keep_cols]

        # Convert unhashable types (like lists) to hashable types before deduplication
        tournaments_for_dedup = tournaments.copy()
        for col in tournaments_for_dedup.columns:
            if tournaments_for_dedup[col].apply(lambda x: isinstance(x, (list, np.ndarray)) if not pd.isna(x) else False).any():
                tournaments_for_dedup[col] = tournaments_for_dedup[col].apply(
                    lambda x: tuple(x) if isinstance(x, (list, np.ndarray)) else x
                )

        # Get unique indices and use them to filter the original dataframe
        unique_indices = tournaments_for_dedup.drop_duplicates().index
        tournaments = tournaments.iloc[unique_indices]
        
        # Convert date columns to datetime
        for date_col in ['start_date', 'end_date']:
            if date_col in tournaments.columns:
                tournaments[date_col] = pd.to_datetime(tournaments[date_col])
        
        # Save processed tournament data
        tournaments.to_parquet(tournaments_out_path, compression='snappy', index=False)

    # --- PLAYERS PREPROCESSING ---
    players_raw_path = "data/raw/players/players_raw.parquet"
    players_out_path = "data/atp_players.parquet"
    
    if os.path.exists(players_raw_path):
        # Load raw player data
        players = pd.read_parquet(players_raw_path)

        # Create a new dob column from either age or existing dob
        if 'dob' not in players.columns:
            players['dob'] = None
        
        # Extract DOB from age column where dob is missing
        mask = players['dob'].isna() & players['age'].notna()
        players.loc[mask, 'dob'] = players.loc[mask, 'age'].apply(extract_dob)

        # Explicitly convert dob to datetime type
        players['dob'] = pd.to_datetime(players['dob'], errors='coerce')

        # Keep only necessary columns
        keep_cols = [c for c in ['atp_id', 'atp_name', 'full_name', 'dob', 'country_code'] if c in players.columns]
        players = players[keep_cols].drop_duplicates()
        
        # Save processed player data
        players.to_parquet(players_out_path, compression='snappy', index=False)