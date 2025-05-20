import polars as pl
import pandas as pd
import numpy as np
import os
import glob
import re
from typing import Optional, Any


def extract_dob(value: Any) -> Optional[str]:
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
        pl.Date or None: Extracted date or None if no valid date found
    """
    if value is None or pd.isna(value):
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
                else:
                    # Standardize separator to '-'
                    date_str = date_str.replace('/', '-')
                
                # Validate by parsing with pandas
                pd_date = pd.to_datetime(date_str)
                # Return as string in YYYY-MM-DD format
                return pd_date.strftime('%Y-%m-%d')
            except:
                # If this format fails, try the next pattern
                continue
    
    return None


def insert_nan_for_gaps(rankings_df: pl.DataFrame, max_gap_days: int) -> pl.DataFrame:
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
    # Sort by atp_id and ranking_date
    rankings_df = rankings_df.sort(["atp_id", "ranking_date"])

    # Process each player separately and collect new rows
    new_rows = []

    # Group by atp_id
    for atp_id in rankings_df.get_column("atp_id").unique():
        # Filter for this player
        group = rankings_df.filter(pl.col("atp_id") == atp_id)
        dates = group.get_column("ranking_date").to_numpy()
        
        # Get the atp_name if it exists in the columns
        atp_name = None
        if "atp_name" in group.columns:
            atp_names = group.get_column("atp_name").unique()
            if len(atp_names) > 0:
                atp_name = atp_names[0]
        
        # Check for gaps between consecutive ranking dates
        for i in range(1, len(dates)):
            # Convert timedelta64 to days using division by numpy.timedelta64(1, 'D')
            gap = (dates[i] - dates[i-1]) / np.timedelta64(1, 'D')
            
            if gap > max_gap_days:
                # Insert a NaN row at midpoint of the gap
                mid_date = dates[i-1] + (dates[i] - dates[i-1]) / 2
                new_rows.append({
                    "atp_id": atp_id,
                    "atp_name": atp_name,
                    "ranking_date": pd.Timestamp(mid_date),  # Convert to pandas Timestamp first
                    "rank": None
                })
    
    # Add the new rows to the original DataFrame if any were created
    if new_rows:
        # Create a DataFrame from new rows
        nan_df = pl.DataFrame(new_rows)
        
        # Ensure all columns from original DataFrame are present
        for col in rankings_df.columns:
            if col not in ["atp_id", "atp_name", "ranking_date", "rank"]:
                nan_df = nan_df.with_columns(pl.lit(None).alias(col))
        
        # Ensure ranking_date has the correct type
        dtype_map = {col: rankings_df.schema[col] for col in rankings_df.columns}
        
        # Convert columns to match the original DataFrame's schema
        for col, dtype in dtype_map.items():
            if col in nan_df.columns:
                # For datetime columns, need special handling
                if str(dtype).startswith("Datetime"):
                    nan_df = nan_df.with_columns(
                        pl.col(col).cast(pl.Datetime(time_unit="us"))
                    )
        
        # Get the column order from the original DataFrame
        column_order = rankings_df.columns
        
        # Ensure nan_df has the same column order
        nan_df = nan_df.select(column_order)
        
        # Combine original data with new NaN rows and sort
        combined = pl.concat([rankings_df, nan_df], how="vertical_relaxed")
        combined = combined.sort(["atp_id", "ranking_date"])
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
        # Read all CSV files with schema override for the Rank column
        rankings_list = [
            pl.read_csv(
                f,
                schema_overrides={"Rank": pl.Utf8},  # Read Rank as string initially
                infer_schema_length=10000
            ) 
            for f in ranking_files
        ]
        rankings = pl.concat(rankings_list, how="vertical_relaxed")

        # Ensure column names are consistent (rename 'Rank' to 'rank' if needed)
        if 'Rank' in rankings.columns and 'rank' not in rankings.columns:
            rankings = rankings.rename({"Rank": "rank"})
        
        # Keep only necessary columns
        keep_cols = [c for c in ['ranking_date', 'rank', 'atp_id', 'atp_name'] if c in rankings.columns]
        rankings = rankings.select(keep_cols).unique()
        
        # Convert ranking_date to datetime
        rankings = rankings.with_columns(
            pl.col("ranking_date").str.to_datetime()
        )

        # Clean rank values by removing 'T' for tied ranks and convert to Int16
        if 'rank' in rankings.columns:
            rankings = rankings.with_columns(
                pl.col("rank").str.replace("T", "").cast(pl.Int16)
            )
        
        # Insert NaN for gaps greater than max_gap_days
        rankings = insert_nan_for_gaps(rankings, max_gap_days)
        
        # Save processed rankings data
        rankings.write_parquet(rankings_out_path, compression="snappy")

    # --- TOURNAMENTS PREPROCESSING ---
    tournament_files = glob.glob("data/raw/tournaments/*/tournaments_*_raw.csv")
    tournaments_out_path = "data/atp_tournaments.parquet"
    
    if tournament_files:
        # Read all CSV files in parallel
        tournaments_list = [pl.read_csv(f) for f in tournament_files]

        # Use diagonal concatenation to handle different column sets
        tournaments = pl.concat(tournaments_list, how="diagonal")
        
        # Keep only necessary columns
        keep_cols = [c for c in ['tournament_name', 'start_date', 'end_date', 'tournament_type',
                                'singles_winner_names', 'singles_winner_urls', 'venue'] if c in tournaments.columns]
        tournaments = tournaments.select(keep_cols)

        # For deduplication, convert to pandas temporarily
        # This is because handling lists in deduplication is complex
        pd_tournaments = tournaments.to_pandas()

        # Convert unhashable types (like lists) to hashable types before deduplication
        pd_tournaments_for_dedup = pd_tournaments.copy()
        for col in pd_tournaments_for_dedup.columns:
            if pd_tournaments_for_dedup[col].apply(lambda x: isinstance(x, (list, np.ndarray)) if not pd.isna(x) else False).any():
                pd_tournaments_for_dedup[col] = pd_tournaments_for_dedup[col].apply(
                    lambda x: tuple(x) if isinstance(x, (list, np.ndarray)) else x
                )

        # Get unique indices and use them to filter the original dataframe
        unique_indices = pd_tournaments_for_dedup.drop_duplicates().index
        pd_tournaments = pd_tournaments.iloc[unique_indices]
        
        # Convert back to polars
        tournaments = pl.from_pandas(pd_tournaments)
        
        # Convert date columns to datetime
        for date_col in ['start_date', 'end_date']:
            if date_col in tournaments.columns:
                tournaments = tournaments.with_columns(
                    pl.col(date_col).str.to_datetime()
                )
        
        # Save processed tournament data
        tournaments.write_parquet(tournaments_out_path, compression="snappy")

    # --- PLAYERS PREPROCESSING ---
    players_raw_path = "data/raw/players/players_raw.parquet"
    players_out_path = "data/atp_players.parquet"
    
    if os.path.exists(players_raw_path):
        # Load raw player data
        players = pl.read_parquet(players_raw_path)
        
        # Create a pandas DataFrame for easier processing
        pd_players = players.to_pandas()
        
        # Process DOB from both dob and age columns
        if 'dob' in pd_players.columns:
            # First, convert any existing DOB strings to proper date format
            # Only process rows where dob is not null and is a string
            mask_dob = (~pd_players['dob'].isna()) & (pd_players['dob'].apply(lambda x: isinstance(x, str)))
            pd_players.loc[mask_dob, 'dob'] = pd_players.loc[mask_dob, 'dob'].apply(
                lambda x: pd.to_datetime(x.replace('/', '-')) if isinstance(x, str) else x
            )
        
        # Then try to extract DOB from age column where dob is missing
        if 'age' in pd_players.columns:
            mask_age = pd_players['dob'].isna() & (~pd_players['age'].isna())
            pd_players.loc[mask_age, 'dob'] = pd_players.loc[mask_age, 'age'].apply(extract_dob)
            # Convert extracted string dates to datetime
            pd_players.loc[mask_age, 'dob'] = pd.to_datetime(pd_players.loc[mask_age, 'dob'], errors='coerce')
        
        # Convert back to polars
        players = pl.from_pandas(pd_players)
        
        # Convert datetime column to Polars Date type
        if 'dob' in players.columns:
            players = players.with_columns(
                pl.col("dob").cast(pl.Date)
            )
        
        # Keep only necessary columns
        keep_cols = [c for c in ['atp_id', 'atp_name', 'full_name', 'dob', 'country_code'] if c in players.columns]
        players = players.select(keep_cols).unique()
        
        # Save processed player data
        players.write_parquet(players_out_path, compression="snappy")