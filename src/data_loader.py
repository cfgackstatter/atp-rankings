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
    """Load player data from atp_players.csv with optimized memory usage"""
    players_file = download_file("atp_players.csv")
    
    # First time: load CSV and save as parquet
    parquet_path = Path("data/cache/players.parquet")

    if not parquet_path.exists():
        # Read CSV with minimal columns first to determine schema
        sample = pd.read_csv(players_file, nrows=5)

        # Define columns to read and their types
        usecols = ['player_id', 'name_first', 'name_last', 'dob', 'ioc']

        dtype_dict = {
            'player_id': 'int32',
            'name_first': 'str',
            'name_last': 'str',
            'hand': 'category',  # Categorical since limited options (R, L)
            'ioc': 'category',   # Country codes are categorical
            'wikidata_id': 'str'
        }

        # Read CSV with optimized settings
        players = pd.read_csv(
            players_file,
            low_memory=False,
            usecols=usecols,
            dtype=dtype_dict,
            parse_dates=['dob'],
            keep_default_na=False,
            na_values=['', 'NA', 'NULL']
        )
        
        # Replace NaN values with empty strings in name columns
        players['name_first'] = players['name_first'].fillna('')
        players['name_last'] = players['name_last'].fillna('')

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

        # Create name search columns for faster lookups - pre-compute lowercase versions
        players['name_lower_first'] = players['first_name'].str.lower()
        players['name_lower_last'] = players['last_name'].str.lower()

        # Combine names efficiently
        players['name_lower_full'] = players['name_lower_first'] + ' ' + players['name_lower_last']
        
        # Apply memory optimization
        players = reduce_mem_usage(players)

        # Save as parquet with optimized compression
        players.to_parquet(
            parquet_path,
            compression='snappy',  # Fast compression/decompression
            index=False           # Don't store the index
        )
    else:
        # Use parquet for subsequent loads (much faster)
        players = pd.read_parquet(parquet_path, memory_map=True)  # Use memory mapping for large files
    
    return players


def load_rankings(decades=None, force_download=False, fill_gaps=False, max_gap_days=5000):
    """
    Load all rankings data
    
    Args:
        decades: List of decades to load (if None, loads all)
        force_download: Whether to force download and reprocessing
        fill_gaps: Whether to insert NaN for gaps in player data
        max_gap_days: Maximum allowed gap in days before inserting NaN
        
    Returns:
        DataFrame with all rankings data
    """
    decades = decades or ['70s', '80s', '90s', '00s', '10s', '20s', 'current']

    # Check if combined parquet file exists
    combined_path = Path("data/cache/combined_rankings.parquet")
    
    if combined_path.exists() and not force_download:
        print("Loading pre-processed rankings data...")
        return pd.read_parquet(combined_path, memory_map=True)  # Use memory mapping
    
    # Otherwise process from CSV files
    temp_files = []
    
    for decade in decades:
        filename = f"atp_rankings_{decade}.csv"
        try:
            file_path = download_file(filename)

            # Check columns in the file (without loading the whole file)
            sample = pd.read_csv(file_path, nrows=5)
            date_col = 'ranking_date' if 'ranking_date' in sample.columns else 'date'

            # Define columns to read and their types - skip points column
            usecols = [date_col, 'rank', 'player']
            dtypes = {
                'player': 'int32',  # Use int32 for player IDs
                'rank': 'int16'     # Rankings won't exceed int16 range
            }

            # Process in chunks to reduce memory usage
            print(f"Processing {filename} in chunks...")
            
            # Create a temporary file for this decade
            temp_parquet = Path(f"data/cache/temp_{decade}.parquet")
            if temp_parquet.exists():
                os.remove(temp_parquet)

            # Process each chunk separately and write to a single parquet file
            chunks = []
            for chunk in pd.read_csv(file_path, usecols=usecols, dtype=dtypes, chunksize=250000):
                # Convert date to datetime
                if date_col == 'date':
                    chunk['ranking_date'] = pd.to_datetime(chunk['date'], format="%Y%m%d", errors='coerce')
                    chunk = chunk.drop(columns=['date'])
                else:
                    chunk['ranking_date'] = pd.to_datetime(chunk[date_col], format="%Y%m%d", errors='coerce')
                
                # Rename player column if needed
                if 'player' in chunk.columns and 'player_id' not in chunk.columns:
                    chunk = chunk.rename(columns={'player': 'player_id'})
                
                # Add to list of chunks
                chunks.append(chunk)
                
                # To save memory, write to parquet if we have accumulated enough chunks
                if len(chunks) >= 4:
                    combined_chunk = pd.concat(chunks, ignore_index=True)
                    combined_chunk = reduce_mem_usage(combined_chunk)
                    
                    # Write to parquet
                    combined_chunk.to_parquet(
                        temp_parquet,
                        compression='snappy',
                        index=False
                    )
                    
                    # Clear chunks to free memory
                    chunks = []
            
            # Process any remaining chunks
            if chunks:
                combined_chunk = pd.concat(chunks, ignore_index=True)
                combined_chunk = reduce_mem_usage(combined_chunk)
                
                # Write to parquet
                combined_chunk.to_parquet(
                    temp_parquet,
                    compression='snappy',
                    index=False
                )
            
            # Add to list of files to combine
            temp_files.append(temp_parquet)
            
        except Exception as e:
            print(f"Error loading {filename}: {e}")
    
    if not temp_files:
        raise ValueError("No ranking data could be loaded")
    
    # Combine all parquet files
    print("Combining all decades...")
    
    # Read and combine in chunks to avoid memory issues
    combined_rankings = pd.concat([pd.read_parquet(file) for file in temp_files], ignore_index=True)
    
    # Final memory optimization
    combined_rankings = reduce_mem_usage(combined_rankings)

    # After loading and combining all rankings data:
    if fill_gaps:
        combined_rankings = preprocess_rankings_with_gaps(combined_rankings, max_gap_days)
    
    # Save combined result
    combined_rankings.to_parquet(
        combined_path,
        compression='snappy',
        index=False
    )
    
    # Clean up temporary files
    for file in temp_files:
        if file.exists():
            os.remove(file)
    
    return combined_rankings


def find_player_by_name(players_df, name):
    """Find player by name, prioritizing exact matches over partial matches"""
    name_lower = name.lower()

    # Use query() for faster filtering when possible
    if 'name_lower_full' in players_df.columns:
        # First, try exact full name match
        exact_full_matches = players_df.query(f"name_lower_full == '{name_lower}'")
        if len(exact_full_matches) > 0:
            return exact_full_matches
    
    # If name contains a space, try matching first and last name parts
    if ' ' in name_lower:
        parts = name_lower.split(' ', 1)
        first_part = parts[0]
        last_part = parts[1]
        
        # Try exact match on first and last parts
        try:
            first_last_matches = players_df.query(
                f"name_lower_first == '{first_part}' and name_lower_last == '{last_part}'"
            )
            if len(first_last_matches) > 0:
                return first_last_matches
            
            # Try exact match on last and first parts (reversed order)
            last_first_matches = players_df.query(
                f"name_lower_first == '{last_part}' and name_lower_last == '{first_part}'"
            )
            if len(last_first_matches) > 0:
                return last_first_matches
        except Exception:
            # Fall back to standard filtering if query fails (e.g., special characters in name)
            first_last_matches = players_df[
                (players_df['name_lower_first'] == first_part) &
                (players_df['name_lower_last'] == last_part)
            ]
            if len(first_last_matches) > 0:
                return first_last_matches
            
            last_first_matches = players_df[
                (players_df['name_lower_first'] == last_part) &
                (players_df['name_lower_last'] == first_part)
            ]
            if len(last_first_matches) > 0:
                return last_first_matches
    
    # Try exact match on first or last name
    try:
        exact_first_matches = players_df.query(f"name_lower_first == '{name_lower}'")
        if len(exact_first_matches) > 0:
            return exact_first_matches
        
        exact_last_matches = players_df.query(f"name_lower_last == '{name_lower}'")
        if len(exact_last_matches) > 0:
            return exact_last_matches
    except Exception:
        # Fall back to standard filtering
        exact_first_matches = players_df[players_df['name_lower_first'] == name_lower]
        if len(exact_first_matches) > 0:
            return exact_first_matches
        
        exact_last_matches = players_df[players_df['name_lower_last'] == name_lower]
        if len(exact_last_matches) > 0:
            return exact_last_matches
    
    # Fall back to partial matching using contains with case=False for case insensitivity
    mask = (
        players_df['name_lower_first'].str.contains(name_lower, regex=False, na=False) |
        players_df['name_lower_last'].str.contains(name_lower, regex=False, na=False)
    )
    
    return players_df[mask]


def reduce_mem_usage(df):
    """Reduce memory usage of DataFrame by optimizing data types"""
    start_mem = df.memory_usage(deep=True).sum() / 1024**2
    print(f"Memory usage of dataframe is {start_mem:.2f} MB")
    
    # Specific optimizations for ATP rankings data
    type_optimizations = {
        'player_id': 'int32',  # Keep player IDs as integers
        'rank': 'int16',       # Rankings won't exceed int16 range
        'ranking_date': 'datetime64[ns]'  # Keep as datetime
    }
    
    # Apply specific optimizations first
    for col, dtype in type_optimizations.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                print(f"Could not convert {col} to {dtype}: {e}")
    
    # Then apply general optimizations to remaining columns
    for col in df.columns:
        # Skip columns we've already optimized
        if col in type_optimizations:
            continue
            
        # Skip datetime columns
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
            
        # Skip categorical columns
        if pd.api.types.is_categorical_dtype(df[col]):
            continue
            
        # For string columns, consider converting to categorical if few unique values
        if pd.api.types.is_object_dtype(df[col]):
            num_unique = df[col].nunique()
            num_total = len(df)
            if num_unique / num_total < 0.5:  # If less than 50% unique values
                df[col] = df[col].astype('category')
            continue
        
        # For numeric columns
        if pd.api.types.is_numeric_dtype(df[col]):
            c_min = df[col].min()
            c_max = df[col].max()
            
            # For integers
            if pd.api.types.is_integer_dtype(df[col]):
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
            # For floats
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
    
    end_mem = df.memory_usage(deep=True).sum() / 1024**2
    print(f"Memory usage after optimization is: {end_mem:.2f} MB")
    print(f"Decreased by {100 * (start_mem - end_mem) / start_mem:.1f}%")
    
    return df


def preprocess_rankings_with_gaps(rankings_df, max_gap_days=5000):
    """
    For each player, insert NaN rows for gaps in ranking data that exceed max_gap_days.
    
    Args:
        rankings_df: DataFrame containing ranking data
        max_gap_days: Maximum allowed gap in days before inserting NaN
        
    Returns:
        DataFrame with NaN values inserted for large gaps
    """
    print("Preprocessing rankings data to insert NaN for gaps...")
    
    # Create a copy to avoid modifying the original
    processed_df = rankings_df.copy()
    
    # Get unique player IDs
    player_ids = processed_df['player_id'].unique()
    
    # List to store DataFrames for each player (with gaps filled)
    player_dfs = []
    
    for player_id in player_ids:
        # Get data for this player
        player_data = processed_df[processed_df['player_id'] == player_id].sort_values('ranking_date')
        
        if len(player_data) <= 1:
            # No gaps to fill if only one or zero records
            player_dfs.append(player_data)
            continue
        
        # Calculate date differences
        player_data['date_diff'] = player_data['ranking_date'].diff().dt.days
        
        # Find gaps larger than max_gap_days
        gap_indices = player_data[player_data['date_diff'] > max_gap_days].index
        
        if len(gap_indices) == 0:
            # No large gaps for this player
            player_dfs.append(player_data.drop(columns=['date_diff']))
            continue
        
        # For each large gap, insert a NaN row
        rows_to_add = []
        
        for idx in gap_indices:
            # Get the dates before and after the gap
            prev_date = player_data.loc[player_data.index[player_data.index.get_loc(idx)-1], 'ranking_date']
            curr_date = player_data.loc[idx, 'ranking_date']
            
            # Calculate a date in the middle of the gap
            gap_middle_date = prev_date + (curr_date - prev_date) / 2
            
            # Create a new row with NaN rank
            new_row = {
                'player_id': player_id,
                'ranking_date': gap_middle_date,
                'rank': np.nan,
                'date_diff': np.nan
            }
            rows_to_add.append(new_row)
        
        # Add the new rows to the player's data
        for row in rows_to_add:
            player_data = pd.concat([player_data, pd.DataFrame([row])], ignore_index=True)
        
        # Sort again by date and drop the date_diff column
        player_data = player_data.sort_values('ranking_date').drop(columns=['date_diff'])
        
        # Add to the list of processed player DataFrames
        player_dfs.append(player_data)
    
    # Combine all player DataFrames
    result_df = pd.concat(player_dfs, ignore_index=True)
    
    print(f"Added {len(result_df) - len(rankings_df)} NaN rows for gaps > {max_gap_days} days")
    
    return result_df