import pandas as pd
from typing import Union, Optional
from pandas import DataFrame, Timestamp


def load_players() -> DataFrame:
    """
    Load preprocessed player data for the app.
    
    Returns:
        DataFrame: Player data containing columns such as 'atp_id', 'atp_name',
                  'full_name', 'dob', and 'country_code'
    """
    return pd.read_parquet("data/atp_players.parquet")


def load_rankings() -> DataFrame:
    """
    Load preprocessed rankings data for the app.
    
    Returns:
        DataFrame: Rankings data containing columns such as 'atp_id', 'atp_name',
                  'ranking_date', and 'rank'
    """
    return pd.read_parquet("data/atp_rankings.parquet")


def load_tournaments() -> DataFrame:
    """
    Load preprocessed tournament data for the app.
    
    Converts string representations of lists (from parquet storage) back into 
    actual Python lists for the winner names and URLs columns.
    
    Returns:
        DataFrame: Tournament data containing columns such as 'tournament_name',
                  'start_date', 'end_date', 'tournament_type', 'singles_winner_names',
                  'singles_winner_urls', and 'venue'
    """
    import ast

    # Load the raw data from parquet file
    df = pd.read_parquet("data/atp_tournaments.parquet")
    
    # Convert string representations of lists to actual lists
    if 'singles_winner_names' in df.columns:
        df['singles_winner_names'] = df['singles_winner_names'].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith('[') else x
        )

    if 'singles_winner_urls' in df.columns:
        df['singles_winner_urls'] = df['singles_winner_urls'].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith('[') else x
        )

    return df


def interpolate_rank_at_date(player_data: pd.DataFrame, target_date: pd.Timestamp) -> float:
    """
    Interpolate the rank value for the target_date between the two nearest ranking dates.
    
    Uses linear interpolation to estimate a player's rank at a specific date that
    falls between two known ranking dates. This is useful for placing tournament
    markers precisely on the ranking curve.
    
    Args:
        player_data: DataFrame containing a player's ranking history,
                    with columns 'ranking_date' and 'rank'
        target_date: The date for which to interpolate the rank
    
    Returns:
        float: The interpolated rank value at the target date
        
    Note:
        Assumes player_data is sorted by 'ranking_date'.
        If target_date is outside the range of available dates, returns
        the first or last available rank.
    """
    # If target_date is before the first ranking date, return the first rank
    if target_date <= player_data['ranking_date'].iloc[0]:
        return player_data['rank'].iloc[0]
    
    # If target_date is after the last ranking date, return the last rank
    if target_date >= player_data['ranking_date'].iloc[-1]:
        return player_data['rank'].iloc[-1]

    # Find the two ranking dates surrounding the target_date
    before = player_data[player_data['ranking_date'] <= target_date].iloc[-1]
    after = player_data[player_data['ranking_date'] > target_date].iloc[0]

    # Linear interpolation
    total_days = (after['ranking_date'] - before['ranking_date']).days
    if total_days == 0:
        return before['rank']  # Same date, no interpolation needed
    
    # Calculate proportion of time elapsed and apply to rank difference
    days_since_before = (target_date - before['ranking_date']).days
    rank_diff = after['rank'] - before['rank']
    interpolated_rank = before['rank'] + (rank_diff * days_since_before / total_days)
    
    return interpolated_rank