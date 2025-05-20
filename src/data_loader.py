import polars as pl
import pandas as pd
from typing import Union, Optional
from pandas import DataFrame, Timestamp


def load_players() -> pl.DataFrame:
    """
    Load preprocessed player data for the app.
    
    Returns:
        pl.DataFrame: Player data containing columns such as 'atp_id', 'atp_name',
                  'full_name', 'dob', and 'country_code'
    """
    return pl.read_parquet("data/atp_players.parquet")


def load_rankings() -> pl.DataFrame:
    """
    Load preprocessed rankings data for the app.
    
    Returns:
        pl.DataFrame: Rankings data containing columns such as 'atp_id', 'atp_name',
                  'ranking_date', and 'rank'
    """
    return pl.read_parquet("data/atp_rankings.parquet")


def load_tournaments() -> pl.DataFrame:
    """
    Load preprocessed tournament data for the app.
    
    Converts string representations of lists (from parquet storage) back into 
    actual Python lists for the winner names and URLs columns.
    
    Returns:
        pl.DataFrame: Tournament data containing columns such as 'tournament_name',
                  'start_date', 'end_date', 'tournament_type', 'singles_winner_names',
                  'singles_winner_urls', and 'venue'
    """
    import ast

    # Load the raw data from parquet file
    df = pl.read_parquet("data/atp_tournaments.parquet")
    
    # Convert string representations of lists to actual lists
    if 'singles_winner_names' in df.columns:
        df = df.with_columns(
            pl.col('singles_winner_names').map_elements(
                lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith('[') else x,
                return_dtype=pl.Object  # Specify the return type
            )
        )

    if 'singles_winner_urls' in df.columns:
        df = df.with_columns(
            pl.col('singles_winner_urls').map_elements(
                lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith('[') else x,
                return_dtype=pl.Object  # Specify the return type
            )
        )

    return df


def interpolate_rank_at_date(player_data: pl.DataFrame, target_date: pl.Date) -> float:
    """
    Interpolate the rank value for the target_date between the two nearest ranking dates.
    
    Args:
        player_data: DataFrame containing a player's ranking history,
                    with columns 'ranking_date' and 'rank'
        target_date: The date for which to interpolate the rank
    
    Returns:
        float: The interpolated rank value at the target date
    """
    # Convert to pandas for now as this function is more complex to rewrite
    # and is called per tournament marker, not in bulk processing
    pdf = player_data.to_pandas()
    target_date_pd = pd.Timestamp(target_date)

    # If target_date is before the first ranking date, return the first rank
    if target_date_pd <= pdf['ranking_date'].iloc[0]:
        return float(pdf['rank'].iloc[0])
    
    # If target_date is after the last ranking date, return the last rank
    if target_date_pd >= pdf['ranking_date'].iloc[-1]:
        return float(pdf['rank'].iloc[-1])

    # Find the two ranking dates surrounding the target_date
    before = pdf[pdf['ranking_date'] <= target_date_pd].iloc[-1]
    after = pdf[pdf['ranking_date'] > target_date_pd].iloc[0]

    # Linear interpolation
    total_days = (after['ranking_date'] - before['ranking_date']).days
    if total_days == 0:
        return float(before['rank'])  # Same date, no interpolation needed
    
    # Calculate proportion of time elapsed and apply to rank difference
    days_since_before = (target_date_pd - before['ranking_date']).days
    rank_diff = after['rank'] - before['rank']
    interpolated_rank = before['rank'] + (rank_diff * days_since_before / total_days)
    
    return float(interpolated_rank)