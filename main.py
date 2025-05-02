import os
import argparse
import logging
import pandas as pd
from datetime import datetime, timedelta

from src.data_loader import load_players, load_rankings, find_player_by_name
from src.visualizer import plot_player_rankings
from src.atp_scraper_utils import scrape_atp_rankings_raw
from src.atp_scraper import map_and_combine_raw_files
from src.atp_tournament_scraper import update_tournament_parquet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mondays_2025_until_today():
    today = datetime.today()
    mondays = []
    d = datetime(2025, 1, 1)
    d += timedelta(days=(0 - d.weekday()) % 7)
    while d <= today:
        mondays.append(d.strftime('%Y-%m-%d'))
        d += timedelta(days=7)
    return mondays

def main():
    parser = argparse.ArgumentParser(description='ATP RankTracker')

    parser.add_argument('--players', type=str, nargs='+',
                        help='One or more player names to visualize (use quotes for full names, e.g., "Roger Federer")')
    
    parser.add_argument('--save', type=str, help='Save the plot to specified path')

    parser.add_argument('--force-download', action='store_true',
                        help='Force download of data files even if cached')
    
    parser.add_argument('--by-age', action='store_true',
                        help='Plot rankings against player age instead of date')
    
    parser.add_argument('--scrape-atp', action='store_true',
                        help='Scrape new ATP 2025 ranking data and update CSVs')
    
    args = parser.parse_args()

    if args.scrape_atp:
        players_df = load_players()
        raw_dir = "data/atp_rankings_2025_raw"
        os.makedirs(raw_dir, exist_ok=True)
        mondays = get_mondays_2025_until_today()
        # Download missing weeks as raw files
        for week_date in mondays:
            raw_path = f"{raw_dir}/atp_rankings_2025_{week_date.replace('-', '')}_raw.csv"
            if not os.path.exists(raw_path):
                logger.info(f"Scraping for {week_date}...")
                try:
                    scrape_atp_rankings_raw(week_date, raw_dir=raw_dir)
                except Exception as e:
                    logger.error(f"Failed to scrape {week_date}: {e}")
        # Combine and map all raw files
        map_and_combine_raw_files(players_df, raw_dir=raw_dir)
        # Update combined parquet
        load_rankings(force_download=True)
        logger.info("Done updating ATP 2025 rankings.")
        # Scrape tournaments for all years and types you want:
        years = list(range(1970, datetime.now().year + 1))
        tournament_types = ['gs', 'atp', 'ch', 'fu']
        update_tournament_parquet(years, tournament_types)
        return
    
    # Load player data
    logger.info("Loading player data...")
    players_df = load_players()
    
    # Load rankings data
    logger.info("Loading rankings data...")
    rankings_df = load_rankings()

    # Process player names
    player_ids = []
    player_names = []
    
    for name in args.players:
        player_matches = find_player_by_name(players_df, name)
        if len(player_matches) == 0:
            logger.warning(f"No players found with name: {name}")
            continue
        elif len(player_matches) > 1:
            logger.info(f"Multiple players found with name: {name}")
            for _, player in player_matches.iterrows():
                first_name = "" if pd.isna(player['first_name']) else player['first_name']
                last_name = "" if pd.isna(player['last_name']) else player['last_name']
                country_code = "" if pd.isna(player.get('country_code', '')) else player.get('country_code', '')
                birth_year = ""
                if 'birth_date' in player and not pd.isna(player['birth_date']):
                    try:
                        birth_year = f" - *{pd.to_datetime(player['birth_date']).year}"
                    except:
                        pass
                display_name = f"{last_name}, {first_name}"
                if country_code:
                    display_name += f" ({country_code})"
                display_name += birth_year
                logger.info(f"  {display_name} (ID: {player['player_id']})")
            player = player_matches.iloc[0]
            first_name = "" if pd.isna(player['first_name']) else player['first_name']
            last_name = "" if pd.isna(player['last_name']) else player['last_name']
            logger.info(f"Using the first match: {first_name} {last_name}")
        else:
            player = player_matches.iloc[0]
        first_name = "" if pd.isna(player['first_name']) else player['first_name']
        last_name = "" if pd.isna(player['last_name']) else player['last_name']
        player_name = f"{first_name} {last_name}".strip()
        player_ids.append(player['player_id'])
        player_names.append(player_name)
    if not player_ids:
        logger.error("No valid players found. Please check the player names and try again.")
        return
    logger.info(f"Plotting ranking history for {len(player_ids)} players...")
    plot_player_rankings(rankings_df, players_df, player_ids, player_names, args.save, by_age=args.by_age)
            
if __name__ == "__main__":
    main()