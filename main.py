import argparse
import logging
from typing import List
from datetime import datetime, timedelta
import asyncio

from src.atp_ranking_scraper import update_rankings, scrape_atp_rankings_by_date
from src.atp_tournament_scraper import update_tournaments, scrape_atp_events
from src.atp_player_scraper import scrape_atp_player_details, update_players_from_rankings
from src.preprocess_data import preprocess_all

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_mondays_between(start_date: datetime, end_date: datetime) -> List[str]:
    """
    Return all Mondays between start_date and end_date as YYYY-MM-DD strings.
    
    This function calculates all Monday dates that fall within the given date range,
    inclusive of both the start and end dates if they are Mondays.
    
    Args:
        start_date: The starting date of the range
        end_date: The ending date of the range
        
    Returns:
        A list of strings representing Monday dates in 'YYYY-MM-DD' format
    """
    mondays = []
    # Find the first Monday on or after start_date
    d = start_date
    d += timedelta(days=(0 - d.weekday()) % 7)
    
    # Collect all Mondays until end_date
    while d <= end_date:
        mondays.append(d.strftime('%Y-%m-%d'))
        d += timedelta(days=7)
    
    return mondays


def main():
    parser = argparse.ArgumentParser(description='ATP RankTracker')
    parser.add_argument('--scrape-atp', action='store_true', help='Scrape all new ATP ranking, tournament, and player data, then preprocess.')
    parser.add_argument('--scrape-players', type=int, default=0, help='Scrape N player details, prioritizing higher-ranked recent players')
    parser.add_argument('--test-scrape', action='store_true', help='Quick test scrape for a single date/year/player (prints, does not write).')
    args = parser.parse_args()

    if args.scrape_atp:
        # 1. Scrape rankings (all Mondays from 1973 to today)
        start_year = 2000
        today = datetime.today()
        all_mondays = []
        for year in range(start_year, today.year + 1):
            start_date = datetime(year, 1, 1)
            end_date = datetime(year, 12, 31) if year < today.year else today
            all_mondays.extend(get_mondays_between(start_date, end_date))
        logger.info(f"Scraping rankings for {len(all_mondays)} Mondays...")
        update_rankings(all_mondays)

        # 2. Scrape tournaments (all years and types)
        years = list(range(start_year, today.year + 1))
        tournament_types = ['gs', 'atp', 'ch', 'fu']
        logger.info(f"Scraping tournaments for years {years} and types {tournament_types}...")
        update_tournaments(years, tournament_types)

        # 3. Preprocess all data for app use
        logger.info("Preprocessing all raw data for app use...")
        preprocess_all()
        logger.info("Done updating all ATP data.")
        return
    
    if args.scrape_players > 0:
        logger.info(f"Scraping up to {args.scrape_players} player details...")
        remaining = update_players_from_rankings(max_players=args.scrape_players)
        
        # Preprocess player data
        logger.info("Preprocessing player data...")
        preprocess_all()
        
        logger.info(f"Finished scraping players. {remaining} players remain without data.")
        return
    
    if args.test_scrape:
        # Test scrape for a single ranking date, tournament year/type, and one player (no file output)
        test_date = "2024-04-22"
        test_year = 2024
        test_ttype = 'atp'
        logger.info("Testing ranking scrape...")
        df_ranking = scrape_atp_rankings_by_date(test_date)
        if df_ranking is not None:
            print("Ranking scrape result:")
            print(df_ranking.head())
        else:
            print("No ranking data scraped.")

        logger.info("Testing tournament scrape...")
        df_tournament = scrape_atp_events(test_year, test_ttype)
        if df_tournament is not None and not df_tournament.empty:
            print("Tournament scrape result:")
            print(df_tournament.head())
        else:
            print("No tournament data scraped.")

        logger.info("Testing player scrape...")
        # Use a known player URL from the ATP site for testing
        test_player_url = "https://www.atptour.com/en/players/roger-federer/f324/overview"
        details = asyncio.run(scrape_atp_player_details(test_player_url))
        print("Player scrape result:")
        print(details)
        return


if __name__ == "__main__":
    main()