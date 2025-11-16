import argparse
import logging
import asyncio
from datetime import datetime

from src.atp_ranking_scraper import update_rankings, scrape_atp_rankings_by_date, get_available_ranking_dates
from src.atp_tournament_scraper import update_tournaments, scrape_atp_events
from src.atp_player_scraper import scrape_atp_player_details, update_players_from_rankings
from src.preprocess_data import preprocess_all

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='ATP RankTracker')
    parser.add_argument('--scrape-atp', type=int, metavar='YEAR',
                       help='Scrape ATP data starting from YEAR (e.g., --scrape-atp 2025)')
    parser.add_argument('--scrape-players', type=int, default=0, metavar='N',
                       help='Scrape N player details, prioritizing higher-ranked recent players')
    parser.add_argument('--test-scrape', action='store_true',
                       help='Quick test scrape for a single date/year/player (prints, does not write).')
    
    args = parser.parse_args()
    
    if args.scrape_atp:
        start_year = args.scrape_atp
        
        # 1. Get available ranking dates from ATP website, filtered by start year
        logger.info(f"Fetching available ranking dates from {start_year} onwards...")
        ranking_dates = get_available_ranking_dates(start_year=start_year)
        
        if not ranking_dates:
            logger.error(f"Failed to retrieve ranking dates from {start_year}. Exiting.")
            return
        
        logger.info(f"Scraping rankings for {len(ranking_dates)} dates from {start_year}...")
        update_rankings(ranking_dates)
        
        # 2. Scrape tournaments from start year onwards
        today = datetime.today()
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
    
    # If no arguments provided, show help
    parser.print_help()


if __name__ == "__main__":
    main()