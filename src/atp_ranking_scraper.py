import os
import time
import requests
import polars as pl
import pandas as pd
from bs4 import BeautifulSoup
import logging
from typing import Optional, List

logger = logging.getLogger(__name__)


def scrape_atp_rankings_by_date(date: str) -> Optional[pd.DataFrame]:
    """
    Scrape ATP singles rankings for a given date from the ATP Tour website.
    
    This function retrieves the ATP singles rankings table for a specified date,
    extracts player information including names, ranks, points, and player IDs,
    and returns the data as a structured DataFrame.
    
    Args:
        date (str): Date in YYYY-MM-DD format for which to scrape rankings
        
    Returns:
        Optional[pd.DataFrame]: DataFrame containing the rankings data with columns:
            - Rank: Player's numerical ranking
            - Player: Player's full name
            - rank_change: Change in ranking (up/down/unchanged)
            - Points: ATP ranking points
            - atp_id: Player's unique ATP ID code
            - atp_name: Player's name in URL-friendly format
            - ranking_date: The date of the rankings (as datetime)
            
        Returns None if no ranking table is found for the given date.
        
    Raises:
        requests.exceptions.HTTPError: If the HTTP request to ATP website fails
    """
    # Construct URL for ATP rankings page with date parameter
    url = f"https://www.atptour.com/en/rankings/singles?rankRange=0-5000&dateWeek={date}"
    headers = {"User-Agent": "Mozilla/5.0"}

    logger.info(f"Requesting {url}")
    response = requests.get(url, headers=headers)
    response.raise_for_status() # Raise exception for HTTP errors

    # Parse HTML content
    soup = BeautifulSoup(response.text, "html.parser")

    # Find the rankings table
    table = soup.find('table', class_='mega-table desktop-table non-live')
    if not table:
        logger.warning(f"No ranking table found for date {date}")
        return None
    
    # Extract table headers
    header_row = table.find("tr")
    headers_list = [th.get_text(strip=True) for th in header_row.find_all("th")][1:]

    # Extract table data
    data = []
    for tr in table.tbody.find_all('tr'):
        tds = tr.find_all('td')
        if not tds or len(tds) == 1:
            continue

        row = []
        for idx, td in enumerate(tds):
            if idx == 1:
                # Extract player name from the player cell
                name_li = td.find('li', class_='name center')
                player_name = name_li.get_text(strip=True) if name_li else ""

                # Extract rank change indicator (up/down/unchanged)
                rank_up = td.find('span', class_='rank-up')
                rank_down = td.find('span', class_='rank-down')
                if rank_up:
                    rank_change = rank_up.get_text(strip=True)
                elif rank_down:
                    rank_change = rank_down.get_text(strip=True)
                else:
                    rank_change = "0" # No change in ranking

                row.append(player_name)
                row.append(rank_change)
            else:
                # Extract text for other cells (rank, points, etc.)
                row.append(td.get_text(strip=True))

        # Extract player identifiers from profile URL
        player_td = tds[1]
        atp_id, atp_name, player_url = "", "", ""
        profile_link = player_td.find("a", href=True)
        if profile_link and "/players/" in profile_link['href']:
            parts = profile_link['href'].split('/')
            if len(parts) >= 6:
                atp_name = parts[3]  # URL-friendly player name
                atp_id = parts[4]    # Unique player ID
        
        row += [atp_id, atp_name]
        data.append(row)

    # Create DataFrame with all extracted columns    
    headers_full = headers_list[:2] + ['rank_change'] + headers_list[2:] + ['atp_id', 'atp_name']
    df = pd.DataFrame(data, columns=headers_full)
    
    # Add ranking date column
    df['ranking_date'] = pd.to_datetime(date)

    return df


def update_rankings(dates: List[str], raw_base_dir: str = "data/raw/rankings", sleep_sec: float = 1):
    """
    Update rankings data, only scraping dates that don't already exist in the raw folders.
    
    Args:
        dates: List of dates to scrape in YYYY-MM-DD format
        raw_base_dir: Base directory for storing raw ranking files
        sleep_sec: Seconds to sleep between requests to avoid being blocked
    """
    for date in dates:
        year = pd.to_datetime(date).year
        raw_dir = os.path.join(raw_base_dir, str(year))
        out_path = os.path.join(raw_dir, f"atp_rankings_{date.replace('-', '')}_raw.csv")
        
        # Skip if file already exists
        if os.path.exists(out_path):
            logger.info(f"Skipping {date}, file already exists: {out_path}")
            continue
            
        # Only scrape if file doesn't exist
        logger.info(f"Scraping rankings for {date}")
        df = scrape_atp_rankings_by_date(date)
        if df is not None and not df.empty:
            os.makedirs(raw_dir, exist_ok=True)
            df.to_csv(out_path, index=False)
            logger.info(f"Saved rankings for {date} to {out_path}")

        # Sleep to avoid being blocked
        logger.info(f"Sleeping for {sleep_sec} seconds before next request")
        time.sleep(sleep_sec)