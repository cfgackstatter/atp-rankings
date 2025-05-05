import asyncio
from requests_html import AsyncHTMLSession
from bs4 import BeautifulSoup
from typing import Dict, Any, Optional
import pandas as pd
import os
import glob
import logging

logger = logging.getLogger(__name__)


async def scrape_atp_player_details(player_url: str) -> Optional[Dict[str, Any]]:
    """
    Scrape all personal details for an ATP player from their overview page.

    Args:
        player_url (str): Full URL to the player's overview page.
        
    Returns:
        dict: Extracted personal details {label: value, ...}, or None if not found.
    """
    asession = AsyncHTMLSession()
    logger.info(f"Scraping player details from {player_url}")
    try:
        response = await asession.get(player_url)
        await response.html.arender(timeout=20, sleep=2)
        soup = BeautifulSoup(response.html.raw_html, 'html.parser')
        
        # Extract player details
        details = {}

        # Extract full name from title tag
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.text
            # Title format is typically "Player Name | Overview | ATP Tour | Tennis"
            full_name = title_text.split('|')[0].strip()
            details['full_name'] = full_name

        # Extract personal details from pd_content section
        pd_content = soup.find('div', class_='pd_content')
        if not pd_content:
            logger.warning("No personal details section (.pd_content) found.")
            return None

        for li in pd_content.find_all('li'):
            spans = li.find_all('span', recursive=False)
            if len(spans) == 2:
                label = spans[0].get_text(strip=True).replace(" ", "_").lower()
                value = spans[1].get_text(strip=True)
                if 'flag' in spans[1].get('class', []):
                    value = spans[1].get_text(strip=True).split(' ', 1)[0]
                    flag_ref = spans[1].find('use').get('href')
                    if '#flag-' in flag_ref:
                        country_code = flag_ref.split('#flag-')[1].upper()
                        details['country_code'] = country_code
                details[label] = value
            elif len(spans) == 2 and "social" in spans[1].decode():
                social_links = {}
                for a in spans[1].find_all('a', href=True):
                    platform = a.find('span', class_='hide-text')
                    if platform:
                        social_links[platform.get_text(strip=True).lower()] = a['href']
                details['social_links'] = social_links
            elif li.find('a', href=True) and not spans:
                a = li.find('a', href=True)
                platform = a.find('span', class_='hide-text')
                if platform:
                    details.setdefault('social_links', {})[platform.get_text(strip=True).lower()] = a['href']
            elif len(spans) == 1:
                label = spans[0].get_text(strip=True).replace(" ", "_").lower()
                details[label] = ""

        url_parts = player_url.strip('/').split('/')
        if len(url_parts) >= 5:
            details['atp_name'] = url_parts[-3]
            details['atp_id'] = url_parts[-2]
        return details
    except Exception as e:
        logger.error(f"Failed to scrape player details from {player_url}: {e}")
        return None
    finally:
        await asession.close()


def prioritize_players(rankings_df: pd.DataFrame, n: int = 100, exclude_ids=None):
    """
    Prioritize players by rank first, then by date, excluding specified IDs.
    
    Args:
        rankings_df (pd.DataFrame): DataFrame with 'atp_id', 'ranking_date', and 'rank' columns
        n (int): Number of players to return
        exclude_ids (set): Set of atp_ids to exclude from prioritization
        
    Returns:
        list: List of atp_ids in priority order
    """
    # Filter out excluded IDs if provided
    if exclude_ids:
        rankings_df = rankings_df[~rankings_df['atp_id'].isin(exclude_ids)].copy()

    # Convert rank to integer by removing 'T' for tied ranks
    rankings_df['rank'] = rankings_df['Rank'].astype(str).str.replace('T', '').astype(int)
    
    # Sort by rank (ascending) and date (ascending)
    sorted_df = rankings_df.sort_values(['rank', 'ranking_date'],
                                        ascending=[True, True])
    
    # Drop duplicates to keep one entry per player (the one with the best rank)
    unique_players = sorted_df.drop_duplicates('atp_id', keep='first')
    
    # Return the top N player IDs
    return unique_players['atp_id'].head(n).tolist()


def update_players_from_rankings(
    rankings_base_dir: str = "data/raw/rankings",
    players_parquet: str = "data/raw/players/players_raw.parquet",
    max_players: int = None
):
    """
    Update the players_raw.parquet with new player URLs found in rankings files.
    Supports prioritization and limiting the number of players to scrape.
    
    Args:
        rankings_base_dir (str): Directory containing raw ranking CSV files.
        players_parquet (str): Path to the players parquet file.
        max_players (int or None): Maximum number of players to scrape. If None, scrape all new players.
        
    Returns:
        int: Number of remaining players without data after scraping.
    """
    logger.info("Updating players from rankings with prioritization and limit...")

    # Gather all ranking files
    ranking_files = glob.glob(os.path.join(rankings_base_dir, "*", "atp_rankings_*_raw.csv"))
    rankings_list = []
    for f in ranking_files:
        df = pd.read_csv(f)
        rankings_list.append(df)
    if not rankings_list:
        logger.warning("No ranking files found.")
        return 0
    rankings_df = pd.concat(rankings_list, ignore_index=True)
    
    # Ensure ranking_date is datetime
    rankings_df['ranking_date'] = pd.to_datetime(rankings_df['ranking_date'])

    # Load existing players first to check for duplicates
    if os.path.exists(players_parquet):
        players_df = pd.read_parquet(players_parquet)
        known_urls = set(players_df['player_url'].dropna().unique())
        known_ids = set(players_df['atp_id'].dropna().unique())
    else:
        players_df = pd.DataFrame()
        known_urls = set()
        known_ids = set()
    
    # Prioritize players if max_players is specified
    if max_players is not None:
        priority_ids = prioritize_players(rankings_df, max_players, exclude_ids=known_ids)
        
        # Generate player URLs for prioritized players
        if priority_ids:
            player_urls_df = rankings_df[rankings_df['atp_id'].isin(priority_ids)].drop_duplicates('atp_id')
            player_urls_with_ids = [(
                f"https://www.atptour.com/en/players/{row['atp_name'].replace('.', '')}/{row['atp_id']}/overview",
                row['atp_id']
            ) for _, row in player_urls_df.iterrows() if not pd.isna(row['atp_name'])]
        else:
            player_urls_with_ids = []
    else:
        # Generate all player URLs with IDs
        player_urls_df = rankings_df.drop_duplicates('atp_id')
        player_urls_with_ids = [(
            f"https://www.atptour.com/en/players/{row['atp_name'].replace('.', '')}/{row['atp_id']}/overview",
            row['atp_id']
        ) for _, row in player_urls_df.iterrows() if not pd.isna(row['atp_name'])]
    
    # Filter new URLs by both URL and ID
    new_urls_with_ids = [(url, atp_id) for url, atp_id in player_urls_with_ids 
                         if url not in known_urls and atp_id not in known_ids]
    
    # Limit to max_players if specified
    if max_players is not None and len(new_urls_with_ids) > max_players:
        new_urls_with_ids = new_urls_with_ids[:max_players]
    
    logger.info(f"Found {len(new_urls_with_ids)} new player URLs to scrape.")
    
    new_players = []
    for url, _ in new_urls_with_ids:
        try:
            details = asyncio.run(scrape_atp_player_details(url))
            if details:
                details['player_url'] = url
                new_players.append(details)
        except Exception as e:
            logger.warning(f"Failed to scrape player {url}: {e}")
    
    if new_players:
        new_df = pd.DataFrame(new_players)
        if not players_df.empty:
            combined = pd.concat([players_df, new_df], ignore_index=True)
        else:
            combined = new_df
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(players_parquet), exist_ok=True)
        
        combined.to_parquet(players_parquet, index=False)
        logger.info(f"Appended {len(new_df)} new players to {players_parquet}")
    else:
        logger.info("No new players to add.")
        combined = players_df
    
    # Count remaining players without data
    all_player_ids = set(rankings_df['atp_id'].dropna().unique())
    known_ids_updated = set(combined['atp_id'].dropna().unique()) if not combined.empty else known_ids
    remaining = len(all_player_ids - known_ids_updated)
    logger.info(f"Remaining players without data: {remaining}")
    
    return remaining