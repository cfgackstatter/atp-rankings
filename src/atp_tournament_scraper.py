import os
import time
import requests
import polars as pl
import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
import logging
from typing import Tuple, Optional, List

logger = logging.getLogger(__name__)


def parse_tournament_date(date_str: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse a tournament date range string into start and end date.

    Handles cases like:
        "31 December, 2023 - 7 January, 2024"
        "Jul 13, 2024 - Aug 15, 2025"
        "1 January, 2024"
        
    Returns:
        (start_date, end_date) as ISO strings (YYYY-MM-DD), or (start_date, None) if only one date.
        If parsing fails, returns (None, None).
    """
    if not date_str or not isinstance(date_str, str):
        return None, None
    
    # Try to split on common range separators
    for sep in [" - ", "–", "-"]:
        if sep in date_str:
            parts = [p.strip() for p in date_str.split(sep, 1)]
            try:
                # Parse end date
                end = dateparser.parse(parts[1], dayfirst=False, fuzzy=True)
                # Heuristic for missing year in start date
                if len(parts[0].split(" ")) == 1:
                    # Only day (e.g. "1" or "Jan"), append year and month from end date
                    start = dateparser.parse(parts[0] + " " + " ".join(parts[1].split(" ")[-2:]), dayfirst=False, fuzzy=True)
                elif len(parts[0].split(" ")) == 2:
                    # Day and month, append year from end date
                    start = dateparser.parse(parts[0] + " " + parts[1].split(" ")[-1], dayfirst=False, fuzzy=True)
                else:
                    start = dateparser.parse(parts[0], dayfirst=False, fuzzy=True)
                return start.date().isoformat(), end.date().isoformat()
            except Exception:
                pass
    # Try to parse as a single date
    try:
        dt = dateparser.parse(date_str, dayfirst=False, fuzzy=True)
        return dt.date().isoformat(), None
    except Exception:
        return None, None
    

def scrape_atp_events(year: int, tournament_type: str) -> pd.DataFrame:
    """
    Scrape all tournament events for a given year and tournament type from the ATP archive.
    Only includes tournaments that are finished (have a singles winner).
    Handles all missing fields robustly.

    Args:
        year (int): Year to scrape (e.g. 2024)
        tournament_type (str): Tournament type code ('gs', 'atp', 'ch', 'fu')

    Returns:
        pd.DataFrame: DataFrame with one row per tournament, columns for all extracted fields.
    """
    BASE_URL = "https://www.atptour.com/en/scores/results-archive"
    url = f"{BASE_URL}?year={year}&tournamentType={tournament_type}"
    headers = {"User-Agent": "Mozilla/5.0"}
    logger.info(f"Scraping {url}")

    r = requests.get(url, headers=headers)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')

    extracted_data = []
    # Loop over all <ul class="events"> blocks (each may contain multiple tournaments)
    for ul_events in soup.find_all('ul', class_='events'):
        # Each tournament is a <li> inside the <ul>
        for li in ul_events.find_all('li', recursive=False):
            event = {
                "year": year,
                "tournament_type": tournament_type
            }
            # Tournament info
            tournament_info = li.find('div', class_='tournament-info')
            if tournament_info:
                # Badge image (ATP 250, 500, etc.)
                badge_img = tournament_info.find('img', class_='events_banner')
                event['badge_alt'] = badge_img.get('alt', '') if badge_img else ''
                event['badge_src'] = badge_img.get('src', '') if badge_img else ''
                event['badge_title'] = badge_img.get('title', '') if badge_img else ''

                # Tournament profile link and details
                profile_link = tournament_info.find('a', class_='tournament__profile')
                event['tournament_url'] = profile_link.get('href', '') if profile_link else ''
                details_holder = profile_link.find('div', class_='details-holder') if profile_link else None
                if details_holder:
                    # Top details (name, flag)
                    top_div = details_holder.find('div', class_='top')
                    if top_div:
                        name_span = top_div.find('span', class_='name')
                        event['tournament_name'] = name_span.get_text(strip=True) if name_span else ''
                        flag_span = top_div.find('span', class_='flag')
                        if flag_span:
                            use_tag = flag_span.find('use')
                            if use_tag:
                                href = use_tag.get('href', '')
                                if '#flag-' in href:
                                    event['country_code'] = href.split('#flag-')[-1]
                                else:
                                    event['country_code'] = ''
                            else:
                                event['country_code'] = ''
                        else:
                            event['country_code'] = ''
                    # Bottom details (venue, date)
                    bottom_div = details_holder.find('div', class_='bottom')
                    if bottom_div:
                        venue_span = bottom_div.find('span', class_='venue')
                        # Remove all vertical bars/dashes and extra whitespace
                        event['venue'] = venue_span.get_text(strip=True).replace("|", "").replace("–", "").replace("-", "").strip() if venue_span else ''
                        date_span = bottom_div.find('span', class_='Date')
                        event['date_range'] = date_span.get_text(strip=True) if date_span else ''
                        # Parse start and end date
                        start_date, end_date = parse_tournament_date(event['date_range'])
                        event['start_date'] = start_date
                        event['end_date'] = end_date

            # Winners info
            cta_holder = li.find('div', class_='cta-holder')
            singles_winner_found = False
            if cta_holder:
                winners = cta_holder.find_all('dl', class_='winner')
                for winner_dl in winners:
                    dt = winner_dl.find('dt')
                    dd_tags = winner_dl.find_all('dd')
                    if dt and dd_tags:
                        winner_type = dt.get_text(strip=True).lower().replace(' ', '_')  # e.g. singles_winner
                        winner_names = []
                        winner_urls = []
                        for dd in dd_tags:
                            a_tag = dd.find('a')
                            if a_tag:
                                winner_names.append(a_tag.get_text(strip=True))
                                winner_urls.append(a_tag.get('href', ''))
                        event[winner_type + '_names'] = winner_names
                        event[winner_type + '_urls'] = winner_urls
                        if winner_type == "singles_winner" and winner_names:
                            singles_winner_found = True

            # Only include tournaments with a singles winner (finished)
            if not singles_winner_found:
                continue

            # Results URL
            non_live_cta = li.find('div', class_='non-live-cta')
            if non_live_cta:
                results_link = non_live_cta.find('a', class_='results')
                event['results_url'] = results_link.get('href', '') if results_link else ''

            extracted_data.append(event)

    # Build DataFrame and ensure all date columns are date type
    df = pd.DataFrame(extracted_data)
    if not df.empty:
        for date_col in ['start_date', 'end_date']:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.date
    return df


def update_tournaments(years: List[int], tournament_types: List[str]) -> None:
    """
    Update tournament data for specified years and tournament types.
    Always overwrites the most recent year with tournament data, even if not current year.
    
    Args:
        years: List of years to scrape
        tournament_types: List of tournament types ('gs', 'atp', 'ch', 'fu')
    """
    # Find the most recent year with tournament files
    most_recent_year = find_most_recent_year_with_files("data/raw/tournaments")

    for year in years:        
        for tournament_type in tournament_types:
            output_path = f"data/raw/tournaments/{year}/tournaments_{tournament_type}_{year}_raw.csv"
            
            if os.path.exists(output_path) and year != most_recent_year:
                logger.info(f"Skipping {year} {tournament_type} - file already exists")
                continue
                
            # Always process most recent year or years without existing files
            logger.info(f"Scraping {year} {tournament_type} tournaments")
            try:
                df = scrape_atp_events(year, tournament_type)
                
                if df is not None and not df.empty:
                    # Create directory if it doesn't exist
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    df.to_csv(output_path, index=False)
                    logger.info(f"Saved {len(df)} tournaments to {output_path}")
                else:
                    logger.warning(f"No data found for {year} {tournament_type}")
            except Exception as e:
                logger.error(f"Error scraping {year} {tournament_type}: {e}")            
            
            # Add a small delay to avoid overwhelming the server
            time.sleep(1)


def find_most_recent_year_with_files(base_path: str) -> Optional[int]:
    """
    Find the most recent year directory under base_path that contains tournament files.
    
    Args:
        base_path: Path to the directory containing year subdirectories
        
    Returns:
        The most recent year (as int) with tournament files, or None if no files found
    """
    if not os.path.exists(base_path):
        return None
        
    # Get all year directories (assuming they're named as digits)
    year_dirs = [d for d in os.listdir(base_path) 
                if d.isdigit() and os.path.isdir(os.path.join(base_path, d))]
    
    if not year_dirs:
        return None
        
    # Sort years in descending order
    year_dirs = sorted(year_dirs, key=int, reverse=True)
    
    # Find the most recent year with tournament files
    for year_dir in year_dirs:
        year_path = os.path.join(base_path, year_dir)
        files = [f for f in os.listdir(year_path) 
                if f.startswith('tournaments_') and f.endswith('_raw.csv')]
        if files:
            return int(year_dir)
            
    return None