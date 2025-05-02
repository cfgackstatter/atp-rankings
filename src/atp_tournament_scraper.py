import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
import logging

logger = logging.getLogger(__name__)

def parse_tournament_date(date_str: str) -> tuple:
    """
    Parse a date range string like "31 December, 2023 - 7 January, 2024"
    into (start_date, end_date) as date objects. Returns (start_date, end_date) or (start_date, None).
    """
    if not date_str or not isinstance(date_str, str):
        return None, None
    # Split on "-"; handle both en-dash and hyphen
    for sep in [" - ", "–", "-"]:
        if sep in date_str:
            parts = [p.strip() for p in date_str.split(sep, 1)]
            try:
                end = dateparser.parse(parts[1], dayfirst=False, fuzzy=True)
                if len(parts[0].split(" ")) == 1:
                    start = dateparser.parse(parts[0] + " ".join(parts[1].split(" ")[-2:]), dayfirst=False, fuzzy=True)
                elif len(parts[0].split(" ")) == 2:
                    start = dateparser.parse(parts[0] + parts[1].split(" ")[-1], dayfirst=False, fuzzy=True)
                else:
                    start = dateparser.parse(parts[0], dayfirst=False, fuzzy=True)
                return start.date().isoformat(), end.date().isoformat()
            except Exception:
                pass
    # Single date
    try:
        dt = dateparser.parse(date_str, dayfirst=False, fuzzy=True)
        return dt.date().isoformat(), None
    except Exception:
        return None, None
    

def scrape_atp_events(year, tournament_type, sleep_sec=1):
    """
    Scrape all tournament events for a given year and tournament type from the ATP archive.
    Returns a DataFrame with all available info for each event.
    Only includes tournaments that are finished (have a winner).
    """
    BASE_URL = "https://www.atptour.com/en/scores/results-archive"
    url = f"{BASE_URL}?year={year}&tournamentType={tournament_type}"
    headers = {"User-Agent": "Mozilla/5.0"}
    logger.info(f"Scraping {url}")
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')

    extracted_data = []
    for ul_events in soup.find_all('ul', class_='events'):
        for li in ul_events.find_all('li', recursive=False):
            event = {
                "year": year,
                "tournament_type": tournament_type
            }
            # Tournament info
            tournament_info = li.find('div', class_='tournament-info')
            if tournament_info:
                badge_img = tournament_info.find('img', class_='events_banner')
                event['badge_alt'] = badge_img.get('alt', '') if badge_img else ''
                event['badge_src'] = badge_img.get('src', '') if badge_img else ''
                event['badge_title'] = badge_img.get('title', '') if badge_img else ''
                profile_link = tournament_info.find('a', class_='tournament__profile')
                event['tournament_url'] = profile_link.get('href', '') if profile_link else ''
                details_holder = profile_link.find('div', class_='details-holder') if profile_link else None
                if details_holder:
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
                    bottom_div = details_holder.find('div', class_='bottom')
                    if bottom_div:
                        venue_span = bottom_div.find('span', class_='venue')
                        event['venue'] = venue_span.get_text(strip=True).replace("|", "").strip() if venue_span else ''
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
    # Sleep to avoid being blocked
    time.sleep(sleep_sec)
    # Ensure all date columns are pandas date types
    df = pd.DataFrame(extracted_data)
    if not df.empty:
        for date_col in ['start_date', 'end_date']:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.date
    return df


def update_tournament_parquet(years, tournament_types, parquet_path="data/cache/atp_tournaments.parquet"):
    """
    Scrape and update the ATP tournament parquet file for all given years and tournament types.
    Only finished tournaments (with a winner) are included.
    Handles differing columns by outer-concatenation (missing columns filled with NaN).
    """
    import os
    import glob

    logger.info("Updating ATP tournament parquet file...")
    all_dfs = []
    for year in years:
        for ttype in tournament_types:
            logger.info(f"Scraping tournaments for {year}, {ttype}...")
            df = scrape_atp_events(year, ttype)
            if not df.empty:
                all_dfs.append(df)
    if not all_dfs:
        logger.warning("No tournaments found for the specified years/types.")
        return
    # Outer-concat to handle differing columns
    full_df = pd.concat(all_dfs, axis=0, ignore_index=True, sort=True)
    # Remove duplicates (by year, tournament_name, start_date)
    full_df = full_df.drop_duplicates(subset=['year', 'tournament_name', 'start_date'])
    # Save as parquet
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    full_df.to_parquet(parquet_path, index=False)
    logger.info(f"Saved {len(full_df)} tournaments to {parquet_path}")
    return full_df