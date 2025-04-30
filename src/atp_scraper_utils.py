import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)

def scrape_atp_rankings_raw(week_date, raw_dir="data/atp_rankings_2025_raw"):
    """
    Scrape ATP singles rankings for a given week and save as a raw CSV.
    Returns the DataFrame.
    """
    os.makedirs(raw_dir, exist_ok=True)
    url = f"https://www.atptour.com/en/rankings/singles?rankRange=0-5000&dateWeek={week_date}"
    headers = {"User-Agent": "Mozilla/5.0"}
    logger.info(f"Requesting {url}")
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find('table', class_='mega-table desktop-table non-live')
    header_row = table.find("tr")
    headers_list = [th.get_text(strip=True) for th in header_row.find_all("th")][1:]
    data = []
    for tr in table.tbody.find_all('tr'):
        tds = tr.find_all('td')
        if not tds or len(tds) == 1:
            continue
        rank = tds[0].get_text(strip=True)
        player_td = tds[1]
        player_name = ""
        rank_change = ""
        rank_up = player_td.find("span", class_="rank-up")
        rank_down = player_td.find("span", class_="rank-down")
        if rank_up:
            rank_change = rank_up.get_text(strip=True)
        elif rank_down:
            rank_change = "-" + rank_down.get_text(strip=True)
        else:
            rank_change = "0"
        name_span = player_td.find("li", class_="name")
        if name_span:
            player_name = name_span.get_text(strip=True)
        else:
            player_name = player_td.get_text(strip=True)
        rest = [td.get_text(strip=True) for td in tds[2:]]
        row = [rank, player_name, rank_change] + rest
        data.append(row)
    headers_full = headers_list[:2] + ["RankChange"] + headers_list[2:]
    df = pd.DataFrame(data, columns=headers_full)
    df['ranking_date'] = pd.to_datetime(week_date)
    out_path = f"{raw_dir}/atp_rankings_2025_{week_date.replace('-', '')}_raw.csv"
    df.to_csv(out_path, index=False)
    logger.info(f"Saved raw rankings for {week_date} to {out_path}")
    return df