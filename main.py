import pandas as pd
import argparse
from src.data_loader import load_players, load_rankings, find_player_by_name
from src.visualizer import plot_player_rankings

def main():
    parser = argparse.ArgumentParser(description='ATP RankTracker')

    parser.add_argument('--players', type=str, nargs='+', required=True,
                        help='One or more player names to visualize (use quotes for full names, e.g., "Roger Federer")')
    
    parser.add_argument('--save', type=str, help='Save the plot to specified path')

    parser.add_argument('--force-download', action='store_true',
                        help='Force download of data files even if cached')
    
    parser.add_argument('--by-age', action='store_true',
                        help='Plot rankings against player age instead of date')
    
    args = parser.parse_args()
    
    # Load player data
    print("Loading player data...")
    players_df = load_players()
    
    # Load rankings data
    print("Loading rankings data...")
    rankings_df = load_rankings()

    # Process player names
    player_ids = []
    player_names = []
    
    for name in args.players:
        player_matches = find_player_by_name(players_df, name)
        
        if len(player_matches) == 0:
            print(f"No players found with name: {name}")
            continue
        elif len(player_matches) > 1:
            print(f"Multiple players found with name: {name}")
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
                
                print(f"  {display_name} (ID: {player['player_id']})")
            
            # Use the first match
            player = player_matches.iloc[0]
            first_name = "" if pd.isna(player['first_name']) else player['first_name']
            last_name = "" if pd.isna(player['last_name']) else player['last_name']
            print(f"Using the first match: {first_name} {last_name}")
        else:
            player = player_matches.iloc[0]
        
        first_name = "" if pd.isna(player['first_name']) else player['first_name']
        last_name = "" if pd.isna(player['last_name']) else player['last_name']
        player_name = f"{first_name} {last_name}".strip()
        player_ids.append(player['player_id'])
        player_names.append(player_name)
    
    if not player_ids:
        print("No valid players found. Please check the player names and try again.")
        return
                
    # Use the unified plotting function for any number of players
    print(f"Plotting ranking history for {len(player_ids)} players...")
    plot_player_rankings(rankings_df, players_df, player_ids, player_names, args.save, by_age=args.by_age)
            
if __name__ == "__main__":
    main()