import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

def plot_player_rankings(rankings_df, players_df, player_ids, player_names=None, save_path=None, by_age=False):
    """
    Plot ranking histories for one or more players with improved aesthetics.
    
    Args:
        rankings_df: DataFrame containing ranking data
        players_df: DataFrame containing player information
        player_ids: List of player IDs to plot (can be a single ID or multiple)
        player_names: Optional list of player names (if None, names will be retrieved from players_df)
        save_path: Optional path to save the plot
    """
    # Convert single player_id to list for consistent handling
    if not isinstance(player_ids, list):
        player_ids = [player_ids]

    # If player_names not provided, get them from the players_df
    if player_names is None:
        player_names = []
        first_name_col = 'first_name' if 'first_name' in players_df.columns else 'name_first'
        last_name_col = 'last_name' if 'last_name' in players_df.columns else 'name_last'
        
        for player_id in player_ids:
            player_info = players_df[players_df['player_id'] == player_id]
            if len(player_info) > 0:
                first_name = player_info.iloc[0][first_name_col]
                last_name = player_info.iloc[0][last_name_col]
                player_names.append(f"{first_name} {last_name}")
            else:
                player_names.append(f"Player {player_id}")

    # Create figure with appropriate size and DPI
    plt.figure(figsize=(14, 8), dpi=100)

    # Set style for a cleaner look
    plt.style.use('seaborn-v0_8-whitegrid')

    # Use a better color palette
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']

    for i, (player_id, player_name) in enumerate(zip(player_ids, player_names)):
        # Get player data
        player_data = rankings_df[rankings_df['player_id'] == player_id]
        player_data = player_data.sort_values('ranking_date')

        if len(player_data) == 0:
            print(f"No ranking data found for {player_name}")
            continue

        if by_age:
            # Get player's birth date
            player_info = players_df[players_df['player_id'] == player_id]
            if len(player_info) > 0:                
                if 'birth_date' in player_info.columns:
                    birth_date = pd.to_datetime(player_info.iloc[0]['birth_date'])
                    
                    # Calculate age at each ranking date (in years)
                    x_values = (player_data['ranking_date'] - birth_date).dt.days / 365.25
                    x_label = 'Age (years)'
                else:
                    print(f"Birth date not available for {player_name}, using dates instead")
                    x_values = player_data['ranking_date']
                    x_label = 'Date'
            else:
                print(f"Player info not found for {player_name}, using dates instead")
                x_values = player_data['ranking_date']
                x_label = 'Date'
        else:
            # Use dates for x-axis
            x_values = player_data['ranking_date']
            x_label = 'Date'
        
        # Plot with just the line (no markers), increased line width
        plt.plot(x_values, player_data['rank'], 
                 label=player_name, color=colors[i % len(colors)],
                 linestyle='-', linewidth=2.5)
    
    # Invert y-axis since lower ranking numbers are better
    plt.gca().invert_yaxis()
    
    # Set labels with better fonts
    plt.xlabel(x_label, fontsize=12, fontweight='bold')
    plt.ylabel('ATP Ranking', fontsize=12, fontweight='bold')

    # Set appropriate title based on number of players
    plt.title(f'ATP Ranking History', fontsize=14, fontweight='bold', pad=20)
    
    # Improve grid appearance
    plt.grid(True, alpha=0.3, linestyle='--')
    
    # Format y-axis to show only integers
    plt.gca().yaxis.set_major_locator(plt.MaxNLocator(integer=True))

    # Add legend if there are multiple players
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15),
                  frameon=True, ncol=3, fontsize=11)
    
    # Format x-axis based on type
    if by_age:
        # For age, use regular numeric ticks
        plt.gca().xaxis.set_major_locator(plt.MaxNLocator(integer=True))
    else:
        # For dates, use date formatting
        plt.gcf().autofmt_xdate()

    # Add subtle spines
    for spine in plt.gca().spines.values():
        spine.set_visible(True)
        spine.set_color('#dddddd')

    # Add a light background color
    plt.gca().set_facecolor('#f8f9fa')
    
    # Tighten layout
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
    
    plt.show()