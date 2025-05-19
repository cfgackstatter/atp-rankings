import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash.exceptions import PreventUpdate
from functools import lru_cache

# Import data loading functions
from src.data_loader import load_players, load_rankings, load_tournaments
from src.data_loader import interpolate_rank_at_date

# Initialize the Dash app
app = dash.Dash(__name__,
                title='TennisRank.net',
                assets_folder='static',
                assets_url_path='static',
                suppress_callback_exceptions=True,
                use_pages=False,
                external_stylesheets=[dbc.themes.BOOTSTRAP],
                meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}])
application = app.server

# Load data at startup
print("Loading player data...")
try:
   players_df = load_players()
except FileNotFoundError:
   print("No player data found.")
   players_df = pd.DataFrame(columns=['atp_id', 'full_name', 'country_code', 'dob'])

print("Loading rankings data...")
rankings_df = load_rankings()

print("Loading tournaments data...")
tournaments_df = load_tournaments()

# Create a function to generate player options once
def generate_player_options(players_df, rankings_df):
    options = []

    # Get all known atp_ids from players_df
    known_atp_ids = set(players_df['atp_id'].unique()) if not players_df.empty else set()

    # Process all players from rankings_df
    if not rankings_df.empty:
        # Get unique players from rankings
        unique_players = rankings_df[['atp_id', 'atp_name']].drop_duplicates()

        for _, player in unique_players.iterrows():
            atp_id = player['atp_id']
            atp_name = player['atp_name']
            search_terms = []
            if atp_name is not None and not pd.isna(atp_name):
                search_terms = [atp_name.replace('-', ' ').lower()]
            else:
                search_terms = []

            # If player exists in players_df, use that data
            if atp_id in known_atp_ids:
                player_info = players_df[players_df['atp_id'] == atp_id].iloc[0]

                # Use full_name if available, otherwise use atp_name
                full_name = player_info.get('full_name', '')
                if pd.isna(full_name) or not full_name:
                    # Fallback to atp_name
                    display_name = atp_name.replace('-', ' ').title()
                else:
                    display_name = full_name
                    search_terms.append(full_name.lower())

                # Add country code if available
                country_code = player_info.get('country_code', '')
                if country_code and not pd.isna(country_code):
                    display_name += f" ({country_code})"
                    search_terms.append(country_code.lower())

                # Add birth year if available
                dob = player_info.get('dob')
                if dob and not pd.isna(dob):
                    try:
                        birth_year = pd.to_datetime(dob).year
                        display_name += f" - *{birth_year}"
                    except:
                        pass

            # If player doesn't exist in players_df, use atp_name as fallback
            else:
                if not atp_name or pd.isna(atp_name):
                    continue
                
                # Use atp_name as display name (with dashes replaced by spaces)
                display_name = atp_name.replace('-', ' ').title()
                search_terms.append(atp_name.replace('-', ' ').lower())

            options.append({
                'label': display_name,
                'value': atp_id,
                'search': ' '.join(search_terms)  # Include all search terms
            })
    
    # Sort options by display name
    options.sort(key=lambda x: x['label'])
    return options

# Generate options once at startup
player_options = generate_player_options(players_df, rankings_df)

# After loading data, create a search index
player_search_index = {}

# First try using players_df
if not players_df.empty:
    for _, player in players_df.iterrows():
        atp_id = player['atp_id']
        full_name = "" if pd.isna(player.get('full_name', '')) else str(player.get('full_name', '')).lower()
        
        # Skip players with empty names
        if not full_name:
            continue
            
        # Add to search index with all possible search terms
        search_terms = []
        
        # Add full name
        search_terms.append(full_name)
        
        # Add each word in the name separately for partial matching
        name_parts = full_name.split()
        for part in name_parts:
            if len(part) > 1:  # Skip very short parts
                search_terms.append(part)
        
        # Add country code if available
        country_code = player.get('country_code', '')
        if country_code and not pd.isna(country_code):
            country_code = country_code.lower()
            search_terms.append(country_code)
            
        # Add to search index with all possible search terms
        for term in search_terms:
            if term:
                if term not in player_search_index:
                    player_search_index[term] = []
                player_search_index[term].append(atp_id)

# Always process rankings_df (remove the else)
unique_players = rankings_df[['atp_id', 'atp_name']].drop_duplicates()

for _, player in unique_players.iterrows():
    atp_id = player['atp_id']

    # Skip players we already processed from players_df
    if atp_id in player_search_index.get('atp_id', []):
        continue

    atp_name = player['atp_name']
    if not atp_name or pd.isna(atp_name):
        continue
        
    # Format atp_name for search
    search_name = atp_name.replace('-', ' ').lower()
    
    # Add to search index
    search_terms = []
    search_terms.append(search_name)
    
    # Add each word in the name separately
    name_parts = search_name.split()
    for part in name_parts:
        if len(part) > 1:  # Skip very short parts
            search_terms.append(part)
            
    # Add to search index
    for term in search_terms:
        if term:
            if term not in player_search_index:
                player_search_index[term] = []
            player_search_index[term].append(atp_id)


# Add a cached search function
@lru_cache(maxsize=1024)
def search_players(query):
    query = query.lower().strip()
    matching_ids = set()

    # Split the query into parts to handle multi-word searches better
    query_parts = query.split()

    # Try exact matches but don't return immediately
    for term, ids in player_search_index.items():
        if query == term:
            matching_ids.update(ids)

    # Try starts-with matches and add to results
    for term, ids in player_search_index.items():
        if term.startswith(query):
            matching_ids.update(ids)
    
    # Try matching individual parts
    if len(query_parts) > 1:
        # For multi-word queries, try to match each part
        potential_matches = {}
        for part in query_parts:
            if len(part) < 3:  # Skip very short parts
                continue
            for term, ids in player_search_index.items():
                if part in term.split():
                    for atp_id in ids:
                        potential_matches[atp_id] = potential_matches.get(atp_id, 0) + 1

        # Find players that match multiple parts of the query
        for atp_id, match_count in potential_matches.items():
            if match_count >= min(2, len(query_parts)):
                matching_ids.add(atp_id)
        
    # Try contains matching
    for term, ids in player_search_index.items():
        if query in term:
            matching_ids.update(ids)
    
    # Try partial matching for longer queries
    if len(query) >= 3:
        for term, ids in player_search_index.items():
            if any(query[:int(len(query)*0.7)] in word for word in term.split()):
                matching_ids.update(ids)
    
    return matching_ids

# App layout
app.layout = html.Div([
    # Compact header with logo
    html.Div([
        # Left side: Logo and title in a row
        html.Div([
            # Logo placeholder
            html.Img(src='/static/logo.svg', height=40, style={'marginRight': '15px'}),
            # Title next to logo, not above content
            html.H1("TennisRank.net", style={'margin': '0', 'fontSize': '24px'})
        ], style={'display': 'flex', 'alignItems': 'center'}),

        # Right side: X-Axis toggle and Tournament Types
        html.Div([
            # X-Axis toggle in header
            html.Div([
                html.Label("X-Axis:", className="control-label-inline"),
                html.Br(),
                dcc.RadioItems(
                    id='x-axis-toggle',
                    options=[
                        {'label': 'Date', 'value': 'date'},
                        {'label': 'Age', 'value': 'age'}
                    ],
                    value='date',
                    labelStyle={'display': 'inline-block', 'marginRight': '15px', 'cursor': 'pointer'},
                    className="radio-group-inline"
                ),
            ], style={'marginRight': '20px'}),
            
            # Tournament Types filter
            html.Div([
                html.Label("Tournaments:", className="control-label-inline"),
                html.Br(),
                dcc.Checklist(
                    id='tournament-types',
                    options=[
                        {'label': 'Grand Slam', 'value': 'gs'},
                        {'label': 'ATP Tour', 'value': 'atp'},
                        {'label': 'Challenger', 'value': 'ch'},
                        {'label': 'ITF Tour', 'value': 'fu'},
                    ],
                    value=['gs', 'atp'],  # Default selection
                    inline=True,
                    className="checklist-inline"
                ),
            ]),
        ], style={'marginLeft': 'auto', 'display': 'flex', 'alignItems': 'center'})
    ], className="header-container-compact", style={
        'display': 'flex', 
        'justifyContent': 'space-between',
        'alignItems': 'center',
        'padding': '10px 20px',
        'borderBottom': '1px solid #eaeaea',
        'backgroundColor': 'white',
        'position': 'sticky',
        'top': 0,
        'zIndex': 1000
    }),
    
    # Main content
    html.Div([
        # Player selection
        html.Div([
            html.Label("Select Players:", className="control-label"),
            dcc.Dropdown(
                id='player-dropdown',
                options=player_options,
                multi=True,
                placeholder="Type to search for players...",
                className="player-dropdown"
            ),
        ], className="player-selection"),
        
        # Chart container
        html.Div([
            dcc.Loading(
                id="loading",
                type="circle",
                children=[dcc.Graph(id='rankings-graph', className="main-chart")]
            ),
        ], className="chart-container"),

        # Footer   
        # Footer
        html.Footer([
            html.P([
                "TennisRank.net | Data from ATP Tour"
            ], style={'fontSize': '0.8rem', 'margin': '5px 0'}),
        ], className="footer")
    ], className="main-content"),
], className="container")


@app.callback(
    Output('rankings-graph', 'figure'),
    [Input('player-dropdown', 'value'),
     Input('x-axis-toggle', 'value'),
     Input('tournament-types', 'value')]
)
def update_graph(selected_atp_ids, x_axis_type, tournament_types):
    # Handle no selection
    if not selected_atp_ids:
        # Create empty figure with message
        fig = go.Figure()
        fig.add_annotation(
            text="Select players from the dropdown above<br>to visualize their ranking history",
            xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="#666666")
        )
        # Hide axis values and grid lines
        fig.update_xaxes(showticklabels=False, showgrid=False, zeroline=False)
        fig.update_yaxes(showticklabels=False, showgrid=False, zeroline=False)
        return fig
    
    # Create figure
    fig = go.Figure()
    
    # Colors for different players
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

    # Pre-fetch all player info at once
    player_info_dict = {}
    for atp_id in selected_atp_ids:
        player_info = players_df[players_df['atp_id'] == atp_id]
        if not player_info.empty:
            player_info_dict[atp_id] = player_info.iloc[0]
    
    messages = []
    all_x_values = []  # To store all x values for padding calculation

    for i, atp_id in enumerate(selected_atp_ids):
        # Filter rankings for this player
        player_data = rankings_df[rankings_df['atp_id'] == atp_id]
        
        if player_data.empty:
            # Get player name from pre-fetched info
            player_info = player_info_dict.get(atp_id)
            if player_info is not None:
                player_name = player_info.get('full_name', '') or f"{player_info.get('first_name', '')} {player_info.get('last_name', '')}".strip()
            else:
                player_name = f"Player {atp_id}"
            messages.append(f"No ranking data found for {player_name}")
            continue

        # Sort by date
        player_data = player_data.sort_values('ranking_date')

        # Get player name
        player_info = player_info_dict.get(atp_id)
        if player_info is not None:
            player_name = player_info.get('full_name', '')
        else:
            # Fallback to atp_name from rankings
            player_row = rankings_df[rankings_df['atp_id'] == atp_id]
            if not player_row.empty:
                atp_name = player_row['atp_name'].iloc[0]
                player_name = atp_name.replace('-', ' ').title()
            else:
                player_name = f"Player {atp_id}"
            
        if x_axis_type == 'age':
            # Check if we have player info and birth date
            if player_info is not None and 'dob' in player_info and pd.notna(player_info['dob']):
                birth_date = pd.to_datetime(player_info['dob'])
                
                # Vectorized age calculation - more efficient
                player_data = player_data.copy()  # Avoid SettingWithCopyWarning
                player_data['age'] = (player_data['ranking_date'] - birth_date).dt.days / 365.25
                
                x_values = player_data['age']
                all_x_values.extend(x_values)
                
                # Plot with age on x-axis
                fig.add_trace(go.Scatter(
                    x=x_values,
                    y=player_data['rank'],
                    mode='lines',
                    name=player_name,
                    line=dict(color=colors[i % len(colors)], width=2.5, shape='linear'),
                    hovertemplate='<b>%{fullData.name}</b><br>Rank: %{y}<extra></extra>',
                    connectgaps=False
                ))
            else:
                messages.append(f"Birth date not available for {player_name}, skipping age-based plot")
                continue
        else:
            x_values = player_data['ranking_date']
            all_x_values.extend(x_values)
            
            # Plot with date on x-axis
            fig.add_trace(go.Scatter(
                x=x_values,
                y=player_data['rank'],
                mode='lines',
                name=player_name,
                line=dict(color=colors[i % len(colors)], width=2.5, shape='linear'),
                hovertemplate='<b>%{fullData.name}</b><br>Rank: %{y}<extra></extra>',
                connectgaps=False
            ))

        if not player_data['rank'].isnull().all():
            best_rank = player_data['rank'].min()
            # Get the first occurrence of the best rank
            best_row = player_data[player_data['rank'] == best_rank].iloc[0]
            if x_axis_type == 'age':
                best_x = best_row['age']
                best_x_label = f"{best_x:.2f}"
            else:
                best_x = best_row['ranking_date']
                best_x_label = best_x.strftime('%b %d, %Y')
            best_y = best_row['rank']

            # Add a marker for the first time the best rank was reached
            fig.add_trace(go.Scatter(
                x=[best_x],
                y=[best_y],
                mode='markers+text',
                marker=dict(
                    size=10,
                    symbol='star',
                    color=colors[i % len(colors)],
                    line=dict(width=2, color='black')
                ),
                text=[f"#{int(best_y)}"],
                textposition="top center",
                textfont=dict(
                    color=colors[i % len(colors)],  # <-- Set your desired color here
                    size=10,                        # (optional) set text size
                    family="Arial"                  # (optional) set font family
                ),
                name=f"{player_name} Best",
                showlegend=False,
                hovertemplate=f"First reached #{int(best_y)}<br>{'Age' if x_axis_type == 'age' else 'Date'}: {best_x_label}<extra></extra>",
                cliponaxis=False
            ))

        # Tournament win markers
        if not tournaments_df.empty:
            # Find tournaments this player won (singles)
            for _, win in tournaments_df.iterrows():
                if win.get('tournament_type', '') in tournament_types:
                    # Check if this player won by matching atp_id in URLs
                    winner_urls = win.get('singles_winner_urls', [])
                    if isinstance(winner_urls, list) and any(atp_id in url for url in winner_urls):
                        # Use end date for x-axis, or skip if missing
                        end_date = win.get('end_date')
                        if pd.isnull(end_date):
                            continue

                        # Tournament info for hover
                        tournament_name = win.get('tournament_name', '')
                        venue = win.get('venue', '')

                        # Map type code to label and marker size
                        type_map = {'gs': 'Grand Slam', 'atp': 'ATP Tour', 'ch': 'Challenger Tour', 'fu': 'ITF Tour'}
                        marker_map = {'gs': 12, 'atp': 10, 'ch': 8, 'fu': 6}
                        ttype = type_map.get(win.get('tournament_type', ''), win.get('tournament_type', ''))
                        marker_size = marker_map.get(win.get('tournament_type', ''), 10)

                        # X-axis value depends on plot type
                        if x_axis_type == 'age':
                            # For age plots, we need birth date
                            birth_date = None
                            if player_info is not None and 'dob' in player_info and pd.notna(player_info['dob']):
                                birth_date = pd.to_datetime(player_info['dob'])

                            if birth_date is None:
                                # Skip this tournament for age plots if no birth date
                                continue

                            win_age = (pd.to_datetime(end_date) - birth_date).days / 365.25
                            win_x = win_age
                        else:
                            # For date plots, we don't need birth date
                            win_x = pd.to_datetime(end_date)

                        # Y value: get player's rank at that date (or nearest previous date)
                        tournament_end_date = pd.to_datetime(end_date)
                        if tournament_end_date <= player_data['ranking_date'].max() and tournament_end_date >= player_data['ranking_date'].min():
                            win_y = interpolate_rank_at_date(player_data, tournament_end_date)
                        else:
                            # Fallback to nearest rank if outside ranking date range
                            rank_row = player_data[player_data['ranking_date'] <= tournament_end_date].tail(1)
                            win_y = rank_row['rank'].values[0] if not rank_row.empty else None

                        if win_y is None or pd.isnull(win_y):
                            continue

                        # Add marker
                        fig.add_trace(go.Scatter(
                            x=[win_x],
                            y=[win_y],
                            mode='markers',
                            marker=dict(
                                size=marker_size,
                                symbol='diamond',
                                color=colors[i % len(colors)],
                                line=dict(width=2, color='black')
                            ),
                            name=f"{player_name} Tournament Win",
                            showlegend=False,
                            hovertemplate=(
                                f"{tournament_name}<br>" +
                                f"Venue: {venue}<br>"
                                f"Type: {ttype}<br>"
                                f"{'Age' if x_axis_type == 'age' else 'Date'}: %{{x}}<br>"
                                "<extra></extra>"
                            ),
                            cliponaxis=False
                        ))
    
    # Set up layout
    if x_axis_type == 'age':
        x_axis_title = 'Age (years)'

        # Add padding to age axis if we have data
        if all_x_values:
            min_x = min(all_x_values)
            max_x = max(all_x_values)
            padding = (max_x - min_x) * 0.01  # 1% padding
            fig.update_xaxes(
                range=[min_x - padding, max_x + padding]  # Add padding
            )

        fig.update_xaxes(
            title=dict(text=x_axis_title, font=dict(size=14, color='#444')),
            tickformat='.1f',
            gridcolor='rgba(0,0,0,0.1)',
            zeroline=True,
            zerolinecolor='rgba(0,0,0,0.2)'
        )
    else:
        x_axis_title = 'Date'

        # Add padding to date axis if we have data
        if all_x_values:
            min_date = min(all_x_values)
            max_date = max(all_x_values)
            date_range = max_date - min_date
            padding = pd.Timedelta(days=int(date_range.days * 0.01))  # 1% padding
            fig.update_xaxes(
                range=[min_date - padding, max_date + padding]  # Add padding
            )

        fig.update_xaxes(
            title=dict(text=x_axis_title, font=dict(size=14, color='#444')),
            hoverformat='%b %d, %Y',
            gridcolor='rgba(0,0,0,0.1)',
            zeroline=True,
            zerolinecolor='rgba(0,0,0,0.2)',
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(count=3, label="3y", step="year", stepmode="backward"),
                    dict(step="all")
                ])
            )
        )

    fig.update_layout(
        yaxis=dict(
            title=dict(text='ATP Ranking', font=dict(size=14, color='#444')),
            autorange='reversed',
            gridcolor='rgba(0,0,0,0.1)',
            zeroline=False
        ),
        plot_bgcolor='#ffffff',
        paper_bgcolor='#ffffff',
        hovermode='x',
        showlegend=False,
        margin=dict(l=10, r=10, t=30, b=20),
        font=dict(family="Segoe UI, Arial, sans-serif")
    )

    # If we have no traces (all players skipped due to missing birth dates)
    if x_axis_type == 'age' and not fig.data:
        fig = go.Figure()
        fig.add_annotation(
            text="Cannot display age-based plot:<br>birth date information is missing<br>for selected player(s)",
            xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="#bb2222")
        )
        # Hide axis values and grid lines
        fig.update_xaxes(showticklabels=False, showgrid=False, zeroline=False)
        fig.update_yaxes(showticklabels=False, showgrid=False, zeroline=False)

    # Add a small watermark
    fig.add_annotation(
        text="TennisRank.net",
        xref="paper", yref="paper",
        x=0.99, y=0.01,
        showarrow=False,
        font=dict(size=16, color="lightgrey"),
        opacity=0.7,
        align="right"
    )
        
    return fig


@app.callback(
    Output('player-dropdown', 'options'),
    [Input('player-dropdown', 'search_value')],
    [State('player-dropdown', 'value')]
)
def update_dropdown_options(search_value, current_values):
    if not search_value:
        raise PreventUpdate

    # Use the cached search function
    matching_ids = search_players(search_value)
    
    # Filter player_options based on matching IDs
    filtered_options = [
        option for option in player_options
        if option['value'] in matching_ids
    ]
    
    # Make sure current selections remain in options
    if current_values:
        current_values_set = set(current_values)
        filtered_values_set = {opt['value'] for opt in filtered_options}
        missing_values = current_values_set - filtered_values_set
        
        for value in missing_values:
            # Find the original option from player_options
            matching_options = [opt for opt in player_options if opt['value'] == value]
            if matching_options:
                filtered_options.append(matching_options[0])

    filtered_options.sort(key=lambda x: x['label'])
    return filtered_options


if __name__ == '__main__':
    application.run()