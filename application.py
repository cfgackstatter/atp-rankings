import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash.exceptions import PreventUpdate
from functools import lru_cache

# Import data loading functions
from src.data_loader import load_players, load_rankings

# Initialize the Dash app
app = dash.Dash(__name__,
                title='ATP Ranking Chart',
                suppress_callback_exceptions=True,
                use_pages=False,
                meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}])
app.enable_dev_tools(debug=True, dev_tools_props_check=False)
server = app.server

# Load data at startup
print("Loading player data...")
players_df = load_players()

print("Loading rankings data...")
rankings_df = load_rankings()

# Create a function to generate player options once
def generate_player_options(players_df):
    options = []
    for _, player in players_df.iterrows():
        # Handle NaN values safely
        first_name = "" if pd.isna(player['first_name']) else player['first_name']
        last_name = "" if pd.isna(player['last_name']) else player['last_name']
        player_id = player['player_id']
        
        # Skip players with empty names
        if first_name == '' and last_name == '':
            continue
            
        # Format display name as "Last Name, First Name"
        display_name = f"{last_name}, {first_name}"

        # Add country code if available
        country_code = "" if pd.isna(player.get('country_code', '')) else player.get('country_code', '')
        if country_code:
            display_name += f" ({country_code})"

        # Add birth year if available
        birth_date = player.get('birth_date')
        birth_year = None
        if birth_date and not pd.isna(birth_date):
            try:
                birth_year = pd.to_datetime(birth_date).year
                display_name += f" - *{birth_year}"
            except:
                pass

        options.append({
            'label': display_name,
            'value': player_id,
            'search': f"{first_name} {last_name} {first_name} {country_code}"
        })
    
    # Sort options by last name
    options.sort(key=lambda x: x['label'])
    return options

# Generate options once at startup
player_options = generate_player_options(players_df)

# After loading data, create a search index
player_search_index = {}
for _, player in players_df.iterrows():
    player_id = player['player_id']
    first_name = "" if pd.isna(player['first_name']) else str(player['first_name']).lower()
    last_name = "" if pd.isna(player['last_name']) else str(player['last_name']).lower()
    
    # Skip players with empty names
    if first_name == '' and last_name == '':
        continue

    # Add to search index with all possible search terms
    search_terms = []

    # Add individual names
    if first_name:
        search_terms.append(first_name)
    
    if last_name:
        search_terms.append(last_name)

        # Add individual parts of compound last names
        last_name_parts = last_name.split()
        if len(last_name_parts) > 1:
            for part in last_name_parts:
                if len(part) > 2:  # Avoid indexing very short parts
                    search_terms.append(part)
    
    # Add combined names in different formats
    if first_name and last_name:
        search_terms.append(f"{first_name} {last_name}")  # First Last
        search_terms.append(f"{last_name} {first_name}")  # Last First
        search_terms.append(f"{last_name}, {first_name}") # Last, First

        # For compound last names, add variations with first name
        last_name_parts = last_name.split()
        if len(last_name_parts) > 1:
            for part in last_name_parts:
                if len(part) > 2:
                    search_terms.append(f"{part} {first_name}")  # LastPart First
                    search_terms.append(f"{first_name} {part}")  # First LastPart
    
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
            player_search_index[term].append(player_id)

# Add a cached search function
@lru_cache(maxsize=1024)
def search_players(query):
    query = query.lower().strip()
    matching_ids = set()

    # Split the query into parts to handle multi-word searches better
    query_parts = query.split()

    # If no exact matches, try starts-with for each term
    if not matching_ids:
        for term, ids in player_search_index.items():
            if term.startswith(query):
                matching_ids.update(ids)
    
    # If still no matches, try matching individual parts
    if not matching_ids and len(query_parts) > 1:
        # For multi-word queries, try to match each part
        potential_matches = {}
        
        for part in query_parts:
            if len(part) < 3:  # Skip very short parts
                continue
                
            for term, ids in player_search_index.items():
                if part in term.split():
                    for player_id in ids:
                        potential_matches[player_id] = potential_matches.get(player_id, 0) + 1

        # Find players that match multiple parts of the query
        for player_id, match_count in potential_matches.items():
            if match_count >= min(2, len(query_parts)):  # Match at least 2 parts or all parts if fewer
                matching_ids.add(player_id)
        
    # If still no matches, try contains matching
    if not matching_ids:
        for term, ids in player_search_index.items():
            if query in term:
                matching_ids.update(ids)
    
    # If still no matches, try partial matching for longer queries
    if not matching_ids and len(query) >= 3:
        for term, ids in player_search_index.items():
            # Match if at least 70% of the query is in the term
            if len(query) >= 3 and any(
                query[:int(len(query)*0.7)] in word 
                for word in term.split()
            ):
                matching_ids.update(ids)
    
    return matching_ids

# App layout
app.layout = html.Div([
    # Compact header with logo
    html.Div([
        # Left side: Logo and title in a row
        html.Div([
            # Logo placeholder
            html.Img(src='/assets/logo.svg', height=40, style={'marginRight': '15px'}),
            # Title next to logo, not above content
            html.H1("ATP RankTracker", style={'margin': '0', 'fontSize': '24px'})
        ], style={'display': 'flex', 'alignItems': 'center'}),

        # Right side: X-Axis toggle
        html.Div([
            # X-Axis toggle in header
            html.Label("X-Axis:", className="control-label-inline"),
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
        ], style={'marginLeft': 'auto'})
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
        html.Footer([
            html.P([
                "Data: ",
                html.A(
                    "Jeff Sackmann's tennis_atp", 
                    href="https://github.com/JeffSackmann/tennis_atp", 
                    target="_blank",
                    rel="noopener noreferrer"
                ),
                " | License: CC BY-NC-SA 4.0"
            ], style={'fontSize': '0.8rem', 'margin': '5px 0'}),
        ], className="footer")
    ], className="main-content"),
], className="container")


@app.callback(
    Output('rankings-graph', 'figure'),
    [Input('player-dropdown', 'value'),
     Input('x-axis-toggle', 'value')]
)
def update_graph(selected_player_ids, x_axis_type):
    # Handle no selection
    if not selected_player_ids:
        # Create empty figure with date range
        earliest_date = rankings_df['ranking_date'].min()
        current_date = pd.Timestamp.now()
        fig = go.Figure()

        # Update x-axis to show the full date range
        fig.update_layout(
            plot_bgcolor='#f8f9fa',
            paper_bgcolor='#ffffff',
            xaxis=dict(
                title='Date',
                range=[earliest_date, current_date],
                type='date',
                gridwidth=1,
                gridcolor='rgba(0,0,0,0.1)'
            ),
            yaxis=dict(
                title='ATP Ranking',
                range=[1000, 1],
                autorange='reversed',
                gridwidth=1,
                gridcolor='rgba(0,0,0,0.1)'
            ),
            margin=dict(l=50, r=30, t=40, b=20, pad=0)
        )
        # Add a helpful annotation
        fig.add_annotation(
            text="Select players from the dropdown above to visualize their ranking history",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color="#666666")
        )
        
        return fig
    
    # Create figure
    fig = go.Figure()
    
    # Colors for different players
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

    # Pre-fetch all player info at once instead of in the loop
    player_info_dict = {}
    for player_id in selected_player_ids:
        player_info = players_df.query(f"player_id == {player_id}")
        if not player_info.empty:
            player_info_dict[player_id] = player_info.iloc[0]
    
    messages = []
    all_x_values = []  # To store all x values for padding calculation

    for i, player_id in enumerate(selected_player_ids):
        # Use query for faster filtering when possible
        try:
            player_data = rankings_df.query(f"player_id == {player_id}")
        except Exception:
            # Fall back to boolean indexing if query fails
            player_data = rankings_df[rankings_df['player_id'] == player_id]

        if player_data.empty:
            # Get player name from pre-fetched info
            player_info = player_info_dict.get(player_id)
            if player_info is not None:
                first_name = player_info['first_name'] or ""
                last_name = player_info['last_name'] or ""
                player_name = f"{first_name} {last_name}".strip()
            else:
                player_name = f"Player {player_id}"
                
            messages.append(f"No ranking data found for {player_name}")
            continue

        # Sort once - more efficient than sort_values for single column
        player_data = player_data.sort_values('ranking_date')

        # Get player name from pre-fetched info
        player_info = player_info_dict.get(player_id)
        if player_info is not None:
            first_name = player_info['first_name'] or ""
            last_name = player_info['last_name'] or ""
            player_name = f"{first_name} {last_name}".strip()
        else:
            player_name = f"Player {player_id}"
            
        if x_axis_type == 'age':
            # Check if we have player info and birth date
            if player_info is not None and 'birth_date' in player_info and pd.notna(player_info['birth_date']):
                birth_date = pd.to_datetime(player_info['birth_date'])
                
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
                    line=dict(color=colors[i % len(colors)], width=2.5, shape='spline'),
                    hovertemplate='<b>%{fullData.name}</b><br>Rank: %{y}<extra></extra>'
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
                line=dict(color=colors[i % len(colors)], width=2.5, shape='spline'),
                hovertemplate='<b>%{fullData.name}</b><br>Rank: %{y}<extra></extra>'
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
            padding = pd.Timedelta(days=int(date_range.days * 0.05))  # 5% padding
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
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor='rgba(255,255,255,0.9)',
            bordercolor='rgba(0,0,0,0.1)',
            borderwidth=1
        ),
        margin=dict(l=50, r=30, t=80, b=50),
        font=dict(family="Segoe UI, Arial, sans-serif")
    )
    
    message = "Displaying ranking data for selected players." if not messages else "\n".join(messages)
    
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
    app.run(debug=True, port=8080)