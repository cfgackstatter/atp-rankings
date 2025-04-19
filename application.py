import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash.exceptions import PreventUpdate

# Import data loading functions
from src.data_loader import load_players, load_rankings, reduce_mem_usage

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

players_df = reduce_mem_usage(players_df)
rankings_df = reduce_mem_usage(rankings_df)

# Create a function to generate player options once
def generate_player_options(players_df):
    options = []
    for _, player in players_df.iterrows():
        first_name = player['first_name']
        last_name = player['last_name']
        player_id = player['player_id']
        
        # Skip players with empty names
        if first_name == '' and last_name == '':
            continue
            
        # Format display name as "Last Name, First Name"
        display_name = f"{last_name}, {first_name}"

        # Add country code if available
        country_code = player.get('country_code', '')
        if country_code:
            display_name += f" ({country_code})"

        # Add birth year if available to help distinguish players with same name
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
            'search': f"{first_name} {last_name} {last_name} {first_name} {country_code}"
        })
    
    # Sort options by last name
    options.sort(key=lambda x: x['label'])
    return options

# Generate options once at startup
player_options = generate_player_options(players_df)

# App layout
app.layout = html.Div([
    # Header section
    html.Div([
        html.H1("ATP Rankings Chart", style={'textAlign': 'center'}),
        html.P("Explore historical ATP tennis rankings data from 1970s to present",
               style={'textAlign': 'center', 'color': '#666'})
    ], className="header-container"),
    
    # Controls section (left-aligned for F-pattern visibility)
    html.Div([
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
            ], className="control-group"),
            
            # X-Axis toggle
            html.Div([
                html.Label("X-Axis:", className="control-label"),
                dcc.RadioItems(
                    id='x-axis-toggle',
                    options=[
                        {'label': 'Date', 'value': 'date'},
                        {'label': 'Age', 'value': 'age'}
                    ],
                    value='date',
                    labelStyle={'display': 'inline-block', 'marginRight': '10px'},
                    className="radio-group"
                ),
            ], className="control-group"),
        ], className="controls-container"),

        # Chart container
        html.Div([
            dcc.Loading(
                id="loading",
                type="circle",
                children=[dcc.Graph(id='rankings-graph', className="main-chart")]
            ),
        ], className="chart-container"),

        # Messages and feedback
        html.Div(id='output-message', className="message-container"),
    ], className="main-content"),

    # Footer   
    html.Footer([
        html.P([
            "Data source: ",
            html.A(
                "Jeff Sackmann's tennis_atp repository", 
                href="https://github.com/JeffSackmann/tennis_atp", 
                target="_blank",
                rel="noopener noreferrer"
            )
        ]),
        html.P("License: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License")
    ], className="footer")
], className="container")


@app.callback(
    [Output('rankings-graph', 'figure'),
     Output('output-message', 'children')],
    [Input('player-dropdown', 'value'),
     Input('x-axis-toggle', 'value')]
)
def update_graph(selected_player_ids, x_axis_type):
    # Handle no selection
    if not selected_player_ids:
        # Get the earliest date from the rankings data
        earliest_date = rankings_df['ranking_date'].min()
        
        # Get current date
        current_date = pd.Timestamp.now()

        # Create an empty figure with the date range
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
            margin=dict(l=50, r=30, t=80, b=50)
        )
        # Add a helpful annotation
        fig.add_annotation(
            text="Select players from the dropdown above to visualize their ranking history",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color="#666666")
        )
        
        return fig, "No players selected. Please select one or more players from the dropdown."
    
    # Load rankings data for selected decades
    filtered_rankings = rankings_df
    
    # Create figure
    fig = go.Figure()
    
    # Colors for different players
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
    
    messages = []
    for i, player_id in enumerate(selected_player_ids):
        # Get player data
        player_data = filtered_rankings[filtered_rankings['player_id'] == player_id]
        player_data = player_data.sort_values('ranking_date')
        
        # Get player name
        player_info = players_df[players_df['player_id'] == player_id]
        if len(player_info) > 0:
            first_name = player_info.iloc[0]['first_name']
            last_name = player_info.iloc[0]['last_name']
            player_name = f"{first_name} {last_name}"
        else:
            player_name = f"Player {player_id}"
        
        if len(player_data) == 0:
            messages.append(f"No ranking data found for {player_name}")
            continue
            
        if x_axis_type == 'age':
            # Get player's birth date
            if len(player_info) > 0 and 'birth_date' in player_info.columns:
                birth_date = pd.to_datetime(player_info.iloc[0]['birth_date'])
                
                # Calculate age at each ranking date
                player_data['age'] = (player_data['ranking_date'] - birth_date).dt.days / 365.25
                
                # Plot with age on x-axis
                fig.add_trace(go.Scatter(
                    x=player_data['age'],
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
            # Plot with date on x-axis
            fig.add_trace(go.Scatter(
                x=player_data['ranking_date'],
                y=player_data['rank'],
                mode='lines',
                name=player_name,
                line=dict(color=colors[i % len(colors)], width=2.5, shape='spline'),
                hovertemplate='<b>%{fullData.name}</b><br>Rank: %{y}<extra></extra>'
            ))
    
    # Set up layout
    if x_axis_type == 'age':
        x_axis_title = 'Age (years)'
        fig.update_xaxes(
            title=dict(text=x_axis_title, font=dict(size=14, color='#444')),
            tickformat='.1f',
            gridcolor='rgba(0,0,0,0.1)',
            zeroline=True,
            zerolinecolor='rgba(0,0,0,0.2)'
        )
    else:
        x_axis_title = 'Date'
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
    
    return fig, message


@app.callback(
    Output('player-dropdown', 'options'),
    [Input('player-dropdown', 'search_value')],
    [State('player-dropdown', 'value')]
)
def update_dropdown_options(search_value, current_values):
    if not search_value:
        raise PreventUpdate
    
    # Filter options based on search value
    filtered_options = []
    search_value = search_value.lower()
    
    for _, player in players_df.iterrows():
        first_name = player['first_name'].lower()
        last_name = player['last_name'].lower()
        player_id = player['player_id']

        # Skip players with empty first AND last names
        if first_name == '' and last_name == '':
            continue

        # Display name in "Last Name, First Name" format
        display_name = f"{player['last_name']}, {player['first_name']}"
        
        # Check if search value is in first name, last name, or combined
        if (search_value in first_name or 
            search_value in last_name or 
            search_value in f"{first_name} {last_name}" or
            search_value in f"{last_name} {first_name}"):
            
            filtered_options.append({
                'label': display_name,
                'value': player_id
            })
    
    # Make sure current selections remain in options
    if current_values:
        for value in current_values:
            if value not in [opt['value'] for opt in filtered_options]:
                player_info = players_df[players_df['player_id'] == value]
                if len(player_info) > 0:
                    filtered_options.append({
                        'label': f"{player_info.iloc[0]['last_name']}, {player_info.iloc[0]['first_name']}",
                        'value': value
                    })
    filtered_options.sort(key=lambda x: x['label'])
    
    return filtered_options


if __name__ == '__main__':
    app.run(debug=True, port=8080)