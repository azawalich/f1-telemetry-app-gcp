import sys, os
import pandas as pd
from pandas.io import gbq
import dash_html_components as html
import dash_core_components as dcc
from google.cloud import bigquery

sys.path.append(os.path.abspath(os.path.join('assets')))
sys.path.append(os.path.abspath(os.path.join('sql')))

import assets.css_classes as css
import sql.q_homepage as bqq
import tiles_stats

tiles_data = {
    'sessions_dropdown': [],
    'data': {
        'time_driven': '',
        'total_datapoint_count': '',
        'last_session_datapoint_count': '',
        'overall_session_count': '',
        'free_training_count': '',
        'event_count': '',
        'event_win_count': '',
        'event_fastest_laps_count': ''
    }
}

# setup credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'bigquery.json'

project_name = 'f1-telemetry-app'
client = bigquery.Client()

def overall_stats():

    # big query data reading
    row_count_df = client.query(bqq.row_count_tables, project=project_name).to_dataframe()
    session_count_df = client.query(bqq.distinct_sessions, project=project_name).to_dataframe()
    row_count_sessions_df = client.query(bqq.row_count_tables_sessions, project=project_name).to_dataframe()
    time_driven_df = client.query(bqq.time_driven_sessions, project=project_name).to_dataframe()
    event_sessions_df = client.query(bqq.event_sessions, project=project_name).to_dataframe()

    # creating stats 
    #### total_datapoint_count
    tiles_data['data']['total_datapoint_count'] = '{}K'.format(
        int(
            round(
                row_count_df['row_count'].sum() / 1000
                )
            )
        )
    
    #### overall_session_count
    tiles_data['data']['overall_session_count'] = session_count_df['sessionUID'].shape[0]

    #### last_insert_datapoint_count
    last_session = session_count_df[session_count_df['insert_time'] == session_count_df['insert_time'].max()]['sessionUID'][0]
    
    tiles_data['data']['last_session_datapoint_count'] = '{}K'.format(
        int(
            round(
                row_count_sessions_df[row_count_sessions_df['sessionUID'] == last_session]['row_count'].sum() / 1000, 0
                )
            )
        )
    
    #### time_driven
    session_start_times = time_driven_df.groupby(['sessionUID'])['min_publish_time'].min()
    session_end_times = time_driven_df.groupby(['sessionUID'])['max_publish_time'].max()

    time_driven_df_joined = pd.concat([session_end_times, session_start_times], axis=1).reset_index()
    time_driven_df_joined['session_length'] = time_driven_df_joined['max_publish_time'] - time_driven_df_joined['min_publish_time']
    
    hours_value = int(
            round(
                time_driven_df_joined['session_length'].sum().seconds / 3600
            )
        )

    if hours_value == 0:
        message = '<{}h'
    else:
        message = '{}h'
    
    tiles_data['data']['time_driven'] = message.format(
        hours_value
    )

    #### event_count    
    tiles_data['data']['event_count'] = len(event_sessions_df['sessionUID'].drop_duplicates().tolist())

    #### free_training_count
    tiles_data['data']['free_training_count'] = time_driven_df_joined.shape[0] - tiles_data['data']['event_count']

    #### event_fastest_laps_count
    event_player_events = event_sessions_df[event_sessions_df['playerCarIndex'] == event_sessions_df['vehicleIdx']]

    tiles_data['data']['event_fastest_laps_count'] = event_player_events[event_player_events['value'] == 'Fastest Lap'].shape[0]
    
    #### event_win_count
    tiles_data['data']['event_win_count'] = event_player_events[event_player_events['value'] == 'Race Winner'].shape[0]

    for single_session in time_driven_df_joined['min_publish_time'].tolist():
        
        single_session_replaced = str(single_session).replace('T', ' ')

        sessions_dropdown_dummy = {
            'label': single_session_replaced,
            'value': single_session_replaced
        }
        
        tiles_data['sessions_dropdown'].append(sessions_dropdown_dummy)

    return tiles_data

stats_data = overall_stats()
tiles_dict = tiles_stats.create_tiles(data_dict = stats_data['data'])

def tile_bar():

    page_render = html.Div([
        html.H1(
            'Overall Statistics',
            style=css.SECTION_HEADINGS
            ),
        html.Div(
            tiles_dict,
            style=css.TILE_WRAPPER
        ),
        html.H1(
            'Choose session from a dropdown below!', 
            style=css.SECTION_HEADINGS
        ),
        html.Br(),
        dcc.Dropdown(
            id='session-dropdown',
            options=stats_data['sessions_dropdown'],
            value=stats_data['sessions_dropdown'][0],
            style={
                'float': 'left', 
                'margin-left': '60px',
                'width': '250px'
                },
            clearable=False,
            searchable=False
    ),
    html.Br(),
    html.Br(),
    html.Div(
            className="lead",
            id="cta-prompt"
        ),
    html.Br()
    ])
    
    return page_render