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
    'data': None
}

# setup credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'bigquery.json'

project_name = 'f1-telemetry-app'
client = bigquery.Client()

def overall_stats():

    # big query data reading
    recent_statistics_df = client.query(bqq.recent_statistics, project=project_name).to_dataframe()
    
    session_times = recent_statistics_df['last_session_publish_time'].tolist()

    for single_session in session_times:

        sessions_dropdown_dummy = {
            'label': single_session,
            'value': single_session
        }
        
        tiles_data['sessions_dropdown'].append(sessions_dropdown_dummy)
    
    temp_dict = recent_statistics_df.head(1).to_dict('record')[0]
    
    del(
        [
            temp_dict['last_sessionUID'], 
            temp_dict['last_session_publish_time'], 
            temp_dict['last_session_type']
            ]
        )
    tiles_data['data'] = temp_dict

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