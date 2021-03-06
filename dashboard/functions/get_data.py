import sys, os
import pandas as pd
from pandas.io import gbq
import dash_html_components as html
import dash_core_components as dcc
from google.cloud import bigquery
import time

import data_assets.style as css
import sql.q_homepage as bqq

pd.options.mode.chained_assignment = None

def overall_stats():

    stats_data = {
        'recent_statistics_df': None,
        'global_records': None,
        'global_statistics': None,
        'session_cards': None,
        'choice_table': None,
        'participants_table': None
    }

    # setup credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'bigquery.json'

    project_name = 'f1-telemetry-app'
    client = bigquery.Client()

    print('Bigquery data download...')

    recent_statistics_df = client.query(bqq.recent_statistics, project=project_name).to_dataframe()

    recent_statistics_df['assist_tractionControl'] = recent_statistics_df['assist_tractionControl'].replace({
        0: 'Off',
        1: 'Medium',
        2: 'High'
    })

    recent_statistics_df['assist_antiLockBrakes'] = recent_statistics_df['assist_antiLockBrakes'].replace({
        0: 'Off',
        1: 'On'
    })

    stats_data['recent_statistics_df'] = recent_statistics_df

    last_session = recent_statistics_df.head(1).to_dict('records')[0]

    global_records = {
        'events_won': recent_statistics_df['event_win'].sum().tolist(),
        'fastest_laps': recent_statistics_df['event_fastest_lap'].sum().tolist()
    }

    stats_data['global_records'] = global_records

    global_statistics = {
        'total_time_driven': recent_statistics_df['sessionTime'].sum().tolist(),
        'total_distance_driven': recent_statistics_df['distance_driven'].sum().tolist(),
        'total_datapoints_count': recent_statistics_df['datapoint_count'].sum().tolist(),
    }

    global_statistics['total_time_driven'] = time.strftime(
        '%Hh %Mm %Ss', 
        time.gmtime(
            global_statistics['total_time_driven']
        )
    )

    global_statistics['total_distance_driven'] = '{:,}km'.format(
        global_statistics['total_distance_driven'] / 1000
        ).replace(',', ' ')
    global_statistics['total_datapoints_count'] = '{:,}'.format(
        global_statistics['total_datapoints_count']
    ).replace(',', ' ')

    global_statistics['last_session_time_driven'] = last_session['sessionTime_format']
    global_statistics['last_session_distance_driven'] = last_session['distance_driven_format']
    global_statistics['last_session_datapoints_count'] = last_session['datapoint_count_format']

    stats_data['global_statistics'] = global_statistics

    # recent_statistics_df = recent_statistics_df.loc[recent_statistics_df.index.repeat(12)]
    # recent_statistics_df['team'] = recent_statistics_df['team'].replace({
    #     'Mercedes': 'Charouz Racing System'
    #     })

    session_cards = {
        'All Sessions': {
            'rows': [],
            'count': 0
        },
        'Online': {
            'rows': [],
            'count': 0
        },
        'Offline': {
            'rows': [],
            'count': 0
        },
        'Time Trial': {
            'rows': [],
            'count': 0
        },
        'Practice': {
            'rows': [],
            'count': 0
        },
        'Qualifications': {
            'rows': [],
            'count': 0
        },
        'Race': {
            'rows': [],
            'count': 0
        },
    }

    session_cards['All Sessions']['rows'] = list(range(0, len(recent_statistics_df['sessionType'].tolist())))
    session_cards['All Sessions']['count'] = len(recent_statistics_df['sessionType'].tolist())
    session_cards['Online']['rows'] = [i for i,v in enumerate(recent_statistics_df['networkGame'].tolist()) if v > 0]
    session_cards['Online']['count'] = len(session_cards['Online']['rows'])
    session_cards['Offline']['rows'] = [i for i,v in enumerate(recent_statistics_df['networkGame'].tolist()) if v == 0]
    session_cards['Offline']['count'] = len(session_cards['Offline']['rows'])

    for session_indeks in range(0, len(recent_statistics_df['sessionType'].tolist())):
        single_session = recent_statistics_df['sessionType'].tolist()[session_indeks]
        #12 = Time Trial
        if single_session == 12:
            session_cards['Time Trial']['count'] += 1
            session_cards['Time Trial']['rows'].append(session_indeks)
        #1 = P1, 2 = P2, 3 = P3, 4 = Short P
        elif single_session in [1, 2, 3, 4]:
            session_cards['Practice']['count'] += 1
            session_cards['Practice']['rows'].append(session_indeks)
        #5 = Q1, 6 = Q2, 7 = Q3, 8 = Short Q, 9 = OSQ
        elif single_session in [5, 6, 7, 8, 9]:
            session_cards['Qualifications']['count'] += 1
            session_cards['Qualifications']['rows'].append(session_indeks)
        #10 = R, 11 = R2
        elif single_session in [10, 11]:
            session_cards['Race']['count'] += 1
            session_cards['Race']['rows'].append(session_indeks)

    stats_data['session_cards'] = session_cards

    choice_table = recent_statistics_df[
        ['publish_time', 'team', 'track', 'lap_count', 'sessionTime_format', 'fastest_lap_format', 'sessionUID']
        ]

    choice_table['id'] = range(1, len(choice_table) + 1)
    choice_table['id'] = choice_table['id'].astype(str) + '.'
    cols = list(choice_table)
    # move the column to head of list using index, pop and insert
    cols.insert(0, cols.pop(cols.index('id')))

    choice_table = choice_table.loc[:, cols]
    
    choice_table = choice_table.rename(columns={
        'id': '', 
        'publish_time': 'Session Date', 
        'team': 'Team', 
        'track': 'Session Track', 
        'lap_count': 'Laps', 
        'sessionTime_format': 'Session Duration', 
        'fastest_lap_format': 'Fastest Lap'
        })

    teams_boxes = []
    for single_team in choice_table['Team'].tolist():
        teams_boxes.append(
            '![{}](assets/images/teams/{}.svg) '.format(single_team, single_team.replace(' ', '_'))
        )

    choice_table['Team'] = teams_boxes + choice_table['Team']

    stats_data['choice_table'] = choice_table

    return stats_data
