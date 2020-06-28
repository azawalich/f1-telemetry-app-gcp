import sys, os
import pandas as pd
from pandas.io import gbq
import dash_html_components as html
import dash_core_components as dcc
from google.cloud import bigquery
import dash_table
import datetime
import time

import data_assets.nationalities as nat
import sql.q_summary as bqq

from dash.dependencies import Input, Output

from app import app

pd.options.mode.chained_assignment = None

participants_data_call = None

def get_summary_data(sessionUID, session_type):
    print('loading time: {}'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    # setup credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'bigquery.json'

    project_name = 'f1-telemetry-app'
    client = bigquery.Client()

    print('Bigquery participants data download...')

    participants_replaced = bqq.summary_participants.replace('$_sessionUID', sessionUID)
    laps_replaced = bqq.summary_laps.replace('$_sessionUID', sessionUID)
    car_status_replaced = bqq.summary_car_status.replace('$_sessionUID', sessionUID)

    summary_participants_df = client.query(participants_replaced, project=project_name).to_dataframe()
    summary_laps_df_flat = client.query(laps_replaced, project=project_name).to_dataframe()
    summary_car_status_df_flat = client.query(car_status_replaced, project=project_name).to_dataframe()

    summary_participants_df['participant_grouping_id'] = list(range(0, summary_participants_df.shape[0]))

    participant_grouping_ids = list(range(0, int(max(summary_laps_df_flat['carPosition'].tolist()))))

    summary_laps_df_flat['participant_grouping_id'] =  participant_grouping_ids * int(summary_laps_df_flat.shape[0] / len(participant_grouping_ids))
    summary_car_status_df_flat['participant_grouping_id'] =  participant_grouping_ids * int(summary_car_status_df_flat.shape[0] / len(participant_grouping_ids))

    #total laps
    total_laps_df = pd.DataFrame(summary_laps_df_flat.groupby(['participant_grouping_id'])['currentLapNum'].max()).reset_index()
    total_laps_df = total_laps_df.rename(columns = {'currentLapNum': 'total_laps'})
    #fastest lap
    fastest_lap_df = pd.DataFrame(summary_laps_df_flat.groupby(['participant_grouping_id'])['lastLapTime'].max()).reset_index()
    fastest_lap_df = fastest_lap_df.rename(columns = {'lastLapTime': 'fastest_lap'})

    fastest_lap_df['fastest_lap_format'] = fastest_lap_df['fastest_lap'].apply((lambda x: str(
            datetime.timedelta(seconds=x)
            )[2:-3]))

    # tires
    tires_final_joined = pd.DataFrame()

    for participant in summary_laps_df_flat['participant_grouping_id'].drop_duplicates().tolist():

        pitstops_info = summary_laps_df_flat[summary_laps_df_flat['participant_grouping_id'] == participant].\
            groupby(['pitStatus', 'currentLapNum']).tail(1)[['currentLapNum', 'pitStatus', 'sessionTime', 'participant_grouping_id']]
        pitstops_info['sessionTime_rounded'] = pitstops_info['sessionTime'].round(0)

        tires_info = summary_car_status_df_flat[summary_car_status_df_flat['participant_grouping_id'] == participant][['tyreVisualCompound', 'sessionTime', 'participant_grouping_id']]
        tires_info['sessionTime_rounded'] = tires_info['sessionTime'].round(0)

        #16 = soft, 17 = medium, 18 = hard, 7 = inter, 8 = wet 
        tires_info['tyreVisualCompound'] = tires_info['tyreVisualCompound'].replace({
            16: 'Soft',
            17: 'Medium',
            18: 'Hard',
            7: 'Inter',
            8: 'Wet'
        })

        pitstops_tires_merged = pd.merge(pitstops_info, tires_info, on=['participant_grouping_id', 'sessionTime_rounded'], how='left')

        tires_laps = pitstops_tires_merged[pitstops_tires_merged['pitStatus'] == 0].groupby(['participant_grouping_id', 'currentLapNum', 'tyreVisualCompound']).head(1).drop_duplicates('tyreVisualCompound')

        tyre_indexes = list(pitstops_tires_merged[pitstops_tires_merged['pitStatus'] == 1].index + 1)

        if len(tyre_indexes) == 0:
            tyre_indexes = [0]
        if len(set(tyre_indexes) - set(pitstops_tires_merged.index)) > 0:
            tyre_indexes = tyre_indexes[:-1]
        if len(set(tires_laps.index) - set(tyre_indexes)) > 0:
            tyre_indexes = tires_laps.index

        tires_final = pd.DataFrame(pitstops_tires_merged.loc[tyre_indexes,:][['participant_grouping_id', 'tyreVisualCompound']].\
            groupby('participant_grouping_id')['tyreVisualCompound'].apply(list)).rename(columns={'tyreVisualCompound': 'stint'})

        tires_final_joined = tires_final_joined.append(tires_final)

    # pit stops
    tires_final_joined['pit_stops'] = tires_final_joined['stint'].str.len() - 1
    tires_final_joined

    # full join
    laps_full_df_joined = pd.merge(fastest_lap_df, total_laps_df, on='participant_grouping_id', how='left').\
        merge(tires_final_joined, on='participant_grouping_id', how='left').sort_values('fastest_lap_format')

    laps_full_df_joined['gap'] = laps_full_df_joined['fastest_lap'].diff().round(3).astype(str)
    laps_full_df_joined.loc[laps_full_df_joined[laps_full_df_joined['gap'] == 'nan'].index, 'gap'] = ''

    full_df_joined = pd.merge(summary_participants_df, laps_full_df_joined, on='participant_grouping_id', how='left').sort_values('fastest_lap')

    full_df_joined['name_short'] = full_df_joined['name'].str.split(' ').apply(lambda x: x[1][0:3].upper())

    full_df_joined = full_df_joined[
        ['name_short', 'nationality', 'team', 'total_laps', 'fastest_lap_format', 'gap', 'pit_stops', 'stint', 'yourTelemetry', 'participant_grouping_id']
        ]

    if session_type in ['Race', 'Race 2']:
        #total time
        laps_grouped_df = summary_laps_df_flat.groupby(['participant_grouping_id', 'currentLapNum']).tail(1)

        first_laps = laps_grouped_df.groupby(['participant_grouping_id']).head(1)[['participant_grouping_id', 'currentLapTime']]
        first_laps = first_laps.rename(columns = {'currentLapTime': 'first_lap'})

        total_time_df = pd.DataFrame(laps_grouped_df.groupby(['participant_grouping_id'])['lastLapTime'].sum()).reset_index()
        total_time_df = total_time_df.rename(columns = {'lastLapTime': 'total_time_laps'})

        penalties_df = pd.DataFrame(summary_laps_df_flat[['participant_grouping_id', 'penalties']].groupby(['participant_grouping_id']).max()).reset_index()

        # TODO: what about pitstop time?

        total_time_df_joined = pd.merge(total_time_df, first_laps, on='participant_grouping_id', how='left').\
            merge(penalties_df, on='participant_grouping_id', how='left')

        total_time_df_joined['full_total_time'] = total_time_df_joined['first_lap'] + total_time_df_joined['total_time_laps'] + total_time_df_joined['penalties'] 

        full_df_joined = pd.merge(full_df_joined, total_time_df_joined, on='participant_grouping_id', how='left').sort_values('full_total_time')

        full_df_joined['int'] = full_df_joined['full_total_time'] - full_df_joined['full_total_time'].tolist()[0]
        full_df_joined['int'] = full_df_joined['int'].round(3).astype(str) + 's'
        full_df_joined.loc[full_df_joined[full_df_joined['int'] == '0.0s'].index, 'int'] = ''

        full_df_joined['full_total_time_format'] = full_df_joined['full_total_time'].apply(
            lambda x: time.strftime(
                '%Hh %Mm %Ss', 
                time.gmtime(x)
                )
            )

        full_df_joined = full_df_joined.drop(columns = 'full_total_time')

        full_df_joined = full_df_joined[
        ['name_short', 'nationality', 'team', 'total_laps', 'full_total_time_format', 'int', 'fastest_lap_format', 'gap', 'pit_stops', 'stint',  'yourTelemetry', 'participant_grouping_id']
        ]

    full_df_joined = full_df_joined.drop(columns = 'participant_grouping_id')

    full_df_joined['id'] = range(1, len(full_df_joined) + 1)
    full_df_joined['id'] = full_df_joined['id'].astype(str) + '.'
    cols = list(full_df_joined)
    # move the column to head of list using index, pop and insert
    cols.insert(0, cols.pop(cols.index('id')))

    full_df_joined = full_df_joined.loc[:, cols]

    full_df_joined = full_df_joined.rename(columns={
        'id': '', 
        'name_short': 'Name',
        'nationality': 'Nat.',
        'team': 'Team', 
        'total_laps': 'Laps', 
        'fastest_lap_format': 'Fastest Lap',
        'pit_stops': 'Pits',
        'stint': 'Stint',
        'gap': 'Gap', 
        })

    teams_boxes = []
    for single_team in full_df_joined['Team'].tolist():
        teams_boxes.append(
            '![{}](assets/images/teams/{}.svg) '.format(single_team, single_team.replace(' ', '_'))
        )

    full_df_joined['Name'] = teams_boxes + full_df_joined['Name']

    nationality_flags = []
    for single_nationality in full_df_joined['Nat.'].tolist():
        nationality_flags.append(
            '![{}]({})'.format(
                nat.NATIONALITIES[single_nationality]['country'],
                nat.NATIONALITIES[single_nationality]['flag_url']
                )
        )
    
    full_df_joined['Nat.'] = nationality_flags

    stint_icons = []
    for single_participant_stint in full_df_joined['Stint'].tolist():
        single_participant_stint_temp = ''
        for single_stint in single_participant_stint:
            single_participant_stint_temp = '{} ![{}](assets/images/tires/{}.svg)'.format(
                single_participant_stint_temp, 
                single_stint, 
                single_stint
                )
            
        stint_icons.append(single_participant_stint_temp)
    
    full_df_joined['Stint'] = stint_icons

    if session_type in ['Race', 'Race 2']:
        full_df_joined = full_df_joined.rename(columns={
        'int': 'Int.', 
        'full_total_time_format': 'Time',
        })

    final_df_splitted = (
        full_df_joined[full_df_joined['yourTelemetry'] == 0].drop(columns='yourTelemetry'), 
        full_df_joined[full_df_joined['yourTelemetry'] == 1].drop(columns='yourTelemetry')
    )

    print('loading time: {}'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    return final_df_splitted

def summary_wrapper(sessionUID, session_type, page_size):
    participants_elements = []
    
    #12 = Time Trial
    if session_type == 12:
        session_type = 'Time Trial'
    #1 = P1, 2 = P2, 3 = P3, 4 = Short P
    elif session_type in [1, 2, 3]:
        session_type = 'Practice {}'.format(session_type)
    elif session_type in [4]:
        session_type = 'Short Practice'
    #5 = Q1, 6 = Q2, 7 = Q3, 8 = Short Q, 9 = OSQ
    elif session_type in [5, 6, 7]:
        session_type = 'Qualification {}'.format(session_type - 4)
    elif session_type in [8]:
        session_type = 'Short Qualification'
    elif session_type in [9]:
        session_type = 'Hot Lap Qualification'
    #10 = R, 11 = R2
    elif session_type in [10]:
        session_type = 'Race'
    elif session_type in [11]:
        session_type = 'Race {}'.format(session_type - 9)
    
    summary_data = get_summary_data(sessionUID, session_type)
    
    your_data = summary_data[0]
    
    if len(summary_data) > 1:
        participants_data = summary_data[1]
        pages_count = int(round(participants_data.shape[0] / page_size, 0))
        if session_type != 'Time Trial':
            participants_elements = [
                html.Div(
                        html.H1(
                            '{} Classification'.format(session_type)
                        ),
                        id='subtitle-wrapper'
                    ),
            dash_table.DataTable(
            id='datatable-3-paging-page-count',
            columns=[{"name": i, "id": i, 'presentation': 'markdown'} if i in ['Name', 'Nat.', 'Stint'] else {"name": i, "id": i} for i in participants_data.columns],
            # data=participants_data.to_dict('records'),
            filter_query='',
            page_current=0,
            page_size=page_size,
            page_action='custom',
            page_count=pages_count,
            style_header={'border': '0 !important'},
            style_cell={'textAlign': 'left'},
            # style_cell_conditional=[
            #     {
            #         'if': {'column_id': ''},
            #         'width': '20px'
            #     },
            #     {
            #         'if': {'column_id': 'Team'},
            #         'width': '222px'
            #     },
            #     {
            #         'if': {'column_id': 'Session Time'},
            #         'width': '250px'
            #     },
            #     {
            #         'if': {'column_id': 'Session Track'},
            #         'width': '167px'
            #     },
            #     {
            #         'if': {'column_id': 'Laps'},
            #         'width': '96px'
            #     },
            #     {
            #         'if': {'column_id': 'Session Duration'},
            #         'width': '133px'
            #     }
            # ]
            )
        ]

    elements_list = html.Div(
        [
        html.Div(
                    html.H1(
                        'Your {}'.format(session_type)
                    ),
                    id='subtitle-wrapper'
                ),
        dash_table.DataTable(
        id='datatable-2-paging-page-count',
        columns=[{"name": i, "id": i, 'presentation': 'markdown'} if i in ['Name', 'Nat.', 'Stint'] else {"name": i, "id": i} for i in your_data.columns],
        data=your_data.to_dict('records'),
        filter_query='',
        page_current=0,
        page_size=page_size,
        page_action='custom',
        page_count=1,
        style_header={'border': '0 !important'},
        style_cell={'textAlign': 'left'},
        # style_cell_conditional=[
        #     {
        #         'if': {'column_id': ''},
        #         'width': '20px'
        #     },
        #     {
        #         'if': {'column_id': 'Team'},
        #         'width': '222px'
        #     },
        #     {
        #         'if': {'column_id': 'Session Time'},
        #         'width': '250px'
        #     },
        #     {
        #         'if': {'column_id': 'Session Track'},
        #         'width': '167px'
        #     },
        #     {
        #         'if': {'column_id': 'Laps'},
        #         'width': '96px'
        #     },
        #     {
        #         'if': {'column_id': 'Session Duration'},
        #         'width': '133px'
        #     }
        # ]
    )] + participants_elements,
        id='page-content',
        style={'height': '690px'}
    )
    
    final_tuple = (
        elements_list,
        participants_data
    )

    return final_tuple