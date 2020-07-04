import sys, os
import numpy as np
import pandas as pd
from pandas.io import gbq
import dash_html_components as html
import dash_core_components as dcc
from google.cloud import bigquery
import dash_table
import datetime
import time

import data_assets.nationalities as nat
import data_assets.sections as sct
import sql.q_laps as bqq

from dash.dependencies import Input, Output

from app import app

pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', None)
participants_data_call = None

def get_laps_data(sessionUID, session_type, record_lap):
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

    summary_car_status_df_flat['sessionTime_rounded'] = summary_car_status_df_flat['sessionTime'].round(0)
    summary_car_status_df_flat = summary_car_status_df_flat.drop(columns = ['sessionTime', 'playerCarIndex']).\
        rename(columns={'tyreVisualCompound': 'tires'})
    #16 = soft, 17 = medium, 18 = hard, 7 = inter, 8 = wet 
    summary_car_status_df_flat['tires'] = summary_car_status_df_flat['tires'].replace({
        16: 'Soft',
        17: 'Medium',
        18: 'Hard',
        7: 'Inter',
        8: 'Wet'
    })

    summary_participants_df['participant_grouping_id'] = list(range(0, summary_participants_df.shape[0]))

    participant_grouping_ids = list(range(0, int(max(summary_laps_df_flat['carPosition'].tolist()))))
    summary_laps_df_flat['participant_grouping_id'] = np.tile(
        participant_grouping_ids, len(summary_laps_df_flat) // len(participant_grouping_ids)).tolist() + \
            participant_grouping_ids[:len(summary_laps_df_flat)%len(participant_grouping_ids)]
    
    summary_car_status_df_flat['participant_grouping_id'] = np.tile(
        participant_grouping_ids, len(summary_car_status_df_flat) // len(participant_grouping_ids)).tolist() + \
            participant_grouping_ids[:len(summary_car_status_df_flat)%len(participant_grouping_ids)]
    
    your_telemery_small_df = summary_participants_df[['participant_grouping_id', 'yourTelemetry']]
        
    summary_laps_df_flat = pd.merge(summary_laps_df_flat, your_telemery_small_df, on='participant_grouping_id', how='left')

    summary_laps_df_flat = summary_laps_df_flat[(summary_laps_df_flat['yourTelemetry'] == 0) & (summary_laps_df_flat['sector'] == 2)]\
        .groupby(['participant_grouping_id', 'currentLapNum']).tail(1).\
        sort_values('currentLapTime', ascending=True).rename(columns={'currentLapNum': 'lap'})

    summary_laps_df_flat['sector3Time'] = summary_laps_df_flat['currentLapTime'] - (summary_laps_df_flat['sector1Time'] + summary_laps_df_flat['sector2Time'])
    summary_laps_df_flat['sessionTime_rounded'] = summary_laps_df_flat['sessionTime'].round(0)

    summary_laps_df_flat['lap_time_format'] = summary_laps_df_flat['currentLapTime'].apply(lambda x: str(
            datetime.timedelta(seconds=x)
            )[2:-3])
    
    summary_laps_df_flat['sector_1_format'] = summary_laps_df_flat['sector1Time'].apply(lambda x: str(
            datetime.timedelta(seconds=x)
            )[2:-3])
    
    summary_laps_df_flat['sector_2_format'] = summary_laps_df_flat['sector2Time'].apply(lambda x: str(
            datetime.timedelta(seconds=x)
            )[2:-3])

    summary_laps_df_flat['sector_3_format'] = summary_laps_df_flat['sector3Time'].apply(lambda x: str(
            datetime.timedelta(seconds=x)
            )[2:-3])

    summary_laps_df_flat['gap'] = summary_laps_df_flat['currentLapTime'] - summary_laps_df_flat['currentLapTime'].tolist()[0]
    summary_laps_df_flat['gap_format'] = summary_laps_df_flat['gap'].apply(lambda x: "+"+str(round(x,3))+'s' if x>0 else str(x)+'s')
    summary_laps_df_flat.loc[summary_laps_df_flat[summary_laps_df_flat['gap_format'] == '0.0s'].index, 'gap_format'] = '+/-'
    summary_laps_df_flat['currentLapInvalid'] = summary_laps_df_flat['currentLapInvalid'].replace({
        1: 'Yes',
        0: 'No'
    })

    summary_car_status_df_flat = pd.merge(summary_car_status_df_flat, your_telemery_small_df, on='participant_grouping_id', how='left')
    summary_car_status_df_flat = summary_car_status_df_flat[summary_car_status_df_flat['yourTelemetry'] == 0]
    
    full_df_joined = pd.merge(summary_laps_df_flat, summary_participants_df, on=['participant_grouping_id', 'yourTelemetry'], how='left').\
        merge(summary_car_status_df_flat, on=['participant_grouping_id', 'yourTelemetry', 'sessionTime_rounded'], how='left')
   
    full_df_joined['name_short'] = full_df_joined['name'].str.split(' ').apply(lambda x: x[1][0:3].upper())

    print(full_df_joined)

    best_lap = full_df_joined[full_df_joined['currentLapTime'] == full_df_joined['currentLapTime'].min()]
    theoretical_best_lap = full_df_joined[full_df_joined['currentLapTime'] == full_df_joined['currentLapTime'].min()][
        ['name_short', 'nationality', 'team', 'name', 'lap', 'gap_format', 'yourTelemetry']]

    theoretical_best_lap['sector1Time'] = summary_laps_df_flat['sector1Time'].min()
    theoretical_best_lap['sector2Time'] = summary_laps_df_flat['sector2Time'].min()
    theoretical_best_lap['sector3Time'] = summary_laps_df_flat['sector3Time'].min()
    theoretical_best_lap['currentLapTime'] = theoretical_best_lap['sector1Time'] + theoretical_best_lap['sector2Time'] + theoretical_best_lap['sector3Time']

    theoretical_best_lap['lap_time_format'] = str(
            datetime.timedelta(seconds=theoretical_best_lap['currentLapTime'].tolist()[0])
            )[2:-3]
    
    theoretical_best_lap['sector_1_format'] = str(
            datetime.timedelta(seconds=theoretical_best_lap['sector1Time'].tolist()[0])
            )[2:-3]
    
    theoretical_best_lap['sector_2_format'] = str(
            datetime.timedelta(seconds=theoretical_best_lap['sector2Time'].tolist()[0])
            )[2:-3]

    theoretical_best_lap['sector_3_format'] = str(
            datetime.timedelta(seconds=theoretical_best_lap['sector3Time'].tolist()[0])
            )[2:-3]

    theoretical_best_lap['currentLapInvalid'] = 'No'
    theoretical_best_lap['tires'] = 'Soft'

    dataframes_list = [full_df_joined, best_lap, theoretical_best_lap]
    achievements_list = ['Best Lap', 'Theoretical Best Lap', 'Record Lap']

    for single_df_indeks in range(0, len(dataframes_list)):
        temp_df = dataframes_list[single_df_indeks]
        temp_df = temp_df[
            ['lap', 'name_short', 'nationality', 'team', 'lap_time_format', 'sector_1_format', 
            'sector_2_format', 'sector_3_format', 'gap_format', 'currentLapInvalid', 'tires', 
            'yourTelemetry', 'name', 'currentLapTime']
            ]
        
        if single_df_indeks > 0:
            temp_df['Achievement'] = achievements_list[single_df_indeks-1]
            cols = list(temp_df)
            # move the column to head of list using index, pop and insert
            cols.insert(0, cols.pop(cols.index('Achievement')))
            temp_df = temp_df.loc[:, cols]
            temp_df['id'] = single_df_indeks
        else:
            temp_df['id'] = range(1, len(temp_df) + 1)
        
        temp_df['id'] = temp_df['id'].astype(str) + '.'
        cols = list(temp_df)
        # move the column to head of list using index, pop and insert
        cols.insert(0, cols.pop(cols.index('id')))

        temp_df = temp_df.loc[:, cols]

        temp_df = temp_df.rename(columns={
            'id': '', 
            'lap': 'Lap',
            'name_short': 'Name',
            'nationality': 'Nat.',
            'team': 'Team', 
            'lap_time_format': 'Lap Time', 
            'sector_1_format': 'Sector 1',
            'sector_2_format': 'Sector 2',
            'sector_3_format': 'Sector 3',
            'gap_format': 'Gap',
            'currentLapInvalid': 'Lap Invalid',
            'tires': 'Tires'
            })

        teams_boxes = []
        for single_team_index in range(0, len(temp_df['Team'].tolist())):
            single_team = temp_df['Team'].tolist()[single_team_index]
            full_name = temp_df['name'].tolist()[single_team_index]
            teams_boxes.append(
                '![{}](assets/images/teams/{}.svg "{}") '.format(single_team, single_team.replace(' ', '_'), full_name)
            )

        temp_df['Name'] = teams_boxes + temp_df['Name']

        nationality_flags = []
        for single_nationality in temp_df['Nat.'].tolist():
            nationality_flags.append(
                '![{}]({} "{}")'.format(
                    nat.NATIONALITIES[single_nationality]['country'],
                    nat.NATIONALITIES[single_nationality]['flag_url'],
                    nat.NATIONALITIES[single_nationality]['country']
                    )
            )
        
        temp_df['Nat.'] = nationality_flags

        stint_icons = []
        for single_participant_stint in temp_df['Tires'].tolist():   
            stint_icons.append(
                '![{}](assets/images/tires/{}.svg "{} Tires")'.format(
                    single_participant_stint, 
                    single_participant_stint, 
                    single_participant_stint
                )
            )

        temp_df['Tires'] = stint_icons

        dataframes_list[single_df_indeks] = temp_df

    if record_lap < best_lap['currentLapTime'].min():
        laps_record_replaced = bqq.summary_laps_record.replace('$_record_lap', str(record_lap))
        summary_laps_redord_df_flat = client.query(laps_record_replaced, project=project_name).to_dataframe()
        if summary_laps_redord_df_flat.shape[0] > 0:
            pass

    if dataframes_list[1]['Lap Time'].tolist() == dataframes_list[2]['Lap Time'].tolist():
        dataframes_list[2] = pd.DataFrame(columns=dataframes_list[1].columns)

    record_df = pd.concat([dataframes_list[1], dataframes_list[2]]).reset_index()
    record_df[''] = range(1, len(record_df) + 1)
    record_df[''] = record_df[''].astype(str) + '.'

    record_df['gap'] = record_df['currentLapTime'] - record_df['currentLapTime'].tolist()[0]
    record_df['Gap'] = record_df['gap'].round(3).astype(str) + 's'
    record_df.loc[record_df[record_df['Gap'] == '0.0s'].index, 'Gap'] = '+/-'

    drop_columns_record = ['yourTelemetry', 'name', 'Lap', 'currentLapTime', 'gap', 'index']
    drop_columns_laps = ['yourTelemetry', 'name', 'currentLapTime']

    final_df_splitted = (
        record_df.drop(columns=drop_columns_record),
        dataframes_list[0][dataframes_list[0]['yourTelemetry'] == 0].drop(columns=drop_columns_laps)
    )

    print('loading time: {}'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    return final_df_splitted

def laps_wrapper(pathname_clean, sessionUID, session_type, page_size, record_lap):
    pathname_clean = pathname_clean.replace('/', '')
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

    laps_data = get_laps_data(sessionUID, session_type, record_lap)
    
    your_data, participants_data = laps_data
    page_count_laps = int(round(participants_data.shape[0] / page_size, 0))
    
    participants_elements = [
        html.Div(
            html.H1(
                    '{} Lap Times'.format(session_type)
                ),
                id='subtitle-wrapper'
            ),
        dash_table.DataTable(
            id='datatable-3-paging-page-count',
            columns=[{"name": i, "id": i, 'presentation': 'markdown'} if i in ['Name', 'Nat.', 'Tires'] \
                else {"name": i, "id": i} for i in participants_data.columns],
            filter_query='',
            page_current=0,
            page_size=page_size,
            page_action='custom',
            page_count=page_count_laps if participants_data.shape[0] > page_size else -1,
            style_header={'border': '0 !important'},
            style_cell={'textAlign': 'left'}
        )
    ]

    elements_list = html.Div(
        [
        html.Div(
                    html.H1(
                        'Your {} Lap Times Summary'.format(session_type)
                    ),
                    id='subtitle-wrapper'
                ),
        dash_table.DataTable(
        id='datatable-4-paging-page-count',
        columns=[{"name": i, "id": i, 'presentation': 'markdown'} if i in ['Name', 'Nat.', 'Tires'] \
            else {"name": i, "id": i} for i in your_data.columns],
        data=your_data.to_dict('records'),
        filter_query='',
        page_current=0,
        page_size=page_size,
        page_action='custom',
        page_count=1,
        style_header={'border': '0 !important'},
        style_cell={'textAlign': 'left'}
    )
    ] + participants_elements,
    id='page-content',
    style={'height': '690px'}
    )

    final_tuple = (
        elements_list,
        participants_data
    )

    return final_tuple