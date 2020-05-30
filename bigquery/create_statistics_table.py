import sys, os
import pandas as pd
from google.cloud import bigquery

sys.path.append(os.path.abspath(os.path.join('sql')))

import sql.q_homepage as bqq

# initialize secrets

secrets = {}
f = open('../secrets.sh', 'r')
lines_read = f.read().splitlines()[1:]
f.close()

for line in lines_read:
    line_splitted = line.replace('\n', '').replace('"', '').split('=')
    secrets[line_splitted[0]] = line_splitted[1]

# setup credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '{}{}'.format(secrets['local_path'], secrets['bigquery_service_account_file'])

project_name = 'f1-telemetry-app'
client = bigquery.Client()

session_count_df = client.query(bqq.distinct_sessions, project=project_name).to_dataframe()

sessions = session_count_df['sessionUID']
del(session_count_df)

rows_to_add = []

for single_session in sessions:
    
    # big query data reading
    row_count_df = client.query(bqq.row_count_tables, project=project_name).to_dataframe()
    session_count_df = client.query(bqq.distinct_sessions, project=project_name).to_dataframe()
    row_count_sessions_df = client.query(bqq.row_count_tables_sessions, project=project_name).to_dataframe()
    time_driven_df = client.query(bqq.time_driven_sessions, project=project_name).to_dataframe()
    event_sessions_df = client.query(bqq.event_sessions, project=project_name).to_dataframe()
    distance_sessions_df = client.query(bqq.distance_sessions, project=project_name).to_dataframe()
    
    tiles_data = dict.fromkeys(
    ['last_sessionUID', 'last_session_publish_time', 'last_session_type', 'total_time_driven', 
    'total_distance_driven', 'total_datapoint_count', 'last_session_time_driven', 
    'last_session_distance_driven', 'last_session_datapoint_count', 'total_session_count', 
    'time_trial_count', 'event_count', 'event_win_count', 'event_fastest_laps_count']
    )

    # reduce data for the first session ever
    if single_session == 8109909506422837145:
        row_count_sessions_df = row_count_sessions_df[row_count_sessions_df['sessionUID'] == single_session]
        session_count_df = session_count_df[session_count_df['sessionUID'] == single_session]
        time_driven_df = time_driven_df[time_driven_df['sessionUID'] == single_session]
        event_sessions_df = event_sessions_df[event_sessions_df['sessionUID'] == single_session]
        distance_sessions_df = distance_sessions_df[distance_sessions_df['sessionUID'] == single_session]

    # creating stats
    tiles_data['last_sessionUID'] = single_session

    #### total_datapoint_count   
    tiles_data['total_datapoint_count'] = int(
        row_count_sessions_df['row_count'].sum()
        )

    #### total_session_count
    tiles_data['total_session_count'] = session_count_df['sessionUID'].shape[0]

    #### last_session_datapoint_count
    tiles_data['last_session_datapoint_count'] = int(
        row_count_sessions_df[row_count_sessions_df['sessionUID'] == single_session]['row_count'].sum()
        )

    #### time_driven
    session_start_times = time_driven_df.groupby(['sessionUID'])['min_publish_time'].min()
    session_end_times = time_driven_df.groupby(['sessionUID'])['max_publish_time'].max()

    time_driven_df_joined = pd.concat([session_end_times, session_start_times], axis=1).reset_index()
    time_driven_df_joined['session_length'] = time_driven_df_joined['max_publish_time'] - time_driven_df_joined['min_publish_time']

    #### last_session_time_driven
    last_hours = time_driven_df_joined[time_driven_df_joined['sessionUID'] == single_session]['session_length'].sum().seconds // 3600
    last_minutes = (time_driven_df_joined[time_driven_df_joined['sessionUID'] == single_session]['session_length'].sum().seconds // 60) % 60
    last_seconds = time_driven_df_joined[time_driven_df_joined['sessionUID'] == single_session]['session_length'].sum().seconds % 60

    tiles_data['last_session_time_driven'] = '{}h {}m {}s'.format(last_hours, last_minutes, last_seconds)

    # last_session_publish_time
    tiles_data['last_session_publish_time'] = str(
        time_driven_df_joined[time_driven_df_joined['sessionUID'] == single_session]['min_publish_time'].tolist()[0]
    )

    #### total_time_driven  
    hours = time_driven_df_joined['session_length'].sum().seconds // 3600
    minutes = (time_driven_df_joined['session_length'].sum().seconds // 60) % 60
    seconds = time_driven_df_joined['session_length'].sum().seconds % 60

    tiles_data['total_time_driven'] = '{}h {}m {}s'.format(hours, minutes, seconds)
    
    #### event_count    
    tiles_data['event_count'] = len(event_sessions_df['sessionUID'].drop_duplicates().tolist())

    #### time_trial_count
    tiles_data['time_trial_count'] = time_driven_df_joined.shape[0] - tiles_data['event_count']

    #### last_session_type
    if single_session in event_sessions_df['sessionUID'].tolist():
        tiles_data['last_session_type'] = 'event'
    else:
        tiles_data['last_session_type'] = 'time_trial'

    #### event_fastest_laps_count
    event_player_events = event_sessions_df[event_sessions_df['playerCarIndex'] == event_sessions_df['vehicleIdx']]

    tiles_data['event_fastest_laps_count'] = event_player_events[event_player_events['value'] == 'Fastest Lap'].shape[0]

    #### event_win_count
    tiles_data['event_win_count'] = event_player_events[event_player_events['value'] == 'Race Winner'].shape[0]

    #### last_session_distance_driven
    tiles_data['last_session_distance_driven'] = '{}m'.format(
        int(
            round(distance_sessions_df[distance_sessions_df['sessionUID'] == single_session]['totalDistance'].sum(), 0)
            )
        )

    #### total_distance_driven
    tiles_data['total_distance_driven'] = '{}m'.format(
        int(
            round(distance_sessions_df['totalDistance'].sum(), 0)
            )
        )

    rows_to_add.append(tiles_data)

dataset_name = 'packets_data'

tables = []

for single_table in client.list_tables(dataset_name):
    tables.append(single_table.table_id)

schema_types = {
    'int': 'INT64',
    'str': 'STRING'
}

schema = []

for single_structure in list(rows_to_add[0].keys()):
    schema.append(
        bigquery.SchemaField(single_structure, schema_types[type(rows_to_add[0][single_structure]).__name__], mode="REQUIRED")
    )

table_name = 'statistics'

if table_name not in tables:
    table_id = "{}.{}.{}".format(secrets['project_name'], dataset_name, table_name)
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

    insert_rows = client.insert_rows(table, rows_to_add)  # Make an API request.
    print(insert_rows)
    if insert_rows == []:
        print("{} rows have been added to: {}".format(len(rows_to_add), table.table_id))
else:
    "Table {} already exists, not adding rows.".format(single_structure)