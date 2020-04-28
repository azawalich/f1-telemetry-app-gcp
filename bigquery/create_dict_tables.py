import os 
from google.cloud import pubsub # needed for not having segfault with bigquery...
from google.cloud import bigquery
from f1_2019_telemetry.packets import TeamIDs, DriverIDs, TrackIDs, NationalityIDs, SurfaceTypes,\
    ButtonFlag, PacketID, PacketID, EventStringCode, EventStringCode
import time

# initialize secrets

secrets = {}
f = open('secrets.sh', 'r')
lines_read = f.read().splitlines()[1:]
f.close()

for line in lines_read:
    line_splitted = line.replace('\n', '').replace('"', '').split('=')
    secrets[line_splitted[0]] = line_splitted[1]

# setup credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '{}{}'.format(secrets['local_path'], secrets['bigquery_service_account_file'])

client = bigquery.Client()

# python type of dataset initialize due to lack of location parameter in gcloud CLI (gcloud alpha bq) 
dataset_name = 'definitions'
dataset = bigquery.Dataset(client.dataset(dataset_name))
dataset.location = 'EU'
dataset = client.create_dataset(dataset)

# create and fillup basic tables

basic_dict = {
    'team': TeamIDs,
    'driver': DriverIDs,
    'track': TrackIDs,
    'nationality': NationalityIDs,
    'surface': SurfaceTypes,
}

tables = []

for single_table in client.list_tables(dataset):
    tables.append(single_table.table_id)

for single_structure in list(basic_dict.keys()):
    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("value", "STRING", mode="REQUIRED"),
    ]
    
    if single_structure not in tables:
        table_id = "{}.{}.{}".format(secrets['project_name'], dataset_name, single_structure)
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
        rows_to_insert = [(k, v) for k, v in basic_dict[single_structure].items()] 
        print(rows_to_insert)

        insert_rows = client.insert_rows(table, rows_to_insert)  # Make an API request.
        print(insert_rows)
        if insert_rows == []:
            print("{} rows have been added to: {}".format(len(rows_to_insert), table.table_id))
        time.sleep(10)
    else:
        "Table {} already exists, not adding rows.".format(single_structure)

# create and fillup more complicated tables

rest_list = {
    'button_flag': ButtonFlag.description,
    'packet': {
        'short': PacketID.short_description,
        'long': PacketID.long_description
    },
    'event_code': {
        'short': EventStringCode.short_description,
        'long': EventStringCode.long_description
    }
}

tuples_to_insert = dict.fromkeys(rest_list.keys())

for single_rest in list(rest_list.keys()):
    rows_to_insert = []
    if len(rest_list[single_rest]) == 2:
        if single_rest == 'event_code':
            for single_key_ind in range(0, len(list(rest_list[single_rest]['short']))):
                final_tuple = (
                    list(rest_list[single_rest]['short'].keys())[single_key_ind].value.decode(),
                    list(rest_list[single_rest]['short'].values())[single_key_ind],
                    list(rest_list[single_rest]['long'].values())[single_key_ind])
                rows_to_insert.append(final_tuple)
        else:
            for single_key_ind in range(0, len(list(rest_list[single_rest]['short']))):
                final_tuple = (
                    list(rest_list[single_rest]['short'].keys())[single_key_ind].value,
                    str(list(rest_list[single_rest]['short'].keys())[single_key_ind]).split('.')[1],
                    list(rest_list[single_rest]['long'].values())[single_key_ind])
                rows_to_insert.append(final_tuple)
    else:
        for single_key_ind in range(0, len(list(rest_list[single_rest]))):
            final_tuple = (
                list(rest_list[single_rest].keys())[single_key_ind].value,
                str(list(rest_list[single_rest].keys())[single_key_ind]).split('.')[1],
                list(rest_list[single_rest].values())[single_key_ind])
            rows_to_insert.append(final_tuple)
    tuples_to_insert[single_rest] = rows_to_insert

for single_rest in list(tuples_to_insert.keys()):   
    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("value", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("description", "STRING", mode="REQUIRED"),
        ]

    if single_rest == 'event_code':
        schema[0] = bigquery.SchemaField("id", "STRING", mode="REQUIRED")
    
    if single_rest not in tables:
        table_id = "{}.{}.{}".format(secrets['project_name'], 'definitions', single_rest)
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        insert_rows = client.insert_rows(table, tuples_to_insert[single_rest])
        if insert_rows == []:
            print("{} rows have been added to: {}".format(len(tuples_to_insert[single_rest]), table.table_id))
        time.sleep(10)
    else:
        print("Table {} already exists, not adding rows.".format(single_rest))