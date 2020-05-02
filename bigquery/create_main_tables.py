import os 
import time
import json
from google.cloud import pubsub # needed for not having segfault with bigquery...
from google.cloud import bigquery
from google.cloud import storage

######### 1. SETUP #########

# initialize secrets
secrets = {}
f = open('secrets.sh', 'r')
lines_read = f.read().splitlines()[1:]
f.close()

for line in lines_read:
    line_splitted = line.replace('\n', '').replace('"', '').split('=')
    secrets[line_splitted[0]] = line_splitted[1]

# setup credentials for gcs
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '{}{}'.format(secrets['local_path'], secrets['computeengine_service_account_file'])

######### 2. GET PACKETS FROM GCS #########

# test data setup
storage_client = storage.Client()
bucket = storage_client.get_bucket(secrets['bucket_name'])

test_files = [
    "8109909506422837145_0_0.json", "8109909506422837145_0_1.json", "8109909506422837145_0_2.json",
    "8109909506422837145_0_3.json", "8109909506422837145_0_4.json", "8109909506422837145_0_5.json", 
    "8109909506422837145_0_6.json", "8109909506422837145_0_7.json"]

read_packets = []

for single_file in test_files:
    blob = bucket.blob('clean/2020-04-22_17:25:29/{}'.format(single_file))
    read_packets.append(json.loads(blob.download_as_string()))
    time.sleep(1)

######### 3. CREATE BIGQUERY TABLE SCHEMAS FOR PACKETS #########

# define supplementary code

def fill_repeated_fields_dict(name_d, type_d):
    return bigquery.SchemaField(
        name_d,
        type_d,
        mode="NULLABLE")

def check_packet_types(chosen_packet_f, single_key_f):
    # in case of nested-type attribures
    if isinstance(chosen_packet_f[single_key_f], list):
        # if normal list, previously pre-defined, then it is about wheel data; 
        # we want to add wheelID to each column
        if single_key_f in wheel_fields:
            temp_fields_list = []
            for single_wheel in range(0, len(chosen_packet_f[single_key_f])):
                temp_fields_list.append(
                    fill_repeated_fields_dict(
                        wheel_names[single_wheel],
                        schema_types[type(chosen_packet_f[single_key_f][single_wheel]).__name__]
                        )
                    )
            temp_schema = bigquery.SchemaField(
                single_key_f,
                'RECORD',
                mode='REPEATED',
                fields=temp_fields_list
                )
        # if not normal list, then it is packet list-alike structure (list of dicts);
        # do function for it once again recursively
        else:
            temp_fields_list = []
            #for single_list_element in range(0, len(chosen_packet_f[single_key_f])):
            for single_list_element in range(0, 1):
                for single_inner_key_dpl in list(chosen_packet_f[single_key_f][single_list_element].keys()):
                    temp_fields_list.append(
                        check_packet_types(
                            chosen_packet_f[single_key_f][single_list_element],
                            single_inner_key_dpl
                                )
                            )
            temp_schema = bigquery.SchemaField(
                single_key_f,
                'RECORD',
                mode='REPEATED',
                fields=temp_fields_list
                )
    # rarely we have simple dicts as attributes 
    elif isinstance(chosen_packet_f[single_key_f], dict):
        temp_fields_list = []
        for single_inner_key_dpl in list(chosen_packet_f[single_key_f].keys()):
            temp_fields_list.append(
                    fill_repeated_fields_dict(
                        single_inner_key_dpl,
                        schema_types[type(chosen_packet_f[single_key_f][single_inner_key_dpl]).__name__]
                    )
            )
        temp_schema = bigquery.SchemaField(
            single_key_f,
            'RECORD',
            mode='REPEATED',
            fields=temp_fields_list
            )
    # in case of non-nested attributes 
    else:
        if single_key_f == 'publish_time':
            bigquery_type = 'DATETIME'
        else:
            bigquery_type = schema_types[type(chosen_packet_f[single_key_f]).__name__]
        temp_schema = fill_repeated_fields_dict(
            single_key_f,
            bigquery_type
            )
    return temp_schema

schema_types = {
    'int': 'INT64',
    'str': 'STRING',
    'float': 'FLOAT64'
}

wheel_fields = [
    'suspensionPosition', 'suspensionVelocity', 'suspensionAcceleration', 'wheelSpeed', 
    'wheelSlip', 'brakesTemperature', 'tyresSurfaceTemperature', 'tyresInnerTemperature', 
    'tyresPressure', 'surfaceType', 'tyresWear', 'tyresDamage']

wheel_names = ['RL', 'RR', 'FL', 'FR']

schema_list = []

packet_types = [
    'motion',
    'session',
    'lap',
    'event',
    'participant',
    'car_setup',
    'car_telemetry',
    'car_status'
]


# setup credentials for bigquery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '{}{}'.format(secrets['local_path'], secrets['bigquery_service_account_file'])

# create schemas for each packet
for chosen_packet in read_packets:
    temp_list = []
    for single_key in chosen_packet.keys():
        try:
            temp_list.append(check_packet_types(chosen_packet, single_key))
        except:
            print('Fuckup, packet {}, key {}'.format(read_packets.index(chosen_packet), single_key))
    schema_list.append(temp_list)
    print('Done, packet {}'.format(read_packets.index(chosen_packet)))

client = bigquery.Client()

# python type of dataset initialize due to lack of location parameter in gcloud CLI (gcloud alpha bq) 
dataset_name = 'packets_data'
dataset = bigquery.Dataset(client.dataset(dataset_name))
dataset.location = 'EU'
dataset = client.create_dataset(dataset)

tables = []

for single_table in client.list_tables(dataset):
    tables.append(single_table.table_id)

for single_schema in schema_list:
    indeks = schema_list.index(single_schema)
    table_name = packet_types[indeks]

    if table_name not in tables:
        table_id = "{}.{}.{}".format(secrets['project_name'], dataset_name, table_name)
        table = bigquery.Table(table_id, schema=single_schema)
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
        time.sleep(10)
    else:
        "Table {} already exists, not adding it.".format(table_name)