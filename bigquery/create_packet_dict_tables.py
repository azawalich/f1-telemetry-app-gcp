import os 
import time
import base64
import requests
import gitlab
import re 
from google.cloud import pubsub # needed for not having segfault with bigquery...
from google.cloud import bigquery

######### 1. SETUP #########

# initialize secrets
secrets = {}
f = open('secrets.sh', 'r')
lines_read = f.read().splitlines()[1:]
f.close()

for line in lines_read:
    line_splitted = line.replace('\n', '').replace('"', '').split('=')
    secrets[line_splitted[0]] = line_splitted[1]

# setup credentials for bigquery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '{}{}'.format(secrets['local_path'], secrets['bigquery_service_account_file'])

######### 2. GITLAB DOWNLOAD #########

gl = gitlab.Gitlab('http://gitlab.com', private_token=secrets['gitlab_token'])
gl.auth()

project = gl.projects.get('azawalich/f1-2019-telemetry')
items = project.repository_tree(path='f1_2019_telemetry', ref='master')
file_info = project.repository_blob(items[-1]['id'])
content = base64.b64decode(file_info['content']).decode()

######### 3. SETUP BASIC STRUCTURES #########

entriesToRemove = [
    'PacketLapData_V1', 'PacketParticipantsData_V1', 'PacketCarStatusData_V1', 
    'PacketCarTelemetryData_V1', 'PacketCarSetupData_V1'
    ]
ccase_pattern = re.compile(r'(?<!^)(?=[A-Z])')
packet_names = []

for packet_s in re.findall(r'class\ (.+)\(Packed', content):
    if packet_s not in entriesToRemove:
        packet_to_lower = packet_s.replace('Packet', '').replace('_V1', '').replace('Data', '')
        packet_to_lower = ccase_pattern.sub('_', packet_to_lower).lower()
        packet_names.append(packet_to_lower)
    else:
        packet_names.append(packet_s)
        
packets = dict.fromkeys(packet_names)

for single_packet in list(packets.keys()):
    packets[single_packet] = {
        'start': None,
        'end': None,
        'content': None,
        'definitions': {
            'value': [],
            'description': []
        },
        'rows': []
    }

######### 4. DETERMINE ENDPOINTS IN CLASSES  #########

indeks = 0
line_number = 0

splitted_content = content.split('\n')
for line in splitted_content:
    if line == '    _fields_ = [':
        packets[list(packets.keys())[indeks]]['start'] = line_number
    if line == '    ]':
        packets[list(packets.keys())[indeks]]['end'] = line_number
        indeks += 1 
    line_number += 1

######### 5. SPLIT TEXT INTO DESCRIPTIONS  #########

for single_packet in list(packets.keys()):
    fields = splitted_content[packets[single_packet]['start']:packets[single_packet]['end']]
    packets[single_packet]['content'] = fields
    for single_line in fields:
        # attribute names
        field_t = re.findall(r"'(.+)' ", single_line)
        # comments
        desc_t = re.findall(r"\# (.+)", single_line)
        if len(field_t) > 0:
            packets[single_packet]['definitions']['value'].append(' '.join(field_t))
            packets[single_packet]['definitions']['description'].append(' '.join(desc_t))
        else:
            # in case comment is multiline, append last element
            if len(packets[single_packet]['definitions']['description']) > 0:
                packets[single_packet]['definitions']['description'][-1] = '{} {}'.format(
                     packets[single_packet]['definitions']['description'][-1],
                     ' '.join(desc_t)
                 )

######### 6. CLEANUP AND PREPARE FOR BIGQUERY  #########

for k in entriesToRemove:
    packets.pop(k, None)

for key in packets.keys():
    print('{} {}'.format(len(packets[key]['definitions']['value']), len(packets[key]['definitions']['description'])))
    for single_indeks in range(0, len(packets[key]['definitions']['value'])):
        packets[key]['rows'].append(
            (
                packets[key]['definitions']['value'][single_indeks], 
                packets[key]['definitions']['description'][single_indeks]
            )
        )

######### 7. INSERT INTO BIGQUERY  #########

client = bigquery.Client()

dataset_name = 'definitions'

tables = []

for single_table in client.list_tables(dataset_name):
    tables.append(single_table.table_id)

for single_structure in list(packets.keys()):
    schema = [
        bigquery.SchemaField("value", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("description", "STRING", mode="REQUIRED"),
    ]
    
    if single_structure not in tables:
        table_id = "{}.{}.{}".format(secrets['project_name'], dataset_name, single_structure)
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

        insert_rows = client.insert_rows(table, packets[single_structure]['rows'])  # Make an API request.
        print(insert_rows)
        if insert_rows == []:
            print("{} rows have been added to: {}".format(len(packets[single_structure]['rows']), table.table_id))
        time.sleep(10)
    else:
        "Table {} already exists, not adding rows.".format(single_structure)
