import argparse
import datetime
import simplejson as json
import logging
import jsonpickle
from ast import literal_eval as make_tuple
import glob
import os
import requests
import time
import datetime

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions

class ConvertPacketsToJSON(beam.PTransform):

    def check_upacked_udp_packet_types(self, unpacked_udp_packet):
        """Check types in attributes of unpacked telemetry packet (appropriately-typed one) 
        and return basic dict-structure for it.

        Args:
            unpacked_udp_packet: the contents of the unpacked UDP packet.

        Returns:
            dict in a basic form to be later-on added to full packet JSON representation.
        """

        if hasattr(unpacked_udp_packet, '_fields_'):
            temp_dict = {}
            for single_attribute in [x[0] for x in getattr(unpacked_udp_packet, '_fields_')]:
                if isinstance(getattr(unpacked_udp_packet, single_attribute), int) or \
                    isinstance(getattr(unpacked_udp_packet, single_attribute), float):
                    temp_dict[single_attribute] = getattr(unpacked_udp_packet, single_attribute)
                elif isinstance(getattr(unpacked_udp_packet, single_attribute), bytes):
                    temp_dict[single_attribute] = getattr(unpacked_udp_packet, single_attribute).decode('utf-8')
                else:
                    list_dict = []
                    for list_element in range(0, len(getattr(unpacked_udp_packet, single_attribute))):
                        list_dict.append(getattr(unpacked_udp_packet, single_attribute)[list_element])
                    temp_dict[single_attribute] = list_dict              
        else:
            if hasattr(unpacked_udp_packet, 'isascii'):
                temp_dict = unpacked_udp_packet.decode('utf-8')
            else:
                temp_dict = unpacked_udp_packet
        return temp_dict

    def convert_upacked_udp_packet_to_json(self, unpacked_udp_packet, publish_time):
        """Convert unpacked telemetry packet (appropriately-typed one) to its' JSON representation.

        Args:
            unpacked_udp_packet: the contents of the unpacked UDP packet.

        Returns:
            JSON representation of the unpacked UDP packet.
        """
        full_dict = {}
        for single_field in [x[0] for x in unpacked_udp_packet._fields_]:
            if hasattr(getattr(unpacked_udp_packet, single_field), '__len__') and \
                hasattr(getattr(unpacked_udp_packet, single_field), 'isascii') == False:
                temp_dict = []
                for single_list_element in range(0, len(getattr(unpacked_udp_packet, single_field))): 
                    temp_dict.append(self.check_upacked_udp_packet_types(
                        unpacked_udp_packet = getattr(unpacked_udp_packet, single_field)[single_list_element]))
                full_dict[single_field] = temp_dict
            else:
                full_dict[single_field] = self.check_upacked_udp_packet_types(
                    unpacked_udp_packet = getattr(unpacked_udp_packet, single_field))
        full_dict['publish_time'] = publish_time
        return json.dumps(full_dict, ensure_ascii=False).encode('utf8').decode()

    def cleanup_packets(self, single_file):
        json_packets = []
        temp_packet = json.loads(single_file)
        decoded_packet = jsonpickle.decode(temp_packet['packet_encoded'])
        json_packet = self.convert_upacked_udp_packet_to_json(
            unpacked_udp_packet = decoded_packet, 
            publish_time = temp_packet['publish_time']
            )
        json_packets.append(json_packet)
        return json_packets
    
    def expand(self, pcoll):
        logging.info(
            '{} starting ConvertPacketsToJSON'.format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        )
        return (
            pcoll
            | beam.Map(lambda x: self.cleanup_packets(single_file = x))
        )

class ConvertSchemasToBeJoinedByKey(beam.PTransform):
    def convertSchemasFromGCS(self, single_file):
        single_file_decoded = json.loads(single_file)
        return single_file_decoded

    def expand(self, pcoll):       
        logging.info(
            '{} starting ConvertSchemasToBeJoinedByKey'.format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        )
        return (
            pcoll
            | beam.Map(lambda x: self.convertSchemasFromGCS(single_file = x))
            | "Add packetId Key2" >> beam.Map(
                lambda elem: (
                    elem['packet_id'],
                    json.dumps({i:elem[i] for i in elem if i!= 'packet_id'})
                        )
                    )
        )

class ConvertToBigQueryJSON(beam.PTransform):
    wheel_fields = [
        'suspensionPosition', 'suspensionVelocity', 'suspensionAcceleration', 'wheelSpeed', 
        'wheelSlip', 'brakesTemperature', 'tyresSurfaceTemperature', 'tyresInnerTemperature', 
        'tyresPressure', 'surfaceType', 'tyresWear', 'tyresDamage']
    
    emptiable_fields = ['carSetups', 'carStatusData', 'carTelemetryData', 'lapData', 
        'carMotionData', 'participants', 'marshalZones']

    def convert_lists_to_wheel_dicts(self, json_element):
        if isinstance(json_element, list) and len(json_element) == 4:
            wheel_names = ['RL', 'RR', 'FL', 'FR']
            wheel_dict = dict.fromkeys(wheel_names)
            for single_wheel in range(0,4):
                wheel_dict[wheel_names[single_wheel]] = json_element[single_wheel]
        
        return wheel_dict
    
    def clean_emptiable_fields(self, packet_json):
        # some fields are dummy filled in with 0 or '', we can delete these to reduce the number of rows
        for single_element in list(packet_json.keys()):
            if single_element in self.emptiable_fields:
                emptiable_indexes = []
                for indeks in range(0, len(packet_json[single_element])):
                    single_list_element = packet_json[single_element][indeks]
                    # it turns out that nested lists need to be checked additionally for emptiability
                    # and reduced to a non-list type which map supports
                    single_list_element_values = list(single_list_element.values())
                    single_list_element_values_types = [type(item).__name__ for item in single_list_element_values]
                    for single_value in range(0, len(single_list_element_values_types)):
                        if single_list_element_values_types[single_value] == 'list':
                            mapped_boolean_list = list(map(bool, list(set(single_list_element_values[single_value]))))
                            if True in mapped_boolean_list:
                                single_list_element_values[single_value] = True
                            else: 
                                single_list_element_values[single_value] = False
                    # map all dict values to bool, 0 and '' give False, everything else gives True (also ' ' !)
                    unique_values = list(map(bool, list(set(single_list_element_values))))
                    # if only unnecessary in list, safely delete it 
                    if True not in unique_values:
                        emptiable_indexes.append(indeks)        
                for indeks in sorted(emptiable_indexes, reverse=True):
                    del packet_json[single_element][indeks]
        return packet_json

    def convert_json_packet_to_bigquery_compliant(self, packet_json):
        for single_element in list(packet_json.keys()):
            if single_element in self.wheel_fields: # normal lists
                packet_json[single_element] = \
                    [self.convert_lists_to_wheel_dicts(json_element = packet_json[single_element])]
            if single_element in ['carTelemetryData', 'carStatusData']: # nested lists
                for i in range(0, len(packet_json[single_element])):
                    for single_keyy in list(packet_json[single_element][i].keys()):
                        if single_keyy in self.wheel_fields: 
                            packet_json[single_element][i][single_keyy] = \
                                [
                                    self.convert_lists_to_wheel_dicts(
                                        json_element = packet_json[single_element][i][single_keyy]
                                        )
                                ]
        return packet_json
    
    def convert_bigquery(self, element):
        decoded_json = json.loads(element[0])
        clean_json = self.clean_emptiable_fields(packet_json = decoded_json)
        bigquery_packet = self.convert_json_packet_to_bigquery_compliant(packet_json = clean_json)
        encoded_bigquery_packet = json.dumps(bigquery_packet, ignore_nan=True)
        return encoded_bigquery_packet

    def expand(self, pcoll):
        logging.info(
            '{} starting ConvertToBigQueryJSON'.format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
            )
        return (
            pcoll
            | beam.Map(lambda x: self.convert_bigquery(element = x))
        )

class AddPacketIdKey(beam.DoFn):
    def process(self, element):
        yield {
            "packet_id": json.loads(element)['header']['packetId'],
            "session_id": json.loads(element)['header']['sessionUID'],
            "packet_json": element
        }

class GroupFilesIntoBatches(beam.PTransform):

    def expand(self, pcoll):
        logging.info(
            '{} starting GroupFilesIntoBatches'.format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        )
        return (
            pcoll
            | "Add packetIds to grouping" >> beam.ParDo(AddPacketIdKey())
            | "Add packetId Key" >> beam.Map(
                lambda elem: (
                    '{}_{}'.format(
                        elem['packet_id'],
                        elem['session_id']
                        ),
                        elem['packet_json']
                        )
                    )
            | "Groupby" >> beam.GroupByKey()
        )

class GroupOutputFilesIntoBatches(beam.PTransform):

    def expand(self, pcoll):
        logging.info(
            '{} starting GroupOutputFilesIntoBatches'.format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"
                )
            )
        )
        return (
            pcoll
            | "Add packetId Key" >> beam.Map(
                lambda elem: (
                        elem['packet_id'],
                        json.dumps({i:elem[i] for i in elem if i!= 'packet_id'})
                        )
                    )
        )

class WriteFilesToGCS(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, batch):
        logging.info(
            '{} starting WriteFilesToGCS'.format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        )
        """Write one batch per file to a Google Cloud Storage bucket. """
        grouping_key, packets_list = batch
        packet_id, session_id = grouping_key.split('_')
        filename = '{}{}_batch_{}.json'.format(
            self.output_path, 
            session_id,
            packet_id
            )
        file_contents = '\n'.join(packets_list)
        
        with beam.io.gcp.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            f.write(file_contents.encode())

        # return batches for further work
        yield {
            'packet_id': int(packet_id),
            'rows_to_insert': file_contents
        }

def bigquery_convert_to_schemas(schema_json):
    schema_converted = beam.io.gcp.bigquery.bigquery.TableSchema()
    for single_field in schema_json['schema']:
        field_schema = beam.io.gcp.bigquery.bigquery.TableFieldSchema()
        if 'fields' in list(single_field.keys()):
            field_schema.name = single_field['name']
            field_schema.type = single_field['type']
            field_schema.mode = single_field['mode']
            single_field_nested = beam.io.gcp.bigquery.bigquery.TableFieldSchema()
            for single_key in list(single_field['fields']):
                single_field_nested.name = single_key['name']
                single_field_nested.type = single_key['type']
                single_field_nested.mode = single_key['mode']
                field_schema.fields.append(single_field_nested)
        else:
            field_schema.name = single_field['name']
            field_schema.type = single_field['type']
            field_schema.mode = single_field['mode']
        schema_converted.fields.append(field_schema)
    return schema_converted

def bigquery_convert_to_rows(packet_json):
    single_row = {}
    packet_json['insert_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for single_key in packet_json.keys():
        if isinstance(packet_json[single_key], dict):
            single_row[single_key] = [packet_json[single_key]]
        elif isinstance(packet_json[single_key], list):
            if len(packet_json[single_key]) > 1:
                temp_list = []
                for single_list_element in range(0, len(packet_json[single_key])):
                    temp_list.append(packet_json[single_key][single_list_element])
                single_row[single_key] = temp_list
            else:
                single_row[single_key] = [packet_json[single_key][0]]
        else:
            single_row[single_key] = packet_json[single_key]
    return single_row

def reduce_data(rows_to_add, packet_id, database, session_type):
    tables_columns = {
        0: 'carMotionData',
        2: 'lapData',
        4: 'participants',
        5: 'carSetups',
        6: 'carTelemetryData',
        7: 'carStatusData',
    }

    # reducing dummy information for any database
    # if table different than session, event AND session is time trial
    if packet_id not in [1, 3] and session_type == 12:
        # in each row, take only first participant from particular column
        for single_row_indeks in range(0, len(rows_to_add)):
            rows_to_add[single_row_indeks][tables_columns[packet_id]] = \
                [rows_to_add[single_row_indeks][tables_columns[packet_id]][0]]

    # trimming data to absolute minimum for dashboard_data database
    if database == 'dashboard_data':
        # motion, car setup, car_telemetry, car status table
        if packet_id in [0, 5, 6, 7]:
            # reducing amount of data: first row from for each second (unique rounded sessionTime)
            session_times_rounded = {
                'second': [],
                'second_indeks': []
            }
            for single_row_indeks in range(0, len(rows_to_add)):
                temp_row = rows_to_add[single_row_indeks]
                if single_row_indeks == 0:
                    session_times_rounded['second'].append(single_row_indeks)
                    session_times_rounded['second_indeks'].append(single_row_indeks)
                else:
                    second_from_row = int(
                        round(temp_row['header'][0]['sessionTime'], 0)
                        )
                    if second_from_row not in session_times_rounded['second']:
                        session_times_rounded['second'].append(second_from_row)
                        session_times_rounded['second_indeks'].append(single_row_indeks)

            rows_to_add = [rows_to_add[i] for i in session_times_rounded['second_indeks']] 
        # session table
        elif packet_id == 1:
            indekses_list = []
            # any row with safetyCarStatus > 0 and any marshalZones['zoneFlag'] > 0
            for single_row_indeks in range(0, len(rows_to_add)):
                temp_row = rows_to_add[single_row_indeks]
                marshal_zones_flag = [d['zoneFlag'] for d in temp_row['marshalZones']]
                if temp_row['safetyCarStatus'] != 0 or \
                    len(set(marshal_zones_flag)) > 1:
                    indekses_list.append(single_row_indeks)
            # last row
            indekses_list.append(len(rows_to_add)-1)
            # join all together
            rows_to_add = [rows_to_add[i] for i in list(set(indekses_list))] 
        # lap table
        elif packet_id == 2:
            participant_dict = {k: [] for k in range(0, len(rows_to_add[0]['lapData']))}
            # for all players, last row from each CurrentLapNum
            for single_row_indeks in range(0, len(rows_to_add)):
                #split into participant groups
                temp_row = rows_to_add[single_row_indeks]
                for single_participant_indeks in range(0, len(temp_row['lapData'])):
                    participant_dict[single_participant_indeks].append(
                        temp_row['lapData'][single_participant_indeks]['currentLapNum']
                        )

            all_indexes = []
            # determine last laps for each participant
            for single_participant in participant_dict.keys():
                temp_participant = participant_dict[single_participant]
                last_lap_indexes = [i for i, x in enumerate(temp_participant) \
                    if i == len(temp_participant) - 1 or x != temp_participant[i + 1]]
                all_indexes.append(last_lap_indexes)

            # join all together
            all_indexes_flat = [item for sublist in all_indexes for item in sublist]
            all_indexes_flat = sorted(set(all_indexes_flat))

            rows_to_add = [rows_to_add[i] for i in all_indexes_flat] 
        # event table
        elif packet_id == 3:
            # no changes
            pass
        # participant table
        elif packet_id == 4:
            # last row
            rows_to_add = [rows_to_add[-1]]

    return rows_to_add

def create_stiatistics_row(statistics_dict, packet_id, rows_to_add_reduced):
    last_row = rows_to_add_reduced[-1]
    if packet_id == 1: # session table
        statistics_dict['sessionUID'] = last_row['header'][0]['sessionUID']
        statistics_dict['sessionType'] = last_row['sessionType']
        statistics_dict['sessionTime'] = int(
            round(last_row['header'][0]['sessionTime'], 0)
            )
        statistics_dict['sessionTime_format'] = time.strftime(
            '%Hh %Mm %Ss', 
            time.gmtime(
                int(
                    round(last_row['header'][0]['sessionTime'], 0)
                    )
                )
            )
        statistics_dict['publish_time'] = (
            datetime.datetime.strptime(last_row['publish_time'],"%Y-%m-%d %H:%M:%S") - \
            datetime.timedelta(seconds=statistics_dict['sessionTime'])
            ).strftime("%Y-%m-%d %H:%M:%S")
        statistics_dict['track_id'] = last_row['trackId']
    elif packet_id == 2: # lap table
        player_indeks = last_row['header'][0]['playerCarIndex']
        statistics_dict['distance_driven'] = int(round(last_row['lapData'][player_indeks]['totalDistance']))
        statistics_dict['distance_driven_format'] = '{}km'.format(statistics_dict['distance_driven'] / 1000)
        statistics_dict['lap_count'] = last_row['lapData'][player_indeks]['currentLapNum']
        
        # to get fastest lap we need to iterate through all lap rows
        lap_times = []
        for single_row_indeks in range(0, len(rows_to_add_reduced)):
            single_row = rows_to_add_reduced[single_row_indeks]
            lap_times.append(single_row['lapData'][player_indeks]['lastLapTime'])
        
        statistics_dict['fastest_lap'] = min(i for i in lap_times if i > 0) 
        statistics_dict['fastest_lap_format'] = str(
            datetime.timedelta(seconds=statistics_dict['fastest_lap'])
            )[2:-3]

        # fix for low fastest lap data refresh 
        if statistics_dict['fastest_lap'] < last_row['lapData'][player_indeks]['bestLapTime']:
            record_lap = statistics_dict['fastest_lap']
        else:
            record_lap = last_row['lapData'][player_indeks]['bestLapTime']

        statistics_dict['record_lap'] = record_lap
        statistics_dict['record_lap_format'] = str(
            datetime.timedelta(seconds=statistics_dict['record_lap'])
            )[2:-3]
    elif packet_id == 3: # event table
        player_indeks = last_row['header'][0]['playerCarIndex']

        # to get fastest lap we need to iterate through all event rows
        event_codes = {
            'race_winner': [],
            'fastest_lap': []
        }

        for single_row_indeks in range(0, len(rows_to_add_reduced)):
            single_row = rows_to_add_reduced[single_row_indeks]
            if single_row['vehicleIdx'] == single_row['header'][0]['playerCarIndex']:
                if single_row['eventStringCode'] == 'RCWN':
                    event_codes['race_winner'].append(1)
                elif single_row['eventStringCode'] == 'FTLP':
                    event_codes['fastest_lap'].append(1)
            
        statistics_dict['event_win'] = len(event_codes['race_winner'])
        statistics_dict['event_fastest_lap'] = len(event_codes['fastest_lap'])
    elif packet_id == 4: # participant table
        player_indeks = last_row['header'][0]['playerCarIndex']
        statistics_dict['team_id'] = last_row['participants'][player_indeks]['teamId']
        statistics_dict['nationality_id'] = last_row['participants'][player_indeks]['nationality']
    elif packet_id == 7: # car_status table
        player_indeks = last_row['header'][0]['playerCarIndex']
        statistics_dict['assist_tractionControl'] = last_row['carStatusData'][player_indeks]['tractionControl']
        statistics_dict['assist_antiLockBrakes'] = last_row['carStatusData'][player_indeks]['antiLockBrakes']

    statistics_dict['insert_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return statistics_dict

def insert_packets_to_bigquery(file_pattern, database):
    # gather batches from text file and split them to tuples
    with open(glob.glob(file_pattern)[0], 'r') as f:
        x = f.read().splitlines()

    tuples_list = []
    for single_line in x:
        tuples_list.append(make_tuple(single_line))

    # switch ordering of tuples, to get session info first
    # (necessary for proper data limitation)
    tuple_indexes = [x[0] for x in tuples_list]
    session_indeks = tuple_indexes.index(1)

    temp_tuple = tuples_list[0]
    tuples_list[0] = tuples_list[session_indeks]
    tuples_list[session_indeks] = temp_tuple

    # create dict for statistics
    statistics_dict = dict.fromkeys(
        ['sessionUID', 'publish_time', 'sessionType', 'sessionTime', 'sessionTime_format',
        'distance_driven', 'distance_driven_format', 'team_id', 'nationality_id', 'track_id', 
        'lap_count', 'fastest_lap', 'fastest_lap_format', 'record_lap', 'record_lap_format', 
        'event_win', 'event_fastest_lap', 'assist_tractionControl', 'assist_antiLockBrakes', 
        'datapoint_count_format', 'insert_time']
        )        
    statistics_dict['datapoint_count'] = 0

    # if event table tuple does not exist (e.g. for time trial), set up stats to 0
    if len(tuples_list[3][1]['data']) == 0:
        statistics_dict['event_win'] = 0
        statistics_dict['event_fastest_lap'] = 0

    # for each packet data, prepare it and insert to bigquery
    for single_tuple_indeks in range(0, len(tuples_list)):
        single_tuple = tuples_list[single_tuple_indeks]
        tuple_packet_id, tuple_packet_data = single_tuple
        # if packet data exist
        if len(tuple_packet_data['data']) > 0:
            tuple_schema = json.loads(tuple_packet_data['schemas'][0])
            tuple_table_name = tuple_schema['table_name']
            tuple_packet_data = json.loads(tuple_packet_data['data'][0])
            tuple_rows_to_insert_string = tuple_packet_data['rows_to_insert']      
            
            # in case there is only one row
            if '\n' not in tuple_rows_to_insert_string:
                rows_to_convert = [tuple_rows_to_insert_string]
            else:
                rows_to_convert = tuple_rows_to_insert_string.split('\n')
            
            # prepare schemas and rows
            schema_converted = bigquery_convert_to_schemas(schema_json = tuple_schema)

            # order packets by frameIdentifier and reverse back to simple packet list
            indeks_logging = []
            rows_to_add = []
            for single_packet in rows_to_convert:
                single_packet_load = json.loads(single_packet)
                indeks_frame = single_packet_load['header']['frameIdentifier']
                rows_to_add.append(
                    (
                        indeks_frame,
                        bigquery_convert_to_rows(packet_json = single_packet_load)
                    )
                )
                indeks_logging.append(indeks_frame)
            rows_to_add = sorted(rows_to_add, key=lambda x: x[0])
            rows_to_add = [x[1] for x in rows_to_add]
            
            # figure out sessionType for data limitation
            if tuple_packet_id == 1:
                session_type_id = rows_to_add[0]['sessionType']
            
            rows_to_add_reduced = reduce_data(rows_to_add, tuple_packet_id, database, session_type_id)

            if database == 'dashboard_data':
                statistics_dict['datapoint_count'] = statistics_dict['datapoint_count'] + len(rows_to_add_reduced)
                statistics_dict['datapoint_count_format'] = '{:,}'.format(
                    statistics_dict['datapoint_count']
                    ).replace(',', ' ')
                statistics_dict = create_stiatistics_row(statistics_dict, tuple_packet_id, rows_to_add_reduced)

            # insert to bigquery (apache beam does not support dynamic inserts and beam.io.WriteToBigQuery
            # needs a separate pipeline to make inserts, hence the for loop)
            pipeline = beam.Pipeline()
            stuff = pipeline | beam.Create(rows_to_add_reduced) | beam.io.WriteToBigQuery(
                        table='{}:{}.{}'.format(secrets['project_name'], database, tuple_table_name),
                        schema=schema_converted,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                        batch_size=500)

            result = pipeline.run()
            logging.info(
                'BigQuery pipeline {} for table {} {} in database {}'.format(
                    result.wait_until_finish(),
                    tuple_packet_id,
                    tuple_table_name,
                    database
                    )
                )
            del pipeline

    # add row to statistics table
    if database == 'dashboard_data':
        statistics_dict_row = [statistics_dict]
        # prepare schemas and rows
        
        schema_types = {
            'int': 'INT64',
            'str': 'STRING',
            'float': 'FLOAT',
            'datetime': 'DATETIME'
        }

        statistics_schema = {
            'schema': []
        }

        for single_structure in list(statistics_dict.keys()):
            statistics_schema['schema'].append(
                {
                    "mode": "NULLABLE",
                    "name": single_structure,
                    "type": schema_types[type(statistics_dict[single_structure]).__name__],
                    "description": None
                }
            )

        schema_converted = bigquery_convert_to_schemas(schema_json = statistics_schema)
        
        # insert to bigquery (apache beam does not support dynamic inserts and beam.io.WriteToBigQuery
        # needs a separate pipeline to make inserts, hence the for loop)
        pipeline = beam.Pipeline()
        stuff = pipeline | beam.Create(statistics_dict_row) | beam.io.WriteToBigQuery(
                    table='{}:{}.{}'.format(secrets['project_name'], database, 'statistics'),
                    schema=schema_converted,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    batch_size=500)

        result = pipeline.run()
        logging.info(
            'BigQuery pipeline {} for table statistics in database {}'.format(
                result.wait_until_finish(),
                database
                )
            )
        del pipeline

    for hgx in glob.glob("mapped_results*.txt"):
        os.remove(hgx)
    return True

def run(gcs_input_files, output_path, pipeline_args=None):
    # `save_main_session` is set to true because some DoFn's rely on
    # globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )
    
    logging.info('{} starting pipeline'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    pipeline = beam.Pipeline(options=pipeline_options)

    schemas_gcs = pipeline | 'get bigquery schemas' >> beam.io.textio.ReadFromText(
        file_pattern="gs://{}/bigquery-schemas/*".format(secrets['bucket_name'])
        )

    converted_schemas = schemas_gcs | 'convert schemas' >> ConvertSchemasToBeJoinedByKey()

    raw_gcs = pipeline | 'read raw gcs' >> beam.io.textio.ReadFromText(file_pattern=gcs_input_files)
    converted = raw_gcs | 'convert JSON' >> ConvertPacketsToJSON()
    bigquery = converted | 'convert to BigQuery JSON' >> ConvertToBigQueryJSON()
    batches = bigquery | 'split to packetId batches' >> GroupFilesIntoBatches()
    clean_gcs = batches | 'save clean gcs' >> beam.ParDo(WriteFilesToGCS(output_path))
    batches_output = clean_gcs | 'add output batches' >> GroupOutputFilesIntoBatches()
    results = ({'schemas': converted_schemas, 'data': batches_output} | 'CoGroupByKey' >> beam.CoGroupByKey())
    text_write = results | 'Temp Write3' >> beam.io.WriteToText('mapped_results', file_name_suffix='.txt')

    result = pipeline.run()
    result.wait_until_finish()

if __name__ == "__main__":  # noqa
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local_debugging",
        help="flag for setting up local debugging instead of worker VM",
    )
    parser.add_argument(
        "--gcs_input_files",
        help="The GCS directory to desired files listing",
    )
    parser.add_argument(
        "--output_path",
        help="GCS Path of the output file including filename prefix.",
    )
    known_args, pipeline_args = parser.parse_known_args()
    
    # initialize secrets
    secrets = {}
    f = open('secrets.sh', 'r')
    lines_read = f.read().splitlines()[1:]
    f.close()

    for line in lines_read:
        line_splitted = line.replace('\n', '').replace('"', '').split('=')
        secrets[line_splitted[0]] = line_splitted[1]

    if known_args.local_debugging != None:
        # setup credentials for gcs
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '{}{}'.format(secrets['local_path'], secrets['computeengine_service_account_file'])

    logging.info('{} starting pipeline'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    run(
        known_args.gcs_input_files,
        known_args.output_path,
        pipeline_args,
    )
    
    logging.info(
        '{} starting bigquery dashboard data insert'.format(
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        )

    # dashboard-limited data insert
    insert_packets_to_bigquery(file_pattern = "mapped_results-*.txt", database = 'dashboard_data')

    logging.info(
        '{} starting bigquery full data insert'.format(
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        )
    
    #full data insert
    insert_packets_to_bigquery(file_pattern = "mapped_results-*.txt", database = 'packets_data')

    data = {
        'auth_token': secrets['webhook_auth_token']
    } 

    webhook_url = 'http://{}:5000/worker-off?auth-token={}'.format(
        secrets['vm_launcher_external_ip'], secrets['webhook_auth_token'])
        
    requests.post(url = webhook_url, data = data)

