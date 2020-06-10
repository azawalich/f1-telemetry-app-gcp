import simplejson as json
import glob
import apache_beam as beam
from ast import literal_eval as make_tuple
import logging
import os

from functions.bigquery_convert_to_rows import bigquery_convert_to_rows
from functions.bigquery_convert_to_schemas import bigquery_convert_to_schemas
from functions.reduce_data import reduce_data
from functions.create_stiatistics_row import create_stiatistics_row

def insert_packets_to_bigquery(file_pattern, database, project_name):
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
            pipeline | beam.Create(rows_to_add_reduced) | beam.io.WriteToBigQuery(
                        table='{}:{}.{}'.format(project_name, database, tuple_table_name),
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
        pipeline | beam.Create(statistics_dict_row) | beam.io.WriteToBigQuery(
                    table='{}:{}.{}'.format(project_name, database, 'statistics'),
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