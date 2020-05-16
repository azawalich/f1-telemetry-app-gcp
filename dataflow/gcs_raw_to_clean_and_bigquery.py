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

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions


# initialize secrets
secrets = {}
f = open('secrets.sh', 'r')
lines_read = f.read().splitlines()[1:]
f.close()

for line in lines_read:
    line_splitted = line.replace('\n', '').replace('"', '').split('=')
    secrets[line_splitted[0]] = line_splitted[1]


class ConvertPacketsToJSON(beam.PTransform):

    def check_upacked_udp_packet_types(self, unpacked_udp_packet):
        """Check types in attributes of unpacked telemetry packet (appropriately-typed one) and return basic dict-structure for it.

        Args:
            unpacked_udp_packet: the contents of the unpacked UDP packet.

        Returns:
            dict in a basic form to be later-on added to full packet JSON representation.
        """

        if hasattr(unpacked_udp_packet, '_fields_'):
            temp_dict = {}
            for single_attribute in [x[0] for x in getattr(unpacked_udp_packet, '_fields_')]:
                if isinstance(getattr(unpacked_udp_packet, single_attribute), int) or isinstance(getattr(unpacked_udp_packet, single_attribute), float):
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
            if hasattr(getattr(unpacked_udp_packet, single_field), '__len__') and hasattr(getattr(unpacked_udp_packet, single_field), 'isascii') == False:
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
        logging.info('{} starting ConvertPacketsToJSON'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        return (
            pcoll
            | beam.Map(lambda x: self.cleanup_packets(single_file = x))
        )

class ConvertSchemasToBeJoinedByKey(beam.PTransform):
    def convertSchemasFromGCS(self, single_file):
        single_file_decoded = json.loads(single_file)
        return single_file_decoded

    def expand(self, pcoll):       
        logging.info('{} starting ConvertSchemasToBeJoinedByKey'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
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

    def convert_lists_to_wheel_dicts(self, json_element):
        if isinstance(json_element, list) and len(json_element) == 4:
            wheel_names = ['RL', 'RR', 'FL', 'FR']
            wheel_dict = dict.fromkeys(wheel_names)
            for single_wheel in range(0,4):
                wheel_dict[wheel_names[single_wheel]] = json_element[single_wheel]
        
        return wheel_dict

    def convert_json_packet_to_bigquery_compliant(self, packet_json):
        for single_element in list(packet_json.keys()):
            if single_element in self.wheel_fields: # normal lists
                packet_json[single_element] = [self.convert_lists_to_wheel_dicts(json_element = packet_json[single_element])]
            if single_element in ['carTelemetryData', 'carStatusData']: # nested lists
                for i in range(0, len(packet_json[single_element])):
                    for single_keyy in list(packet_json[single_element][i].keys()):
                        if single_keyy in self.wheel_fields: 
                            packet_json[single_element][i][single_keyy] = [
                                self.convert_lists_to_wheel_dicts(json_element = packet_json[single_element][i][single_keyy])]
        return packet_json
    
    def convert_bigquery(self, element):
        decoded_json = json.loads(element[0])
        bigquery_packet = self.convert_json_packet_to_bigquery_compliant(packet_json = decoded_json)
        encoded_bigquery_packet = json.dumps(bigquery_packet, ignore_nan=True)
        return encoded_bigquery_packet

    def expand(self, pcoll):
        logging.info('{} starting ConvertToBigQueryJSON'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        return (
            pcoll
            | beam.Map(lambda x: self.convert_bigquery(element = x))
        )

class AddPacketIdKey(beam.DoFn):
    def process(self, element):
        #logging.info('{} starting AddPacketIdKey'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        yield {
            "packet_id": json.loads(element)['header']['packetId'],
            "session_id": json.loads(element)['header']['sessionUID'],
            "packet_json": element
        }

class GroupFilesIntoBatches(beam.PTransform):

    def expand(self, pcoll):
        logging.info('{} starting GroupFilesIntoBatches'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
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
        logging.info('{} starting GroupOutputFilesIntoBatches'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
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
        logging.info('{} starting WriteFilesToGCS'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
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

def insert_packets_to_bigquery(file_pattern):
    # gather batches from text file and split them to tuples
    with open(glob.glob(file_pattern)[0], 'r') as f:
        x = f.read().splitlines()

    tuples_list = []
    for single_line in x:
        tuples_list.append(make_tuple(single_line))

    # for each packet data, prepare it and insert to bigquery
    for single_tuple in tuples_list:
        tuple_packet_id, tuple_packet_data = single_tuple
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
        rows_to_add = []
        for single_packet in rows_to_convert:
            rows_to_add.append(bigquery_convert_to_rows(packet_json = json.loads(single_packet)))
        
        # insert to bigquery (apache beam does not support dynamic inserts and beam.io.WriteToBigQuery
        # needs a separate pipeline to make inserts, hence the for loop)
        pipeline = beam.Pipeline()
        stuff = pipeline | beam.Create(rows_to_add) | beam.io.WriteToBigQuery(
                    table='{}:packets_data.{}'.format(secrets['project_name'], tuple_table_name),
                    schema=schema_converted,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    batch_size=300)

        result = pipeline.run()
        print('BigQuery pipeline {} for table: {} {}'.format(result.wait_until_finish(), tuple_packet_id, tuple_table_name))
        del pipeline
    
    for hgx in glob.glob("mapped_results*.txt"):
        os.remove(hgx)
    return result.wait_until_finish()

def run(gcs_input_files, output_path, pipeline_args=None):
    # `save_main_session` is set to true because some DoFn's rely on
    # globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )
    
    logging.info('{} starting pipeline'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    pipeline = beam.Pipeline(options=pipeline_options)

    schemas_gcs = pipeline | 'get bigquery schemas' >> beam.io.textio.ReadFromText(file_pattern="gs://{}/bigquery-schemas/*".format(secrets['bucket_name']))
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
        "--gcs_input_files",
        help="The GCS directory to desired files listing",
    )
    parser.add_argument(
        "--output_path",
        help="GCS Path of the output file including filename prefix.",
    )
    known_args, pipeline_args = parser.parse_known_args()
    
    logging.info('{} starting pipeline'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    run(
        known_args.gcs_input_files,
        known_args.output_path,
        pipeline_args,
    )
    
    logging.info('{} starting bigquery final insert'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    insert_packets_to_bigquery(file_pattern = "mapped_results-*.txt")

    time.sleep(60)

    data = {
    'auth_token': secrets['webhook_auth_token']
    } 

    webhook_url = 'http://{}:5000/worker-off?auth-token={}'.format(
        secrets['vm_launcher_external_ip'], secrets['webhook_auth_token'])
        
    requests.post(url = webhook_url, data = data)


