import argparse
import datetime
import logging
import os
import requests
import time
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from classes.ConvertSchemasToBeJoinedByKey import ConvertSchemasToBeJoinedByKey
from classes.ConvertPacketsToJSON import ConvertPacketsToJSON
from classes.ConvertToBigQueryJSON import ConvertToBigQueryJSON
from classes.GroupFilesIntoBatches import GroupFilesIntoBatches
from classes.WriteFilesToGCS import WriteFilesToGCS
from classes.WriteBatchesToGCS import WriteBatchesToGCS
from classes.GroupWindowsIntoBatches import GroupWindowsIntoBatches
from classes.GroupOutputFilesIntoBatches import GroupOutputFilesIntoBatches
from functions.insert_packets_to_bigquery import insert_packets_to_bigquery

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
    results | 'Temp Write3' >> beam.io.WriteToText('mapped_results', file_name_suffix='.txt')

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
    insert_packets_to_bigquery(
        file_pattern = "mapped_results-*.txt",
        database = 'dashboard_data',
        project_name=secrets['project_name']
        )

    logging.info(
        '{} starting bigquery full data insert'.format(
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        )
    
    #full data insert
    insert_packets_to_bigquery(
        file_pattern = "mapped_results-*.txt",
        database = 'packets_data',
        project_name=secrets['project_name']
        )

    data = {
        'auth_token': secrets['webhook_auth_token']
    } 

    webhook_url = 'http://{}:5000/worker-off?auth-token={}'.format(
        secrets['vm_launcher_external_ip'], secrets['webhook_auth_token'])
        
    requests.post(url = webhook_url, data = data)

