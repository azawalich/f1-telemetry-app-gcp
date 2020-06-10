import apache_beam as beam
import logging
import datetime

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