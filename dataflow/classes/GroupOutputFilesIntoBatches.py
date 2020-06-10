import apache_beam as beam
import simplejson as json
import logging
import datetime

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