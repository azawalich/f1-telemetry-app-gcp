import apache_beam as beam
import simplejson as json
import logging
import datetime

from classes.AddPacketIdKey import AddPacketIdKey

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