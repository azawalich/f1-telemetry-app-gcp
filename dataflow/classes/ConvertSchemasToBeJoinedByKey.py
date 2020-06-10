import apache_beam as beam
import simplejson as json
import logging
import datetime

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