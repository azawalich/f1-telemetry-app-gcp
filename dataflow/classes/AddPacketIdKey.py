import apache_beam as beam
import simplejson as json

class AddPacketIdKey(beam.DoFn):
    def process(self, element):
        yield {
            "packet_id": json.loads(element)['header']['packetId'],
            "session_id": json.loads(element)['header']['sessionUID'],
            "packet_json": element
        }
