import apache_beam as beam
import datetime

class AddTimestamps(beam.DoFn):
    def process(self, element, publish_time=beam.DoFn.TimestampParam):
        """Processes each incoming windowed element by extracting the Pub/Sub
        message and its publish timestamp into a dictionary. `publish_time`
        defaults to the publish timestamp returned by the Pub/Sub server. It
        is bound to each element by Beam at runtime.
        """

        yield {
            "packet_encoded": element.decode("utf-8"),
            "publish_time": datetime.datetime.utcfromtimestamp(
                float(publish_time)
            ).strftime("%Y-%m-%d %H:%M:%S"),
        }