import json
import apache_beam as beam

class WriteBatchesToGCS(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, batch, window=beam.DoFn.WindowParam):
        """Write one batch per file to a Google Cloud Storage bucket. """

        ts_format = "%H:%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        filename = "-".join([self.output_path, window_start, window_end])
        with beam.io.gcp.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            for element in batch:
                f.write("{}\n".format(json.dumps(element)).encode("utf-8"))
