import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from classes.GroupWindowsIntoBatches import GroupWindowsIntoBatches
from classes.WriteBatchesToGCS import WriteBatchesToGCS

def run(input_topic, output_path, window_size=1.0, pipeline_args=None):
    # `save_main_session` is set to true because some DoFn's rely on
    # globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read PubSub Messages"
            >> beam.io.ReadFromPubSub(topic=input_topic)
            | "Window into" >> GroupWindowsIntoBatches(window_size)
            | "Write to GCS" >> beam.ParDo(WriteBatchesToGCS(output_path))
        )


if __name__ == "__main__":  # noqa
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        help="The Cloud Pub/Sub topic to read from.\n"
        '"projects/<PROJECT_NAME>/topics/<TOPIC_NAME>".',
    )
    parser.add_argument(
        "--window_size",
        type=float,
        default=1.0,
        help="Output file's window size in number of minutes.",
    )
    parser.add_argument(
        "--output_path",
        help="GCS Path of the output file including filename prefix.",
    )
    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_topic,
        known_args.output_path,
        known_args.window_size,
        pipeline_args,
    )