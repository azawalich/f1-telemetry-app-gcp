import argparse
import datetime
import json
import logging
import jsonpickle

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

def check_upacked_udp_packet_types(unpacked_udp_packet):
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

def convert_upacked_udp_packet_to_json(unpacked_udp_packet, publish_time):
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
                temp_dict.append(check_upacked_udp_packet_types(getattr(unpacked_udp_packet, single_field)[single_list_element]))
            full_dict[single_field] = temp_dict
        else:
           full_dict[single_field] = check_upacked_udp_packet_types(getattr(unpacked_udp_packet, single_field))
    full_dict['publish_time'] = publish_time
    return json.dumps(full_dict, ensure_ascii=False).encode('utf8').decode()

class convertPacketsToJSON(beam.PTransform):
    def cleanup_packets(self, single_file):
        json_packets = []
        temp_packet = json.loads(single_file)
        decoded_packet = jsonpickle.decode(temp_packet['packet_encoded'])
        json_packet = convert_upacked_udp_packet_to_json(decoded_packet, temp_packet['publish_time'])
        json_packets.append(json_packet)
        return json_packets

    def expand(self, pcoll):
        return (
            pcoll
            | beam.Map(lambda x: self.cleanup_packets(single_file = x))
        )

class WriteFilesToGCS(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, batch):
        """Write one batch per file to a Google Cloud Storage bucket. """
        filename = None

        for element in batch:
            element_json = json.loads(element)
            filename = '{}{}_{}_{}.json'.format(
                self.output_path, 
                element_json['header']['sessionUID'],
                element_json['header']['frameIdentifier'],
                element_json['header']['packetId']
                )

        with beam.io.gcp.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            for element in batch:
                f.write(element.encode())


def run(gcs_input_files, output_path, pipeline_args=None):
    # `save_main_session` is set to true because some DoFn's rely on
    # globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read from raw GCS bucket" >>  beam.io.textio.ReadFromText(file_pattern=gcs_input_files)
            | "Convert To JSON" >>  convertPacketsToJSON()
            | "Save to clean GCS bucket" >>  beam.ParDo(WriteFilesToGCS(output_path))
        )

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

    run(
        known_args.gcs_input_files,
        known_args.output_path,
        pipeline_args,
    )

