import apache_beam as beam
import simplejson as json
import jsonpickle
import logging
import datetime

class ConvertPacketsToJSON(beam.PTransform):

    def check_upacked_udp_packet_types(self, unpacked_udp_packet):
        """Check types in attributes of unpacked telemetry packet (appropriately-typed one) 
        and return basic dict-structure for it.

        Args:
            unpacked_udp_packet: the contents of the unpacked UDP packet.

        Returns:
            dict in a basic form to be later-on added to full packet JSON representation.
        """

        if hasattr(unpacked_udp_packet, '_fields_'):
            temp_dict = {}
            for single_attribute in [x[0] for x in getattr(unpacked_udp_packet, '_fields_')]:
                if isinstance(getattr(unpacked_udp_packet, single_attribute), int) or \
                    isinstance(getattr(unpacked_udp_packet, single_attribute), float):
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

    def convert_upacked_udp_packet_to_json(self, unpacked_udp_packet, publish_time):
        """Convert unpacked telemetry packet (appropriately-typed one) to its' JSON representation.

        Args:
            unpacked_udp_packet: the contents of the unpacked UDP packet.

        Returns:
            JSON representation of the unpacked UDP packet.
        """
        full_dict = {}
        for single_field in [x[0] for x in unpacked_udp_packet._fields_]:
            if hasattr(getattr(unpacked_udp_packet, single_field), '__len__') and \
                hasattr(getattr(unpacked_udp_packet, single_field), 'isascii') == False:
                temp_dict = []
                for single_list_element in range(0, len(getattr(unpacked_udp_packet, single_field))): 
                    temp_dict.append(self.check_upacked_udp_packet_types(
                        unpacked_udp_packet = getattr(unpacked_udp_packet, single_field)[single_list_element]))
                full_dict[single_field] = temp_dict
            else:
                full_dict[single_field] = self.check_upacked_udp_packet_types(
                    unpacked_udp_packet = getattr(unpacked_udp_packet, single_field))
        full_dict['publish_time'] = publish_time
        full_dict['header']['sessionUID'] = str(full_dict['header']['sessionUID'])
        return json.dumps(full_dict, ensure_ascii=False).encode('utf8').decode()

    def cleanup_packets(self, single_file):
        json_packets = []
        temp_packet = json.loads(single_file)
        decoded_packet = jsonpickle.decode(temp_packet['packet_encoded'])
        json_packet = self.convert_upacked_udp_packet_to_json(
            unpacked_udp_packet = decoded_packet, 
            publish_time = temp_packet['publish_time']
            )
        json_packets.append(json_packet)
        return json_packets
    
    def expand(self, pcoll):
        logging.info(
            '{} starting ConvertPacketsToJSON'.format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        )
        return (
            pcoll
            | beam.Map(lambda x: self.cleanup_packets(single_file = x))
        )