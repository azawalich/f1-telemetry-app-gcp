import apache_beam as beam
import simplejson as json
import logging
import datetime

class ConvertToBigQueryJSON(beam.PTransform):
    wheel_fields = [
        'suspensionPosition', 'suspensionVelocity', 'suspensionAcceleration', 'wheelSpeed', 
        'wheelSlip', 'brakesTemperature', 'tyresSurfaceTemperature', 'tyresInnerTemperature', 
        'tyresPressure', 'surfaceType', 'tyresWear', 'tyresDamage']
    
    emptiable_fields = ['carSetups', 'carStatusData', 'carTelemetryData', 'lapData', 
        'carMotionData', 'participants', 'marshalZones']

    def convert_lists_to_wheel_dicts(self, json_element):
        if isinstance(json_element, list) and len(json_element) == 4:
            wheel_names = ['RL', 'RR', 'FL', 'FR']
            wheel_dict = dict.fromkeys(wheel_names)
            for single_wheel in range(0,4):
                wheel_dict[wheel_names[single_wheel]] = json_element[single_wheel]
        
        return wheel_dict
    
    def clean_emptiable_fields(self, packet_json):
        # some fields are dummy filled in with 0 or '', we can delete these to reduce the number of rows
        for single_element in list(packet_json.keys()):
            if single_element in self.emptiable_fields:
                emptiable_indexes = []
                for indeks in range(0, len(packet_json[single_element])):
                    single_list_element = packet_json[single_element][indeks]
                    # it turns out that nested lists need to be checked additionally for emptiability
                    # and reduced to a non-list type which map supports
                    single_list_element_values = list(single_list_element.values())
                    single_list_element_values_types = [type(item).__name__ for item in single_list_element_values]
                    for single_value in range(0, len(single_list_element_values_types)):
                        if single_list_element_values_types[single_value] == 'list':
                            mapped_boolean_list = list(map(bool, list(set(single_list_element_values[single_value]))))
                            if True in mapped_boolean_list:
                                single_list_element_values[single_value] = True
                            else: 
                                single_list_element_values[single_value] = False
                    # map all dict values to bool, 0 and '' give False, everything else gives True (also ' ' !)
                    unique_values = list(map(bool, list(set(single_list_element_values))))
                    # if only unnecessary in list, safely delete it 
                    if True not in unique_values:
                        emptiable_indexes.append(indeks)        
                for indeks in sorted(emptiable_indexes, reverse=True):
                    del packet_json[single_element][indeks]
        return packet_json

    def convert_json_packet_to_bigquery_compliant(self, packet_json):
        for single_element in list(packet_json.keys()):
            if single_element in self.wheel_fields: # normal lists
                packet_json[single_element] = \
                    [self.convert_lists_to_wheel_dicts(json_element = packet_json[single_element])]
            if single_element in ['carTelemetryData', 'carStatusData']: # nested lists
                for i in range(0, len(packet_json[single_element])):
                    for single_keyy in list(packet_json[single_element][i].keys()):
                        if single_keyy in self.wheel_fields: 
                            packet_json[single_element][i][single_keyy] = \
                                [
                                    self.convert_lists_to_wheel_dicts(
                                        json_element = packet_json[single_element][i][single_keyy]
                                        )
                                ]
        return packet_json
    
    def convert_bigquery(self, element):
        decoded_json = json.loads(element[0])
        clean_json = self.clean_emptiable_fields(packet_json = decoded_json)
        bigquery_packet = self.convert_json_packet_to_bigquery_compliant(packet_json = clean_json)
        encoded_bigquery_packet = json.dumps(bigquery_packet, ignore_nan=True)
        return encoded_bigquery_packet

    def expand(self, pcoll):
        logging.info(
            '{} starting ConvertToBigQueryJSON'.format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
            )
        return (
            pcoll
            | beam.Map(lambda x: self.convert_bigquery(element = x))
        )