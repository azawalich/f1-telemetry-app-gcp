import datetime

def bigquery_convert_to_rows(packet_json):
    single_row = {}
    packet_json['insert_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for single_key in packet_json.keys():
        if isinstance(packet_json[single_key], dict):
            single_row[single_key] = [packet_json[single_key]]
        elif isinstance(packet_json[single_key], list):
            if len(packet_json[single_key]) > 1:
                temp_list = []
                for single_list_element in range(0, len(packet_json[single_key])):
                    temp_list.append(packet_json[single_key][single_list_element])
                single_row[single_key] = temp_list
            else:
                single_row[single_key] = [packet_json[single_key][0]]
        else:
            single_row[single_key] = packet_json[single_key]
    return single_row