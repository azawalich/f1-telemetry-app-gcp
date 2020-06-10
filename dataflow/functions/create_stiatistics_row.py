import time
import datetime

def create_stiatistics_row(statistics_dict, packet_id, rows_to_add_reduced):
    last_row = rows_to_add_reduced[-1]
    if packet_id == 1: # session table
        statistics_dict['sessionUID'] = last_row['header'][0]['sessionUID']
        statistics_dict['sessionType'] = last_row['sessionType']
        statistics_dict['sessionTime'] = int(
            round(last_row['header'][0]['sessionTime'], 0)
            )
        statistics_dict['sessionTime_format'] = time.strftime(
            '%Hh %Mm %Ss', 
            time.gmtime(
                int(
                    round(last_row['header'][0]['sessionTime'], 0)
                    )
                )
            )
        statistics_dict['publish_time'] = (
            datetime.datetime.strptime(last_row['publish_time'],"%Y-%m-%d %H:%M:%S") - \
            datetime.timedelta(seconds=statistics_dict['sessionTime'])
            ).strftime("%Y-%m-%d %H:%M:%S")
        statistics_dict['track_id'] = last_row['trackId']
    elif packet_id == 2: # lap table
        player_indeks = last_row['header'][0]['playerCarIndex']
        statistics_dict['distance_driven'] = int(round(last_row['lapData'][player_indeks]['totalDistance']))
        statistics_dict['distance_driven_format'] = '{:,}km'.format(
            statistics_dict['distance_driven'] / 1000
            ).replace(',', ' ')
        statistics_dict['lap_count'] = last_row['lapData'][player_indeks]['currentLapNum']
        
        # to get fastest lap we need to iterate through all lap rows
        lap_times = []
        for single_row_indeks in range(0, len(rows_to_add_reduced)):
            single_row = rows_to_add_reduced[single_row_indeks]
            lap_times.append(single_row['lapData'][player_indeks]['lastLapTime'])
        
        statistics_dict['fastest_lap'] = min(i for i in lap_times if i > 0) 
        statistics_dict['fastest_lap_format'] = str(
            datetime.timedelta(seconds=statistics_dict['fastest_lap'])
            )[2:-3]

        # fix for low fastest lap data refresh 
        if statistics_dict['fastest_lap'] < last_row['lapData'][player_indeks]['bestLapTime']:
            record_lap = statistics_dict['fastest_lap']
        else:
            record_lap = last_row['lapData'][player_indeks]['bestLapTime']

        statistics_dict['record_lap'] = record_lap
        statistics_dict['record_lap_format'] = str(
            datetime.timedelta(seconds=statistics_dict['record_lap'])
            )[2:-3]
    elif packet_id == 3: # event table
        player_indeks = last_row['header'][0]['playerCarIndex']

        # to get fastest lap we need to iterate through all event rows
        event_codes = {
            'race_winner': [],
            'fastest_lap': []
        }

        for single_row_indeks in range(0, len(rows_to_add_reduced)):
            single_row = rows_to_add_reduced[single_row_indeks]
            if single_row['vehicleIdx'] == single_row['header'][0]['playerCarIndex']:
                if single_row['eventStringCode'] == 'RCWN':
                    event_codes['race_winner'].append(1)
                elif single_row['eventStringCode'] == 'FTLP':
                    event_codes['fastest_lap'].append(1)
            
        statistics_dict['event_win'] = len(event_codes['race_winner'])
        statistics_dict['event_fastest_lap'] = len(event_codes['fastest_lap'])
    elif packet_id == 4: # participant table
        player_indeks = last_row['header'][0]['playerCarIndex']
        statistics_dict['team_id'] = last_row['participants'][player_indeks]['teamId']
        statistics_dict['nationality_id'] = last_row['participants'][player_indeks]['nationality']
    elif packet_id == 7: # car_status table
        player_indeks = last_row['header'][0]['playerCarIndex']
        statistics_dict['assist_tractionControl'] = last_row['carStatusData'][player_indeks]['tractionControl']
        statistics_dict['assist_antiLockBrakes'] = last_row['carStatusData'][player_indeks]['antiLockBrakes']

    statistics_dict['insert_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return statistics_dict