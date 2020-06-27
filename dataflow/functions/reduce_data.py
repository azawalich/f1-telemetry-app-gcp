def reduce_data(rows_to_add, packet_id, database, session_type):
    tables_columns = {
        0: 'carMotionData',
        2: 'lapData',
        4: 'participants',
        5: 'carSetups',
        6: 'carTelemetryData',
        7: 'carStatusData',
    }

    # reducing dummy information for any database
    # if table different than session, event AND session is time trial
    if packet_id not in [1, 3] and session_type == 12:
        # in each row, take only first participant from particular column
        for single_row_indeks in range(0, len(rows_to_add)):
            rows_to_add[single_row_indeks][tables_columns[packet_id]] = \
                [rows_to_add[single_row_indeks][tables_columns[packet_id]][0]]

    # trimming data to absolute minimum for dashboard_data database
    if database == 'dashboard_data':
        # motion, car setup, car_telemetry, car status table
        if packet_id in [0, 5, 6, 7]:
            # reducing amount of data: first row from for each second (unique rounded sessionTime)
            session_times_rounded = {
                'second': [],
                'second_indeks': []
            }
            for single_row_indeks in range(0, len(rows_to_add)):
                temp_row = rows_to_add[single_row_indeks]
                if single_row_indeks == 0:
                    session_times_rounded['second'].append(single_row_indeks)
                    session_times_rounded['second_indeks'].append(single_row_indeks)
                else:
                    second_from_row = int(
                        round(temp_row['header'][0]['sessionTime'], 0)
                        )
                    if second_from_row not in session_times_rounded['second']:
                        session_times_rounded['second'].append(second_from_row)
                        session_times_rounded['second_indeks'].append(single_row_indeks)

            rows_to_add = [rows_to_add[i] for i in session_times_rounded['second_indeks']] 
        # session table
        elif packet_id == 1:
            indekses_list = []
            # any row with safetyCarStatus > 0 and any marshalZones['zoneFlag'] > 0
            for single_row_indeks in range(0, len(rows_to_add)):
                temp_row = rows_to_add[single_row_indeks]
                marshal_zones_flag = [d['zoneFlag'] for d in temp_row['marshalZones']]
                if temp_row['safetyCarStatus'] != 0 or \
                    len(set(marshal_zones_flag)) > 1:
                    indekses_list.append(single_row_indeks)
            # last row
            indekses_list.append(len(rows_to_add)-1)
            # join all together
            rows_to_add = [rows_to_add[i] for i in list(set(indekses_list))] 
        # lap table
        elif packet_id == 2:
            participant_dict = {k: [] for k in range(0, len(rows_to_add[0]['lapData']))}
            participant_dict2 = {k: [] for k in range(0, len(rows_to_add[0]['lapData']))}

            # for all players, last row from each CurrentLapNum
            for single_row_indeks in range(0, len(rows_to_add)):
                #split into participant groups
                temp_row = rows_to_add[single_row_indeks]
                for single_participant_indeks in range(0, len(temp_row['lapData'])):
                    participant_dict[single_participant_indeks].append(
                        temp_row['lapData'][single_participant_indeks]['currentLapNum']
                        )
                    participant_dict2[single_participant_indeks].append(
                        temp_row['lapData'][single_participant_indeks]['pitStatus']
                        )
                        
            all_indexes = []
            # determine last laps for each participant
            for single_participant in participant_dict.keys():
                temp_participant = participant_dict[single_participant]
                last_lap_indexes = [i for i, x in enumerate(temp_participant) \
                    if i == len(temp_participant) - 1 or x != temp_participant[i + 1]]
                all_indexes.append(last_lap_indexes)

            #determine first and last pitstop status index for each participant
            pitstop_indexes = []
            for single_participant in participant_dict2.keys():
                temp_participant = participant_dict2[single_participant]
                for single_status in sorted(set(temp_participant)):
                    if single_status > 0:
                        pitstop_statuses = [
                            i for i, x in enumerate(temp_participant) if x == single_status
                            ]
                        res, last = [[]], None
                        for x in pitstop_statuses:
                            if last is None or abs(last - x) <= 1:
                                res[-1].append(x)
                            else:
                                res.append([x])
                            last = x
                        for single_status_group_indeks in range(0, len(res)):
                            res[single_status_group_indeks] = [
                                res[single_status_group_indeks][0],
                                res[single_status_group_indeks][-1]
                                ]
                        flat_res = [item for sublist in res for item in sublist]
                        pitstop_indexes.append(flat_res)

            pitstop_indexes_flat = [item for sublist in pitstop_indexes for item in sublist]
            pitstop_indexes_flat = sorted(set(pitstop_indexes_flat))

            # join all together
            all_indexes_flat = [item for sublist in all_indexes for item in sublist]
            all_indexes_flat = sorted(set(all_indexes_flat))

            full_list = all_indexes_flat + pitstop_indexes_flat
            full_list = sorted(set(full_list))

            rows_to_add = [rows_to_add[i] for i in full_list]
        # event table
        elif packet_id == 3:
            # no changes
            pass
        # participant table
        elif packet_id == 4:
            # last row
            rows_to_add = [rows_to_add[-1]]

    return rows_to_add