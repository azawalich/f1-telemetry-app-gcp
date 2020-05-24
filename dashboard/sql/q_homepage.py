row_count_tables = '''
SELECT "car_setup" as table_name, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.car_setup` UNION ALL
SELECT "car_status" as table_name, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.car_status` UNION ALL
SELECT "car_telemetry" as table_name, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.car_telemetry` UNION ALL
SELECT "event" as table_name, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.event` UNION ALL
SELECT "lap" as table_name, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.lap` UNION ALL
SELECT "motion" as table_name, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.motion` UNION ALL
SELECT "participant" as table_name, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.participant` UNION ALL
SELECT "session" as table_name, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.session`
'''

row_count_tables_sessions = '''
SELECT "car_setup" as table_name, header.sessionUID, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.car_setup`, UNNEST(header) as header GROUP BY header.sessionUID UNION ALL
SELECT "car_status" as table_name, header.sessionUID, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.car_status`, UNNEST(header) as header GROUP BY header.sessionUID UNION ALL
SELECT "car_telemetry" as table_name, header.sessionUID, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.car_telemetry`, UNNEST(header) as header GROUP BY header.sessionUID UNION  ALL
SELECT "event" as table_name, header.sessionUID, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.event`, UNNEST(header) as header GROUP BY header.sessionUID UNION ALL
SELECT "lap" as table_name, header.sessionUID, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.lap`, UNNEST(header) as header GROUP BY header.sessionUID UNION ALL
SELECT "motion" as table_name, header.sessionUID, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.motion`, UNNEST(header) as header GROUP BY header.sessionUID UNION ALL
SELECT "participant" as table_name, header.sessionUID, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.participant`, UNNEST(header) as header GROUP BY header.sessionUID UNION ALL
SELECT "session" as table_name, header.sessionUID, count(insert_time) as row_count FROM `f1-telemetry-app.packets_data.session`, UNNEST(header) as header GROUP BY header.sessionUID ORDER BY table_name
'''

distinct_sessions = '''
select distinct header.sessionUID, insert_time from `f1-telemetry-app.packets_data.car_setup`, UNNEST(header) as header
'''

time_driven_sessions = '''
SELECT "car_setup" as table_name, header.sessionUID, min(publish_time) as min_publish_time, max(publish_time) as max_publish_time FROM `f1-telemetry-app.packets_data.car_setup`, unnest(header) as header GROUP BY header.sessionUID UNION ALL
SELECT "car_status" as table_name, header.sessionUID, min(publish_time) as min_publish_time, max(publish_time) as max_publish_time FROM `f1-telemetry-app.packets_data.car_status`, unnest(header) as header GROUP BY header.sessionUID UNION ALL
SELECT "car_telemetry" as table_name, header.sessionUID, min(publish_time) as min_publish_time, max(publish_time) as max_publish_time FROM `f1-telemetry-app.packets_data.car_telemetry`, unnest(header) as header GROUP BY header.sessionUID UNION ALL
SELECT "event" as table_name, header.sessionUID, min(publish_time) as min_publish_time, max(publish_time) as max_publish_time FROM `f1-telemetry-app.packets_data.event`, unnest(header) as header GROUP BY header.sessionUID UNION ALL
SELECT "lap" as table_name, header.sessionUID, min(publish_time) as min_publish_time, max(publish_time) as max_publish_time FROM `f1-telemetry-app.packets_data.lap`, unnest(header) as header GROUP BY header.sessionUID UNION ALL
SELECT "motion" as table_name, header.sessionUID, min(publish_time) as min_publish_time, max(publish_time) as max_publish_time FROM `f1-telemetry-app.packets_data.motion`, unnest(header) as header GROUP BY header.sessionUID UNION ALL
SELECT "participant" as table_name, header.sessionUID, min(publish_time) as min_publish_time, max(publish_time) as max_publish_time FROM `f1-telemetry-app.packets_data.participant`, unnest(header) as header GROUP BY header.sessionUID UNION ALL
SELECT "session" as table_name, header.sessionUID, min(publish_time) as min_publish_time, max(publish_time) as max_publish_time FROM `f1-telemetry-app.packets_data.session`, unnest(header) as header GROUP BY header.sessionUID
'''

event_sessions = '''
SELECT * FROM `f1-telemetry-app.definitions.event_code` as event_code 
INNER JOIN (select header.sessionUID, header.playerCarIndex, vehicleIdx, eventStringCode, "event" as table_name from `f1-telemetry-app.packets_data.event` as event_table, unnest(header) as header) as event_table
ON event_code.id = event_table.eventStringCode'''