recent_statistics = '''
select * from `f1-telemetry-app.dashboard_data.statistics` as dd_stats
JOIN (select id as team_id, value as team from `f1-telemetry-app.definitions.team`) as def_team using(team_id)
JOIN (select id as nationality_id, value as nationality from `f1-telemetry-app.definitions.nationality`) as def_nats using(nationality_id)
JOIN (select id as track_id, value as track from `f1-telemetry-app.definitions.track`) as def_tck using(track_id)
JOIN (select distinct sessionUID, weather, trackLength from `f1-telemetry-app.dashboard_data.session`, unnest(header) as header) as def_ses using(sessionUID)
JOIN (select sessionUID, max(airTemperature) as airTemperature, max(trackTemperature) as trackTemperature from `f1-telemetry-app.dashboard_data.session`, unnest(header) as header group by sessionUID) as def_ses2 using(sessionUID)
order by publish_time desc
'''