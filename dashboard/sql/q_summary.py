summary_participants = '''
select nationality_val as nationality, name, team, yourTelemetry from (
select * from `f1-telemetry-app.dashboard_data.participant`, unnest(header) as header, unnest(participants) as participants
JOIN (select id as teamId, value as team from `f1-telemetry-app.definitions.team`) as def_team using(teamId)
JOIN (select id as nationality, value as nationality_val from `f1-telemetry-app.definitions.nationality`) as def_nats using(nationality)
) where sessionUID = '$_sessionUID'
'''

summary_laps = '''
select header.sessionTime, header.playerCarIndex, lapData.currentLapNum, lapData.lastLapTime, lapData.pitStatus, lapData.currentLapTime, 
lapData.penalties, lapData.carPosition, lapData.sector  from `f1-telemetry-app.dashboard_data.lap` as t, t.lapData as lapData, 
t.header as header where header.sessionUID = '$_sessionUID' order by header.frameIdentifier
'''

summary_car_status = '''
select header.sessionTime, header.playerCarIndex, carStatusData.tyreVisualCompound 
from `f1-telemetry-app.dashboard_data.car_status` as t, t.carStatusData as carStatusData, t.header as header
where header.sessionUID = '$_sessionUID' order by header.frameIdentifier
'''