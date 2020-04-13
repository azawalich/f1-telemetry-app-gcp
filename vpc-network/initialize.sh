# adding firewall rules
gcloud compute --project=f1-telemetry-app firewall-rules create "vm-allow-flask" --direction=INGRESS --priority=1000 --network=default --action=ALLOW --rules=tcp:5000-5010 --source-ranges=0.0.0.0/0 --target-tags=http-server,https-server
gcloud compute --project=f1-telemetry-app firewall-rules create "vm-allow-udp" --direction=INGRESS --priority=1000 --network=default --action=ALLOW --rules=udp:5005-5006 --source-ranges=0.0.0.0/0 --target-tags=http-server,https-server
