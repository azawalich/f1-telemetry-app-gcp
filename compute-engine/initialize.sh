gcloud beta compute --project=f1-telemetry-app instances create f1-telemetry-app-vm --zone=europe-west3-a --machine-type=g1-small --subnet=default --network-tier=PREMIUM --maintenance-policy=MIGRATE --service-account=237255567678-compute@developer.gserviceaccount.com --scopes=https://www.googleapis.com/auth/cloud-platform --tags=http-server,https-server --image=ubuntu-1910-eoan-v20200331 --image-project=ubuntu-os-cloud --boot-disk-size=10GB --boot-disk-type=pd-standard --boot-disk-device-name=f1-telemetry-app-vm --no-shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring --reservation-affinity=any

#reserve static IP
gcloud compute addresses create vm-static-ip --project=f1-telemetry-app --network-tier=STANDARD --region=europe-west3
gcloud compute instances add-access-config f1-telemetry-app-vm --project=f1-telemetry-app --zone=europe-west3-a --address=IP_OF_THE_NEWLY_CREATED_STATIC_ADDRESS --network-tier=STANDARD

gcloud beta compute ssh --zone "europe-west3-a" "f1-telemetry-app-vm" --project "f1-telemetry-app"

#making gcr.io images work within a VM
sudo su

VERSION=2.0.0
OS=linux  # or "darwin" for OSX, "windows" for Windows.
ARCH=amd64  # or "386" for 32-bit OSs, "arm64" for ARM 64.

curl -fsSL "https://github.com/GoogleCloudPlatform/docker-credential-gcr/releases/download/v${VERSION}/docker-credential-gcr_${OS}_${ARCH}-${VERSION}.tar.gz" \
  | tar xz --to-stdout ./docker-credential-gcr \
  > /usr/bin/docker-credential-gcr && chmod +x /usr/bin/docker-credential-gcr

docker-credential-gcr configure-docker
exit

#after login 
docker pull gcr.io/f1-telemetry-app/f1-telemetry-app-gcp

tmux

docker run -it -p 0.0.0.0:5005:5005 -p 0.0.0.0:5005:5005/udp gcr.io/f1-telemetry-app/f1-telemetry-app-gcp

#click ctrl+b and $; name session "pubsub"; click ctrl+b and d

tmux
docker cp de4f9ae0e43b:/telemetry-app/secrets.sh /home/aleksander_zawalich/
docker cp de4f9ae0e43b:/telemetry-app/runner.py /home/aleksander_zawalich/
source secrets.sh

apt-get install python3-pip
pip3 install apache-beam==2.19.0
pip3 install apache_beam[gcp]
pip3 install apitools

#enable dataflow api

python3 runner.py \
  --project=$project_name \
  --input_topic=projects/$project_name/topics/$topic_name \
  --output_path=gs://$bucket_name/ \
  --runner=DataflowRunner \
  --window_size=1 \
  --temp_location=gs://$bucket_name/temp

#click ctrl+b and $; name session "dataflow"; click ctrl+b and d

#useful
#tmux list-sessions
#tmux attach-session -t sessionName
#tmux kill-session -t sessionName
gcloud compute scp f1-telemetry-app-gcp/compute-engine/docker-compose.yml f1-telemetry-app-vm:~
sudo curl -L "https://github.com/docker/compose/releases/download/1.25.4/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
docker-compose up

logout