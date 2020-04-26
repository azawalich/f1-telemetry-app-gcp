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

#useful
#tmux new -s sessionName
#tmux list-sessions
#tmux attach-session -t sessionName
#tmux kill-session -t sessionName
# click ctrl+b and d - exit session

#get docker-compose.yml to a machine
gcloud compute scp f1-telemetry-app-gcp/compute-engine/docker-compose.yml f1-telemetry-app-vm:~

# install docker-compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.25.4/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
docker-compose pull
docker-compose up
# sometimes docker image prune

# set proper timezone
sudo timedatectl set-timezone Europe/Warsaw
sudo timedatectl set-ntp on

logout