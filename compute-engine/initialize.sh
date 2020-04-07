gcloud beta compute --project=f1-telemetry-app instances create f1-telemetry-app-vm --zone=europe-west3-a --machine-type=f1-micro --subnet=default --network-tier=PREMIUM --maintenance-policy=MIGRATE --service-account=237255567678-compute@developer.gserviceaccount.com --scopes=https://www.googleapis.com/auth/cloud-platform --tags=http-server,https-server --image=ubuntu-1910-eoan-v20200331 --image-project=ubuntu-os-cloud --boot-disk-size=10GB --boot-disk-type=pd-standard --boot-disk-device-name=f1-telemetry-app-vm --no-shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring --reservation-affinity=any

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
docker run -it -p 0.0.0.0:5005:5005 -p 0.0.0.0:5005:5005/udp gcr.io/f1-telemetry-app/f1-telemetry-app-gcp