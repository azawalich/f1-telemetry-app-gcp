cd ../../f1-telemetry-app-gcp
docker build -t f1-telemetry-app-gcp -f container-registry/Dockerfile . --no-cache
docker tag f1-telemetry-app-gcp gcr.io/f1-telemetry-app/f1-telemetry-app-gcp
gcloud auth configure-docker
docker push gcr.io/f1-telemetry-app/f1-telemetry-app-gcp