cd ../../f1-telemetry-app-gcp
gcloud auth configure-docker

docker build --file ./dataflow/Dockerfile -t dataflow . --no-cache
docker tag dataflow gcr.io/f1-telemetry-app/dataflow
docker push gcr.io/f1-telemetry-app/dataflow

docker build --file ./pub-sub/Dockerfile -t pub-sub . --no-cache
docker tag pub-sub gcr.io/f1-telemetry-app/pub-sub
docker push gcr.io/f1-telemetry-app/pub-sub

docker build --file ./compute-engine/launcher/Dockerfile -t compute-engine . --no-cache
docker tag compute-engine gcr.io/f1-telemetry-app/compute-engine
docker push gcr.io/f1-telemetry-app/compute-engine

docker build --file ./dashboard/Dockerfile -t dashboard . --no-cache
docker tag dashboard gcr.io/f1-telemetry-app/dashboard
docker push gcr.io/f1-telemetry-app/dashboard