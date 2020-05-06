cd ../../f1-telemetry-app-gcp
gcloud auth configure-docker

docker build --file ./dataflow/Dockerfile -t dataflow . --no-cache
docker tag dataflow gcr.io/f1-telemetry-app/dataflow
docker push gcr.io/f1-telemetry-app/dataflow

docker build --file ./pub-sub/Dockerfile -t pub-sub . --no-cache
docker tag pub-sub gcr.io/f1-telemetry-app/pub-sub
docker push gcr.io/f1-telemetry-app/pub-sub