cd ../../f1-telemetry-app-gcp
docker build --file ./container-registry/dataflow/Dockerfile -t dataflow . --no-cache
docker tag dataflow gcr.io/f1-telemetry-app/dataflow
gcloud auth configure-docker
docker push gcr.io/f1-telemetry-app/dataflow
docker build --file ./container-registry/dataflow/Dockerfile -t pubsub . --no-cache
docker tag dataflow gcr.io/f1-telemetry-app/pubsub
docker push gcr.io/f1-telemetry-app/pubsub