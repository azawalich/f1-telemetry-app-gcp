# run from main repo path
FROM python:3.7
COPY container-registry/ /telemetry-app
COPY dataflow/runner.py /telemetry-app/runner.py
COPY secrets.py /telemetry-app/secrets.py
COPY secrets.sh /telemetry-app/secrets.sh
RUN pip install -r telemetry-app/requirements.txt
RUN pip install --upgrade google-cloud-pubsub
WORKDIR /telemetry-app
ENTRYPOINT ["python3.7"]
CMD ["publish.py"]