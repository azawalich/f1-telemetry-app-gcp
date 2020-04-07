# run from main repo path
FROM python:3.7
COPY container-registry/ /telemetry-app
COPY secrets.py /telemetry-app/secrets.py
RUN pip install -r telemetry-app/requirements.txt
RUN pip install --upgrade google-cloud-pubsub
WORKDIR /telemetry-app
ENTRYPOINT ["python3.7"]
CMD ["publish.py"]