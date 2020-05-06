# run from main repo path
FROM google/cloud-sdk
COPY ./dataflow/ /telemetry-app-dataflow
COPY secrets.sh /telemetry-app-dataflow/secrets.sh
RUN pip3 install -r /telemetry-app-dataflow/requirements.txt
WORKDIR /telemetry-app-dataflow
ENTRYPOINT ["python3.7"]
CMD ["launch.py"]