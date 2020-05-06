# run from main repo path
FROM python:3.7
COPY ./pub-sub/ /telemetry-app-pub-sub
COPY secrets.sh /telemetry-app-pub-sub/secrets.sh
RUN pip install -r telemetry-app-pub-sub/requirements.txt
WORKDIR /telemetry-app-pub-sub
ENTRYPOINT ["python3.7"]
CMD ["publish.py"]