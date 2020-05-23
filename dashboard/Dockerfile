# run from main repo path
FROM python:3.7
COPY ./dashboard/ /telemetry-app-dashboard
RUN pip3 install -r /telemetry-app-dashboard/requirements.txt
WORKDIR /telemetry-app-dashboard
ENTRYPOINT ["python3.7"]
CMD ["index.py"]