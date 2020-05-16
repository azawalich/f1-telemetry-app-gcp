# run from main repo path
FROM google/cloud-sdk
COPY ./compute-engine/launcher/ /compute-engine-launcher
COPY secrets.sh /compute-engine-launcher/secrets.sh
RUN pip3 install -r /compute-engine-launcher/requirements.txt
WORKDIR /compute-engine-launcher
ENTRYPOINT ["python3.7"]
CMD ["launch.py"]