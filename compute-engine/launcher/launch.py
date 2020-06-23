from flask import Flask, Response, request
import subprocess
import re
import time
import requests

secrets = {}

f = open('secrets.sh', 'r')
lines_read = f.read().splitlines()[1:]
f.close()

for line in lines_read:
    line_splitted = line.replace('\n', '').replace('"', '').split('=')
    secrets[line_splitted[0]] = line_splitted[1]

app = Flask(__name__)

def _check_token_auth(auth_token):
    """This function is called to check if a submitted token argument matches the expected token """
    return auth_token == secrets['webhook_auth_token']

def _get_instances_list(status):
    jobs_list = []
    string = subprocess.check_output('gcloud compute instances list', shell=True).decode()
    string_clean = re.sub(' +', ' ', string).strip().split('\n')

    for single_job in [s for s in string_clean if status in s]:
        jobs_list.append(single_job.split(' ')[0])

    if status == 'RUNNING':
        jobs_list.remove(secrets['vm_launcher_name'])
    
    return jobs_list

@app.route('/')
def hello():
    return "Hello World!"

@app.route('/worker-on', methods=['POST'])
def token_auth_handler_on():
    auth_token = request.form['auth_token']
    if not auth_token or not _check_token_auth(auth_token):
        error_msg = '403 Please pass the correct authentication token'
        return Response(error_msg, 403)
    else:
        stopped_jobs = _get_instances_list("TERMINATED")
        if len(stopped_jobs) > 0:
            for single_job in stopped_jobs:
                subprocess.Popen('gcloud compute instances start {}'.format(single_job), shell=True)
                print("Started Compute Engine VMs: {}".format(single_job))
        
        time.sleep(40)

        # let's convert the data to JSONs
        data = {
            'auth_token': auth_token
            } 
        
        webhook_url = 'http://{}:5000/dataflow-on?auth-token={}'.format(
            secrets['vm_worker_external_ip'], secrets['webhook_auth_token'])
        
        requests.post(url = webhook_url, data = data)

        return Response("Starting VMs and PubSub job completed.")

    

@app.route('/worker-off', methods=['POST'])
def token_auth_handler_off():
    auth_token = request.form['auth_token']
    if not auth_token or not _check_token_auth(auth_token):
        error_msg = '403 Please pass the correct authentication token'
        return Response(error_msg, 403)
    else:
        running_jobs = _get_instances_list("RUNNING")
        if len(running_jobs) > 0:
            for single_job in running_jobs:
                subprocess.Popen('gcloud compute instances stop {}'.format(single_job), shell=True)
                print("Stopped Compute Engine VMs: {}".format(single_job))
        
        return Response("Stopping VMs completed.")


if __name__ == '__main__':
    app.run('0.0.0.0', port=5000)