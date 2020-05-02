from flask import Flask, Response, request
import subprocess
import time
import re
import datetime
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

def _get_dataflow_jobs_list(status):
    jobs_list = []
    string = subprocess.check_output('gcloud dataflow jobs list', shell=True).decode()
    string_clean = re.sub(' +', ' ', string).strip().split('\n')

    for single_job in [s for s in string_clean if status in s]:
        jobs_list.append(single_job.split(' ')[0])
    
    return jobs_list

def _get_raw_folder_to_convert():
    raw_gcs = 'gs://{}/raw'.format(secrets['bucket_name'])
    clean_gcs = 'gs://{}/clean'.format(secrets['bucket_name'])
    
    string_raw_gcs = subprocess.check_output('gsutil list {}'.format(raw_gcs), shell=True).decode()
    string_raw_gcs_clean = re.sub('{}|/'.format(raw_gcs), '', string_raw_gcs).strip().split('\n')
    raw_folders = string_raw_gcs_clean

    string_clean_gcs = subprocess.check_output('gsutil list {}'.format(clean_gcs), shell=True).decode()
    string_clean_gcs_clean = re.sub('{}|/'.format(clean_gcs), '', string_clean_gcs).strip().split('\n')
    clean_folders = string_clean_gcs_clean

    #assume that there always will be only one
    folders_to_convert = list(set(clean_folders) - set(raw_folders))[0]
   
    return folders_to_convert

@app.route('/')
def hello():
    return "Hello World!"

@app.route('/dataflow-on', methods=['POST'])
def token_auth_handler_on():
    auth_token = request.args.get('auth_token')

    if not auth_token or not _check_token_auth(auth_token):
        error_msg = '403 Please pass the correct authentication token'
        return Response(error_msg, 403)
    else:
        running_jobs = _get_dataflow_jobs_list("Running")
        current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        # Dataflow accepts only names consisting of [a-z0-9-]
        job_name = re.sub('_|:', '-', 'pubsub_to_gcs_raw_{}'.format(current_time))
        if len(running_jobs) == 0:
            open_dataflow_cmd = """python3 pubsub_to_gcs_raw.py \
            --job_name={} \
            --project={} \
            --input_topic=projects/{}/topics/{} \
            --output_path=gs://{}/raw/{}/ \
            --runner=DataflowRunner \
            --window_size=1 \
            --region={} \
            --temp_location=gs://{}/raw/{}/temp
            """.format(
                job_name, secrets['project_name'],secrets['project_name'],
                secrets['topic_name'], secrets['bucket_name'], current_time, 
                secrets['region'], secrets['bucket_name'], current_time)
            
            subprocess.Popen(open_dataflow_cmd, shell=True)
            message="Started a Dataflow job."
        else:
            message='Dataflow job already running: {}'.format(running_jobs)
        
        return Response(message)

@app.route('/dataflow-off', methods=['POST'])
def token_auth_handler_off():
    auth_token = request.args.get('auth_token')
    
    if not auth_token or not _check_token_auth(auth_token):
        error_msg = '403 Please pass the correct authentication token'
        return Response(error_msg, 403)
    else:
        running_jobs = _get_dataflow_jobs_list("Running")
        if len(running_jobs) > 0:
            for single_job in running_jobs:
                subprocess.Popen('gcloud dataflow jobs drain {} --region={}'.format(single_job, secrets['region']), shell=True)
            time.sleep(180)
        
        # let's convert the data to JSONs
        data = {
            'auth_token': request.args.get('auth_token'),
            'raw_folder': _get_raw_folder_to_convert()
            } 
        
        webhook_url = 'http://{}:5000/dataflow-json-conv-on?auth-token={}'.format(
            secrets['vm_external_ip'], secrets['webhook_auth_token'])
        
        requests.post(url = webhook_url, data = data)
        
        return Response("All running Dataflow jobs closed.")

@app.route('/dataflow-json-conv-on', methods=['GET', 'POST'])
def conv_token_auth_handler_on():
    auth_token = request.args.get('auth_token')
    raw_folder = request.form['raw_folder']
    
    if not auth_token or not _check_token_auth(auth_token):
        error_msg = '403 Please pass the correct authentication token'
        return Response(error_msg, 403)
    else:
        # Dataflow accepts only names consisting of [a-z0-9-]
        job_name = re.sub('_|:', '-', 'gcs_raw_to_gcs_clean_{}'.format(raw_folder))
        if len(raw_folder) > 0:
            open_dataflow_cmd = """
            python3 gcs_raw_to_gcs_clean.py \
            --job_name={} \
            --project={} \
            --gcs_input_files=gs://{}/raw/{}/-* \
            --output_path=gs://{}/clean/{}/ \
            --runner=DataflowRunner \
            --region={} \
            --temp_location=gs://{}/clean/{}/temp \
            --requirements_file requirements.txt
            """.format(
                job_name, secrets['project_name'], secrets['bucket_name'], 
                raw_folder, secrets['bucket_name'], raw_folder, secrets['region'], 
                secrets['bucket_name'], raw_folder)
            
            subprocess.Popen(open_dataflow_cmd, shell=True)
            message="Started a Dataflow job."
        else:
            message='No raw_folder such as to convert: {}'.format(raw_folder)
        
        return Response(message)

if __name__ == '__main__':
    app.run('0.0.0.0', port=5000)