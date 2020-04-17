from flask import Flask, Response, request
import subprocess
import time
import re
import datetime

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
        if len(running_jobs) == 0:
            open_dataflow_cmd = """python3 runner.py \
            --project={} \
            --input_topic=projects/{}/topics/{} \
            --output_path=gs://{}/raw/{} \
            --runner=DataflowRunner \
            --window_size=1 \
            --region={} \
            --temp_location=gs://{}/raw/{}/temp
            """.format(
                secrets['project_name'],secrets['project_name'],secrets['topic_name'],
                secrets['bucket_name'], current_time, secrets['region'], 
                secrets['bucket_name'], current_time)
            
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
        return Response("All running Dataflow jobs closed.")

if __name__ == '__main__':
    app.run('0.0.0.0', port=5000)