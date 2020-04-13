from flask import Flask
import subprocess

cmd = "gcloud dataflow jobs list"

returned_value = subprocess.call(cmd, shell=True)  # returns the exit code in unix
print('returned value:', returned_value)

app = Flask(__name__)

@app.route('/')
def hello():
    return "Hello World!"

if __name__ == '__main__':
    app.run('0.0.0.0', port=5000)
