#!/bin/bash
source ../secrets.sh
gsutil mb -c STANDARD -l EUROPE-WEST3 gs://$bucket_name/