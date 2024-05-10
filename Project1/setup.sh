#!/bin/bash

# Create the virtual environment
python3 -m venv env

# Activate the virtual environment and install the required package
source env/bin/activate
pip install google-cloud-storage
pip install google-cloud-pubsub
pip install psycopg2

#python3 publisher.py

#sleep 30
python3 reciever.py

deactivate