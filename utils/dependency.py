from dotenv import load_dotenv
load_dotenv()
import os
import requests
import json

def call_internal_services(endpoint,method,body=None):
    if body:
        response = requests.request(method,url=f"{os.getenv('INTERNAL_SERVICE_END_POINT')}{endpoint}",data=json.dumps(body))
    else:
        response = requests.request(method, url=f"{os.getenv('INTERNAL_SERVICE_END_POINT')}{endpoint}")
    return response

