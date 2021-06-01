import requests
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime
from elasticsearch import helpers
from es_connect import connect_elasticsearch
import math
import sys
from zerto_helper import logs, u_logs


def login(session_url, zvm_user, zvm_password):
    print("Getting ZVM API token...")
    auth_info = "{\r\n\t\"AuthenticationMethod\":1\r\n}"
    headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
            }
    response = requests.post(session_url, headers=headers, data=auth_info, verify=False, auth=HTTPBasicAuth(zvm_user, zvm_password))
    if response.ok:
        auth_token = response.headers['x-zerto-session']
        return auth_token
if len(sys.argv) >= 2:
    url = sys.argv[1]
else:
    sys.exit("Missing DataDomain Host Information.  Exiting program.")


# Declaring Environment variables
if url == 'IP1':
    zvm_ip = 'Cluster2'
else:
    zvm_ip = 'Cluster1'
zvm_u = u_logs()
zvm_p = logs()
base_url = "https://"+zvm_ip+":9669/v1"
session = base_url+"/session/add"

returned_token = login(session, zvm_u, zvm_p)

# url = 'IP1'
# url = 'IP2'

payload = {}
headers = {
  'x-zerto-session': returned_token,
  'Authorization': f'Bearer {returned_token}'
}

response = requests.request("GET", f'https://{url}:9669/v1/vpgs', headers=headers, data=payload, verify=False)
data = json.loads(response.text)
data_list = []
count = 0
timestamp = datetime.today().strftime('%s')

for info in data:
    x = {'vpg_name': data[count]['VpgName'], 'vm_count': data[count]['VmsCount'], 'config_rpo_hours': math.trunc(data[count]['ConfiguredRpoSeconds'] / 60 / 60),
         'actual_rpo_seconds': data[count]['ActualRPO'], 'timestamp': timestamp}
    count += 1
    data_list.append(x)

es = connect_elasticsearch()
resp = helpers.bulk(es, data_list, index='zerto')
