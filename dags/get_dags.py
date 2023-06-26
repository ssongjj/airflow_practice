import requests
from requests.auth import HTTPBasicAuth

url = "http://localhost:8080/api/v1/dags"

response = requests.get(url, auth=HTTPBasicAuth("airflow", "airflow"))
dags = response.json()

for dag in dags['dags']:
    if not dag['is_paused']:
        print(dag['dag_id'])