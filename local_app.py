import configparser
import json

config = configparser.ConfigParser()
config.read(".streamlit/secrets.toml")

file_json = ".streamlit/google-bigquery-fin-viz.json"
bigquery_key_json = json.loads(file_json, strict=False)
print(bigquery_key_json)