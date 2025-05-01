from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
import requests
import re
import logging
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

log_counts_dataset = Dataset("log_counts")

def fetch_log_file():
    url = "https://s3.amazonaws.com/ds2002-resources/data/messy.log"
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def clean_log_file(**context):
    log_content = context['ti'].xcom_pull(task_ids='fetch_log_file')
    lines = [line for line in log_content.split('\n') if line.strip() and not re.match(r'^\s*\d+\s*$', line)]
    cleaned_lines = [re.sub(r':\.+', '', line) for line in lines]
    return '\n'.join(cleaned_lines)

def count_log_types(**context):
    cleaned_content = context['ti'].xcom_pull(task_ids='clean_log_file')
    lines = cleaned_content.split('\n')
    info_count = sum(1 for line in lines if 'INFO' in line)
    trace_count = sum(1 for line in lines if 'TRACE' in line)
    event_count = sum(1 for line in lines if 'EVENT' in line)
    proterr_count = sum(1 for line in lines if 'PROTERR' in line)
    logging.info(f"INFO lines: {info_count}")
    logging.info(f"TRACE lines: {trace_count}")
    logging.info(f"EVENT lines: {event_count}")
    logging.info(f"PROTERR lines
