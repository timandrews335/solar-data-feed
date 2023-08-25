import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago

import pandas as pd
import pymssql
import psycopg2                         #Need to ass this to docker
import os
from sqlalchemy import create_engine
from sqlalchemy import text
import tempfile
import requests
import csv
import sys
from datetime import date, timedelta, time, datetime
from sys import platform
import datetime


default_args = {
                'owner': 'airflow',
                #'start_date': airflow.utils.dates.days_ago(2),
                # 'end_date': datetime(),
                # 'depends_on_past': False,
                #'email': ['andrews.lauren@protonmail.com'],
                #'email_on_failure': False,
                #'email_on_retry': False,
                # If a task fails, retry it once after waiting
                # at least 5 minutes
                #'retries': 1,
                'retry_delay': timedelta(minutes=5),
        }


POSTGRES_IP = os.environ['POSTGRES_SERVER']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRESS_PASSWORD = os.environ['POSTGRES_PASS']

#Set up the destionation file based on OS & temp directory
solar_csv = ""
solar_csv_power = ""
if platform == "linux" or platform == "linux2":
    solar_csv_energy = tempfile.gettempdir() + "/solar_energy.csv"
    solar_csv_power = tempfile.gettempdir() + "/solar_power.csv"
elif platform == "darwin":
    solar_csv_energy = tempfile.gettempdir() + "/solar_energy.csv"
    solar_csv_power = tempfile.gettempdir() + "/solar_power.csv"
elif platform == "win32": 
    solar_csv_energy = tempfile.gettempdir() + "\\solar_energy.csv"
    solar_csv_power = tempfile.gettempdir() + "\\solar_power.csv"


def import_solar():
    SOLAR_SITE_ID = os.environ["SOLAR_SITE_ID"]
    SOLAR_API_KEY = os.environ["SOLAR_API_KEY"]

    #For the solar api energy, we want to get a rolling 30 days
    today = date.today()
    start_date_energy = today + timedelta(days=-30)

    #For the solar api power, we want to get a rolling 26 days
    start_date_power = today + timedelta(days=-26)
    end_date_power = today + timedelta(days=+1)

    #Format our dates as strings to pass to the URL
    today_string = today.strftime("%Y-%m-%d")
    start_date_energy_string = start_date_energy.strftime("%Y-%m-%d")
    start_date_power_string = start_date_power.strftime("%Y-%m-%d") + " 00:00:00"
    end_date_power_string = end_date_power.strftime("%Y-%m-%d") + " 11:59:59"

    #Build our URLs dynamically and download
    solar_energy_url = "https://monitoringapi.solaredge.com/site/" + SOLAR_SITE_ID + "/energy?timeUnit=QUARTER_OF_AN_HOUR&endDate=" + today_string + "&startDate=" + start_date_energy_string + "&format=csv&api_key=" + SOLAR_API_KEY
    solar_url_power = "https://monitoringapi.solaredge.com/site/" + SOLAR_SITE_ID + "/power?endTime=" + end_date_power_string + "&startTime=" + start_date_power_string + "&format=csv&api_key=" + SOLAR_API_KEY


    with requests.Session() as s:
        download = s.get(solar_energy_url)
        with open(solar_csv_energy, "w", newline='') as text_file:
            text_file.write(download.content.decode('utf-8'))

    with requests.Session() as s2:
        download = s2.get(solar_url_power)
        with open(solar_csv_power, "w", newline='') as text_file:
            text_file.write(download.content.decode('utf-8'))
            
            
def process_csv_to_db(csv, db_con, table_name):

    #For the solar api energy, we want to get a rolling 30 days
    today = date.today()
    start_date_energy = today + timedelta(days=-30)

    #For the solar api power, we want to get a rolling 26 days
    start_date_power = today + timedelta(days=-26)
    end_date_power = today + timedelta(days=+1)

    #Format our dates as strings to pass to the URL
    today_string = today.strftime("%Y-%m-%d")
    start_date_energy_string = start_date_energy.strftime("%Y-%m-%d")
    start_date_power_string = start_date_power.strftime("%Y-%m-%d") + " 00:00:00"
    end_date_power_string = end_date_power.strftime("%Y-%m-%d") + " 11:59:59"
        
    #Save csv to database
    df = pd.read_csv(csv)
    df.to_sql(name = table_name, con = db_con, if_exists='replace', index = False)

def process_csv_to_db_postgres():
    connection_string = "postgresql+psycopg2://" + POSTGRES_USER + ":" + POSTGRESS_PASSWORD + "@" + POSTGRES_IP + "/solar"
    engine = create_engine(connection_string)
        
    process_csv_to_db(solar_csv_energy, engine, 'raw_solar_energy')
    process_csv_to_db(solar_csv_power, engine, 'raw_solar_power')

dag_solar_data_feed = DAG(
        dag_id = "solar_data_feed",
        default_args=default_args,
        # schedule_interval='0 0 * * *',
        schedule_interval='@once',
        dagrun_timeout=timedelta(minutes=60),
        description='imports solar panel power generation, weather information, and stores it locally and in the cloud for a predictive model.',
        start_date = airflow.utils.dates.days_ago(1))

task_dummy = DummyOperator(task_id='dummy_task', retries=3, dag=dag_solar_data_feed)
task_import_solar = PythonOperator(task_id='task_import_solar', python_callable=import_solar, dag=dag_solar_data_feed)
task_solar_to_postgres = PythonOperator(task_id='task_solar_to_postgres', python_callable=process_csv_to_db_postgres, dag=dag_solar_data_feed)

task_dummy >> task_import_solar >> task_solar_to_postgres

if __name__ == "__main__":
        dag_python.cli()