import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago

import pandas as pd
import pymssql
import psycopg2                         
import os
from sqlalchemy import create_engine
from sqlalchemy import text
from google.cloud import bigquery
import tempfile
import requests
import csv
import sys
from datetime import date, timedelta, time, datetime
from sys import platform
import datetime
from enum import Enum


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
BQ_CLIENT = bigquery.Client()   # relies on os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
ACCUWEATHER_5DAY_URL = os.environ['ACCUWEATHER_5DAY_URL']
OPENWEATHER_URL = os.environ['OPENWEATHER_URL']

class DB_TYPE(Enum):
    POSTGRES = 1
    BIGQUERY = 2

#Set up the destionation files based on OS & temp directory
solar_csv = ""
solar_csv_power = ""
fiveday_csv = ""
openweather_csv = ""
if platform == "linux" or platform == "linux2":
    solar_csv_energy = tempfile.gettempdir() + "/solar_energy.csv"
    solar_csv_power = tempfile.gettempdir() + "/solar_power.csv"
    fiveday_csv = tempfile.gettempdir() + "/accuweather_five_day.csv"
    openweather_csv = tempfile.gettempdir() + "/openweather.csv"
elif platform == "darwin":
    solar_csv_energy = tempfile.gettempdir() + "/solar_energy.csv"
    solar_csv_power = tempfile.gettempdir() + "/solar_power.csv"
    fiveday_csv = tempfile.gettempdir() + "/accuweather_five_day.csv"
    openweather_csv = tempfile.gettempdir() + "/openweather.csv"
elif platform == "win32": 
    solar_csv_energy = tempfile.gettempdir() + "\\solar_energy.csv"
    solar_csv_power = tempfile.gettempdir() + "\\solar_power.csv"
    fiveday_csv = tempfile.gettempdir() + "\\accuweather_five_day.csv"
    openweather_csv = tempfile.gettempdir() + "\\openweather.csv"
    
#Connections to target DBs 
POSTGRES_CONNECTION_STRING = "postgresql+psycopg2://" + POSTGRES_USER + ":" + POSTGRESS_PASSWORD + "@" + POSTGRES_IP + "/solar"
POSTGRES_ENGINE = create_engine(POSTGRES_CONNECTION_STRING)


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
            
def import_five_day():
    response = requests.get(ACCUWEATHER_5DAY_URL)
    data = response.json()
    csv_results_string = '"the_date","min_temp","max_temp","day_phrase","day_precip","day_precip_type","day_precip_intensity"\n'

    for day in data['DailyForecasts']:
        the_date = day['Date']
        temps = day['Temperature']
        min_temp = temps['Minimum']['Value']
        max_temp = temps['Maximum']['Value']
        day_phrase = day['Day']['IconPhrase']
        day_precip = day['Day']['HasPrecipitation']
        day_precip_type = ""
        day_precip_intensity = ""

        try:
            day_precip_type = day['Day']['PrecipitationType']
            day_precip_intensity = day['Day']['PrecipitationIntensity']
        except:
            day_precip_type = ""
            day_precip_intensity = ""
        
        csv_results_string = csv_results_string + '"' + str(the_date) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(min_temp) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(max_temp) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(day_phrase) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(day_precip) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(day_precip_type) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(day_precip_intensity) + '"'
        csv_results_string = csv_results_string + '\n'

    with open(fiveday_csv, "w", newline='') as text_file:
        text_file.write(csv_results_string)
        
def import_openweather():
    response = requests.get(OPENWEATHER_URL)
    data = response.json()
    timezone = data['timezone']
    timezone_offset = data['timezone_offset']

    csv_results_string = '"timezone","timezone_offset","forecast_date","sunrise","sunset","weather_main","weather_description","min_temp","max_temp","cloud_pct","rain","snow"\n'

    for day in data['daily']:
        day_name = datetime.datetime.utcfromtimestamp(day['dt'] + timezone_offset)  #Unix UTC
        sunrise = datetime.datetime.utcfromtimestamp(day['sunrise'] + timezone_offset) #Unix UTC
        sunset = datetime.datetime.utcfromtimestamp(day['sunset'] + timezone_offset)  #Unix UTC
        weather = day['weather']
        temp = day['temp']
        min = (1.8 * (temp['min'] - 273.15)) + 32  #kelvin to F
        max = (1.8 * (temp['max'] - 273.15)) + 32  #kelvin to F
        clouds = day['clouds'] #pct
        rain = 0.0      #mm
        snow = 0.0
        try:
            rain = day['rain']
        except:
            rain = 0.0

        try:
            snow = day['snow']
        except:
            snow = 0.0

        for specific_weather in weather:
            main_weather = specific_weather['main']
            description_weather = specific_weather['description']
        
        csv_results_string = csv_results_string + '"' + str(timezone) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(timezone_offset) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(day_name) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(sunrise) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(sunset) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(main_weather) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(description_weather) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(min) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(max) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(clouds) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(rain) + '"' + ','
        csv_results_string = csv_results_string + '"' + str(snow) + '"' 
        csv_results_string = csv_results_string + '\n'

    with open(openweather_csv, "w", newline='') as text_file:
        text_file.write(csv_results_string)
                
            
def process_csv_to_db(csv, db_con, db_type, table_name):
    df = pd.read_csv(csv)
        
    if(db_type == DB_TYPE.POSTGRES):
        df.to_sql(name = table_name, con = db_con, if_exists='replace', index = False)
    if(db_type == DB_TYPE.BIGQUERY):
        bq_table_id = table_name
        bq_job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        bq_job = BQ_CLIENT.load_table_from_dataframe(df, bq_table_id, job_config=bq_job_config)
        bq_job.result()
        
def run_sql_against_db(db_con, db_type, sql):

    if(db_type == DB_TYPE.POSTGRES):
        cur = db_con.cursor()
        cur.execute(sql)
        db_con.commit()
    if (db_type == DB_TYPE.BIGQUERY):
        BQ_CLIENT.query(sql)

#****************************************************************
#****************************************************************
#*****************Methods that are use as dag tasks**************
#****************************************************************
#****************************************************************
def process_solar_csv_to_db_postgres():
    process_csv_to_db(solar_csv_energy, POSTGRES_ENGINE, DB_TYPE.POSTGRES, 'raw_solar_energy')
    process_csv_to_db(solar_csv_power, POSTGRES_ENGINE, DB_TYPE.POSTGRES, 'raw_solar_power')
    
def process_solar_csv_to_db_bigquery():
    process_csv_to_db(solar_csv_energy, None, DB_TYPE.BIGQUERY, 'solar.raw_solar_energy')
    process_csv_to_db(solar_csv_power, None, DB_TYPE.BIGQUERY, 'solar.raw_solar_power')
    
def process_five_day_csv_to_db_postgres():
    process_csv_to_db(fiveday_csv, POSTGRES_ENGINE, DB_TYPE.POSTGRES, 'raw_five_day')
    
def process_five_day_csv_to_db_bigquery():
    process_csv_to_db(fiveday_csv, None, DB_TYPE.BIGQUERY, 'solar.raw_five_day')
    
def process_openweather_csv_to_db_postgres():
    process_csv_to_db(openweather_csv, POSTGRES_ENGINE, DB_TYPE.POSTGRES, 'raw_openweather')
    
def process_openweather_csv_to_db_bigquery():
    process_csv_to_db(openweather_csv, None, DB_TYPE.BIGQUERY, 'solar.raw_openweather')
    
def upsert_target_solar_postgres():
    conn = psycopg2.connect("user='" + POSTGRES_USER + "' host='" + POSTGRES_IP + "' password='" + POSTGRESS_PASSWORD + "'")
    run_sql_against_db(conn,DB_TYPE.POSTGRES, 'INSERT INTO solar_values ("date", "value") select "date"::timestamp, "value" from raw_solar_energy ON CONFLICT("date") DO UPDATE SET "value" = excluded."value";')

def insert_target_solar_bigquery():
    sql = """
    insert into solar.solar_production
    select cast(`date` as datetime) as production_time, value as production, current_timestamp() as time_inserted 
    from solar.raw_solar_energy
    where value is not null
    and cast(`date` as datetime) >= (select datetime_add(max(production_time), interval -4 HOUR) from solar.solar_production)
    ;
    """
    run_sql_against_db(None,DB_TYPE.BIGQUERY, sql)
 
def upsert_target_five_day_postgres():
    conn = psycopg2.connect("user='" + POSTGRES_USER + "' host='" + POSTGRES_IP + "' password='" + POSTGRESS_PASSWORD + "'")
    #Accuweather - to target tables 
    sql = """
    insert into forecast_accu
    (time_recorded, the_date, min_temp, max_temp, day_phrase, day_precip, day_precip_type, day_precip_intensity)
    select 
    localtimestamp as time_recorded
    , the_date::date as the_date
    , min_temp::float as min_temp
    , max_temp::float as max_temp
    , day_phrase
    , day_precip::bool as day_precip
    , coalesce(day_precip_type::varchar(50), '') as day_precip_type
    , coalesce(day_precip_intensity::varchar(50), '') as day_precip_intensity 
    from raw_five_day
    on conflict (time_recorded, the_date) do update 
    set min_temp = excluded.min_temp,
     max_temp = excluded.max_temp,
     day_phrase = excluded.day_phrase,
     day_precip = excluded.day_precip,
     day_precip_type = excluded.day_precip_type,
     day_precip_intensity = excluded.day_precip_intensity
    ;
    """
    run_sql_against_db(conn,DB_TYPE.POSTGRES, sql)
    
def insert_target_five_day_bigquery():
    sql = """
    insert into solar.accuweather_five_day_forecast
    select 
    current_timestamp() as time_recorded
    , cast(left(the_date, 10) as Date) as the_date
    , min_temp 
    , max_temp 
    , day_phrase
    , day_precip 
    , coalesce(day_precip_type, '') as day_precip_type
    , coalesce(day_precip_intensity, '') as  day_precip_intensity
    from solar.raw_five_day
    ;
    """
    run_sql_against_db(None,DB_TYPE.BIGQUERY, sql)
    
def insert_target_openweather_postgres():
    conn = psycopg2.connect("user='" + POSTGRES_USER + "' host='" + POSTGRES_IP + "' password='" + POSTGRESS_PASSWORD + "'")
    sql = """
    insert into forecast 
    select 
    localtimestamp as time_recorded
    , timezone::varchar(100) as timezone
    , timezone_offset::int as timezone_offset
    , forecast_date::date as forecast_date
    , sunrise::timestamp as sunrise
    , sunset::timestamp as sunset
    , weather_main::varchar(100) as weather_main
    , weather_description::varchar(100) as weather_description
    , min_temp::float as min_temp
    , max_temp::float as max_temp
    , cloud_pct::int as cloud_pct
    , rain::float as rain 
    , snow::float as snow
    from raw_openweather ro 
    ;
    """
    run_sql_against_db(conn,DB_TYPE.POSTGRES, sql)
    
def insert_target_openweather_bigquery():
    sql = """
  insert into solar.openweather_forecast
  select
	current_timestamp() as time_recorded  ,
	timezone  ,
	timezone_offset  ,
	cast(left(forecast_date, 10) as date) ,
	cast(sunrise as Timestamp) as sunrise  ,
	cast(sunset as Timestamp) as sunset ,
  weather_main,
	weather_description  ,
	min_temp  ,
	max_temp  ,
	cloud_pct  ,
	rain  ,
	snow  
  from solar.raw_openweather
;
    """
    run_sql_against_db(None,DB_TYPE.BIGQUERY, sql)
    
def insert_target_combined_weather_bigquery():
    sql = """
insert
	into
	solar.combined_weather
 select
	c.time_recorded,
	c.the_date,
	cast(c.min_temp as int),
	cast(c.max_temp as int),
	c.day_phrase,
	c.day_precip,
	c.day_precip_type,
	c.day_precip_intensity,
	c.sunrise,
	c.sunset,
	c.daylight
from
	(
	select
		a.time_recorded,
		a.the_date,
		a.min_temp,
		a.max_temp,
		a.day_phrase,
		a.day_precip,
		a.day_precip_type,
		a.day_precip_intensity,
		b.sunrise,
		b.sunset,
		date_diff(b.sunset,
		b.sunrise,
		second) / 3600.0 as daylight
	from
		solar.accuweather_five_day_forecast a
	join (
		select
			z.forecast_date,
			min(z.sunrise) as sunrise,
			max(z.sunset) as sunset
		from
			solar.openweather_forecast z
		group by
			z.forecast_date) b on
		b.forecast_date = a.the_date) c
join (
	select
		x.the_date,
		max(time_recorded) as max_time
	from
		solar.accuweather_five_day_forecast x
	group by
		x.the_date) d on
	c.the_date = d.the_date
	and c.time_recorded = d.max_time
;
    """
    run_sql_against_db(None,DB_TYPE.BIGQUERY, sql)
 
dag_solar_data_feed = DAG(
        dag_id = "solar_data_feed",
        default_args=default_args,
        schedule_interval='25 * * * *',
        #schedule_interval='@once',
        dagrun_timeout=timedelta(minutes=60),
        description='imports solar panel power generation, weather information, and stores it locally and in the cloud for a predictive model.',
        start_date = airflow.utils.dates.days_ago(1))

task_dummy = DummyOperator(task_id='dummy_task', retries=3, dag=dag_solar_data_feed)
task_import_solar = PythonOperator(task_id='task_import_solar', python_callable=import_solar, dag=dag_solar_data_feed)
task_import_five_day = PythonOperator(task_id='task_import_five_day', python_callable=import_five_day, dag=dag_solar_data_feed)
task_import_openweather = PythonOperator(task_id='task_import_openweather', python_callable=import_openweather, dag=dag_solar_data_feed)

task_solar_to_postgres = PythonOperator(task_id='task_solar_to_postgres', python_callable=process_solar_csv_to_db_postgres, dag=dag_solar_data_feed)
task_solar_to_bigquery = PythonOperator(task_id='task_solar_to_bigquery', python_callable=process_solar_csv_to_db_bigquery, dag=dag_solar_data_feed)

task_five_day_to_postgres = PythonOperator(task_id='task_five_day_to_postgres', python_callable=process_five_day_csv_to_db_postgres, dag=dag_solar_data_feed)
task_five_day_to_bigquery = PythonOperator(task_id='task_five_day_to_bigquery', python_callable=process_five_day_csv_to_db_bigquery, dag=dag_solar_data_feed)

task_openweather_to_postgres = PythonOperator(task_id='task_openweather_to_postgres', python_callable=process_openweather_csv_to_db_postgres, dag=dag_solar_data_feed)
task_openweather_to_bigquery = PythonOperator(task_id='task_openweather_to_bigquery', python_callable=process_openweather_csv_to_db_bigquery, dag=dag_solar_data_feed)

task_upsert_solar_postgres_target =  PythonOperator(task_id='task_upsert_solar_postgres_target', python_callable=upsert_target_solar_postgres, dag=dag_solar_data_feed)
task_insert_solar_bigquery_target = PythonOperator(task_id='task_insert_solar_bigquery_target', python_callable=insert_target_solar_bigquery, dag=dag_solar_data_feed)

task_upsert_five_day_postgres_target =  PythonOperator(task_id='task_upsert_five_day_postgres_target', python_callable=upsert_target_five_day_postgres, dag=dag_solar_data_feed)
task_insert_five_day_bigquery_target = PythonOperator(task_id='task_insert_five_day_bigquery_target', python_callable=insert_target_five_day_bigqueryg, dag=dag_solar_data_feed)

task_insert_openweather_postgres_target =  PythonOperator(task_id='task_insert_openweather_postgres_target', python_callable=insert_target_openweather_postgres, dag=dag_solar_data_feed)
task_insert_openweather_bigquery_target = PythonOperator(task_id='task_insert_openweather_bigquery_target', python_callable=insert_target_openweather_bigquery, dag=dag_solar_data_feed)
task_insert_combined_weather_bigquery_target = PythonOperator(task_id='task_insert_combined_weather_bigquery_target', python_callable=insert_target_combined_weather_bigquery, dag=dag_solar_data_feed)

task_dummy >> task_import_solar
task_dummy >> task_import_five_day
task_dummy >> task_import_openweather

task_dummy >> task_import_solar >> task_solar_to_postgres >> task_upsert_solar_postgres_target
task_import_solar >> task_solar_to_bigquery >> task_insert_solar_bigquery_target

task_dummy >> task_import_five_day  >> task_five_day_to_postgres >> task_upsert_five_day_postgres_target
task_import_five_day >> task_five_day_to_bigquery >> task_insert_five_day_bigquery_target

task_dummy >> task_import_openweather >> task_openweather_to_postgres >> task_insert_openweather_postgres_target
task_import_openweather >>  task_openweather_to_bigquery >> task_insert_openweather_bigquery_target

task_insert_openweather_bigquery_target >> task_insert_combined_weather_bigquery_target
task_insert_five_day_bigquery_target >> task_insert_combined_weather_bigquery_target

if __name__ == "__main__":
    dag_python.cli()