import pandas as pd
import pymssql
import psycopg2                         
from google.cloud import bigquery
import os
from sys import platform
import datetime
from datetime import date, timedelta, time, datetime
from sqlalchemy import create_engine
from sqlalchemy import text
import tempfile
from enum import Enum

#"solar_tracking.py" "1879346" "98B05PGVU5IYBQGWF4W1GY7QG5Z0YPVK" "192.168.40.214" "solar" "threekings%5"

POSTGRES_IP = os.environ['POSTGRES_SERVER']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRESS_PASSWORD = os.environ['POSTGRES_PASS']
BQ_CLIENT = bigquery.Client()   # relies on os.environ["GOOGLE_APPLICATION_CREDENTIALS"]


class DB_TYPE(Enum):
    POSTGRES = 1
    BIGQUERY = 2



def process_csv_to_db(csv, db_con, db_type, table_name):

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
    
    if(db_type == DB_TYPE.POSTGRES):
        df.to_sql(name = table_name, con = engine, if_exists='replace', index = False)
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
    
connection_string = "postgresql+psycopg2://" + POSTGRES_USER + ":" + POSTGRESS_PASSWORD + "@" + POSTGRES_IP + "/solar"
engine = create_engine(connection_string)
    
process_csv_to_db(solar_csv_energy, engine, DB_TYPE.POSTGRES, 'raw_solar_energy')
process_csv_to_db(solar_csv_power, engine, DB_TYPE.POSTGRES, 'raw_solar_power')
process_csv_to_db(solar_csv_energy, engine, DB_TYPE.BIGQUERY, 'solar.raw_solar_energy')
process_csv_to_db(solar_csv_power, engine, DB_TYPE.BIGQUERY, 'solar.raw_solar_power')

conn = psycopg2.connect("user='" + POSTGRES_USER + "' host='" + POSTGRES_IP + "' password='" + POSTGRESS_PASSWORD + "'")
run_sql_against_db(conn,DB_TYPE.POSTGRES, 'INSERT INTO solar_values ("date", "value") select "date"::timestamp, "value" from raw_solar_energy ON CONFLICT("date") DO UPDATE SET "value" = excluded."value";')
run_sql_against_db(conn,DB_TYPE.POSTGRES, 'INSERT INTO solar_power_values ("time_recorded", "watts") select "date"::timestamp, "value" as watts from raw_solar_power ON CONFLICT("time_recorded") DO UPDATE SET "watts" = excluded."watts";')

sql = """
insert into solar.solar_production
select cast(`date` as datetime) as production_time, value as production, current_timestamp() as time_inserted 
from solar.raw_solar_energy
where value is not null
and cast(`date` as datetime) >= (select datetime_add(max(production_time), interval -4 HOUR) from solar.solar_production)
;
"""
run_sql_against_db(None,DB_TYPE.BIGQUERY, sql)