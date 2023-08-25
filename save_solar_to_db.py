import pandas as pd
import pymssql
import psycopg2                         #Need to ass this to docker
import os
from sys import platform
import datetime
from datetime import date, timedelta, time, datetime
from sqlalchemy import create_engine
from sqlalchemy import text
import tempfile

#"solar_tracking.py" "1879346" "98B05PGVU5IYBQGWF4W1GY7QG5Z0YPVK" "192.168.40.214" "solar" "threekings%5"

POSTGRES_IP = os.environ['POSTGRES_SERVER']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRESS_PASSWORD = os.environ['POSTGRES_PASS']



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
    df.to_sql(name = table_name, con = engine, if_exists='replace', index = False)
        
    
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
    
process_csv_to_db(solar_csv_energy, engine, 'raw_solar_energy')
process_csv_to_db(solar_csv_power, engine, 'raw_solar_power')