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
    
 
connection_string = "postgresql+psycopg2://" + POSTGRES_USER + ":" + POSTGRESS_PASSWORD + "@" + POSTGRES_IP + "/solar"
engine = create_engine(connection_string)

#Solar - to raw tables
process_csv_to_db(solar_csv_energy, engine, DB_TYPE.POSTGRES, 'raw_solar_energy')
process_csv_to_db(solar_csv_power, engine, DB_TYPE.POSTGRES, 'raw_solar_power')
process_csv_to_db(solar_csv_energy, None, DB_TYPE.BIGQUERY, 'solar.raw_solar_energy')
process_csv_to_db(solar_csv_power, None, DB_TYPE.BIGQUERY, 'solar.raw_solar_power')

#Accuweather - to raw tables
process_csv_to_db(fiveday_csv, engine, DB_TYPE.POSTGRES, 'raw_five_day')
process_csv_to_db(fiveday_csv, None, DB_TYPE.BIGQUERY, 'solar.raw_five_day')

#Openweather - to raw tables
process_csv_to_db(openweather_csv, engine, DB_TYPE.POSTGRES, 'raw_openweather')
process_csv_to_db(openweather_csv, None, DB_TYPE.BIGQUERY, 'solar.raw_openweather')

#Solar - to target tables
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


#Openweather - to target tables 
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