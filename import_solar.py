import tempfile
import requests
import csv
import sys
from datetime import date, timedelta, time, datetime
from sys import platform

import os
import datetime


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
    
#Build our URLs dynamically and download
solar_energy_url = "https://monitoringapi.solaredge.com/site/" + SOLAR_SITE_ID + "/energy?timeUnit=QUARTER_OF_AN_HOUR&endDate=" + today_string + "&startDate=" + start_date_energy_string + "&format=csv&api_key=" + SOLAR_API_KEY
solar_url_power = "https://monitoringapi.solaredge.com/site/" + SOLAR_SITE_ID + "/power?endTime=" + end_date_power_string + "&startTime=" + start_date_power_string + "&format=csv&api_key=" + SOLAR_API_KEY


try:
    with requests.Session() as s:
        download = s.get(solar_energy_url)
        with open(solar_csv_energy, "w", newline='') as text_file:
            text_file.write(download.content.decode('utf-8'))

    with requests.Session() as s2:
        download = s2.get(solar_url_power)
        with open(solar_csv_power, "w", newline='') as text_file:
            text_file.write(download.content.decode('utf-8'))
    
except:
    print("Cannot download from api")
    quit()
 