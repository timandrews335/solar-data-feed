import tempfile
import requests
import csv
import sys
from datetime import date, timedelta, time, datetime
from sys import platform

import os
import datetime


ACCUWEATHER_5DAY_URL = os.environ['ACCUWEATHER_5DAY_URL']
response = requests.get(ACCUWEATHER_5DAY_URL)
data = response.json()


fiveday_csv = ""
if platform == "linux" or platform == "linux2":
    fiveday_csv = tempfile.gettempdir() + "/accuweather_five_day.csv"
elif platform == "darwin":
    fiveday_csv = tempfile.gettempdir() + "/accuweather_five_day.csv"
elif platform == "win32": 
    fiveday_csv = tempfile.gettempdir() + "\\accuweather_five_day.csv"

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

    print(the_date)
    print(min_temp)
    print(max_temp)
    print(day_phrase)
    print(day_precip)
    print(day_precip_type)
    print(day_precip_intensity)
    print("\n")
    
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