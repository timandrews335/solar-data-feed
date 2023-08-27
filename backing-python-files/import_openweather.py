import tempfile
import requests
import csv
import sys
from datetime import date, timedelta, time, datetime
from sys import platform

import os
import datetime


OPENWEATHER_URL = os.environ['OPENWEATHER_URL']
response = requests.get(OPENWEATHER_URL)
data = response.json()

openweather_csv = ""
if platform == "linux" or platform == "linux2":
    openweather_csv = tempfile.gettempdir() + "/openweather.csv"
elif platform == "darwin":
    openweather_csv = tempfile.gettempdir() + "/openweather.csv"
elif platform == "win32": 
    openweather_csv = tempfile.gettempdir() + "\\openweather.csv"
    
    
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

    print('timezone: ' + timezone)
    print('timezone_offset: ' + str(timezone_offset))
    print('dt: ' + str(day_name))
    print('sunrise: ' + str(sunrise))
    print('sunset: ' + str(sunset))
    print('main weather: ' + main_weather)
    print('weather description: ' + description_weather)
    print('min temp: ' + str(min))
    print('max temp: ' + str(max))
    print('cloud pct: ' + str(clouds))
    print('rain: ' + str(rain))
    print('snow:  ' + str(snow))
    
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