#python
import requests
from datetime import datetime as dt, timezone
import json
from inmetpy.stations import InmetStation

inmet = InmetStation()

# list all inmet stations available
stations = inmet.get_stations() # get details of all stations available
print(stations)

# get data from all inmet station after a selected date
date = '2021-09-01'
dataAll = inmet.get_all_stations(date) # date in format YYYY-MM-DD"

print(dataAll)

# get data from a station or a list of stations
start_date = '2021-09-07'
end_date = '2021-09-30'
by = 'day'
station_id = ['A422','A360']
dataByStation = inmet.get_data_station(start_date, end_date,by,station_id)
#print(dataByStation)