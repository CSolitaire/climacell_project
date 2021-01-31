import pandas as pd
import numpy as np
import requests
import os
from env import key, client
import scipy as sp 
import datetime
from datetime import date
from datetime import datetime, timedelta
from dateutil.relativedelta import *
#################### Acquire ##################

def api_get():
    stop = datetime.now().isoformat()
    start = (datetime.now() - relativedelta(days=1)).isoformat()

    url = "https://api.climacell.co/v3/weather/historical/station"

    querystring = {'lat': 30, 
               'lon': 40,
               "unit_system":"si",
               "apikey": key,
               "start_time": start, 
               "end_time": stop,
               "fields":['temp','wind_speed','precipitation','baro_pressure'],
              }

    response = requests.request("GET", url, params=querystring)
    data = response.json()
    df = pd.DataFrame(data)
    return df

def data_clean(df):
    '''
    Function modifies the colums to make sense
    '''
    #Modify Temp
    df['temp'] = df['temp'].astype(str)
    df['temp_degree_c']=df['temp'].str.extract(r'(\d+)')
    df.drop(columns =['temp'], inplace = True)
    df['temp_degree_c']=df['temp_degree_c'].fillna(0)
    df['temp_degree_c']=df['temp_degree_c'].astype(int)
    #Modify Baro-Pressure
    df['baro_pressure'] = df['baro_pressure'].astype(str)
    df['baro_pressure_hPa']=df['baro_pressure'].str.extract(r'(\d+)')
    df.drop(columns =['baro_pressure'], inplace = True)
    df['baro_pressure_hPa']=df['baro_pressure_hPa'].fillna(0)
    df['baro_pressure_hPa']=df['baro_pressure_hPa'].astype(int)
    #Modify Wind_speed
    df['wind_speed'] = df['wind_speed'].astype(str)
    df['wind_speed_m/s']=df['wind_speed'].str.extract(r'(\d+)')
    df.drop(columns =['wind_speed'], inplace = True)
    df['wind_speed_m/s']=df['wind_speed_m/s'].fillna(0)
    df['wind_speed_m/s']=df['wind_speed_m/s'].astype(int)
    #Modify preciatation
    df['precipitation'] = df['precipitation'].astype(str)
    df['precipitation_mm/hr']=df['precipitation'].str.extract(r'(\d+)')
    df.drop(columns =['precipitation'], inplace = True)
    df['precipitation_mm/hr'] = df['precipitation_mm/hr'].fillna(0)
    df['precipitation_mm/hr']=df['precipitation_mm/hr'].astype(int)
    #Modify observation_time
    df['observation_time']= df['observation_time'].astype(str)
    df['observation_time']= df['observation_time'].str.extract(r'(\d+.\d+.\d+\d+.\d+.\d+)')
    df['observation_time']= df['observation_time'].str.replace('T',' ')
    #df['observation_time']= pd.to_datetime(df['observation_time'])
    return df

def get_climacell_data(cached = False):
    '''
    This function reads in climate data from API if cached == False 
    or if cached == True reads in zillow df from a csv file, returns df
    '''
    if cached or os.path.isfile('climate_data.csv') == False:
        # obtain new data
        df = api_get()
        # clean new data
        df = data_clean(df)
        # obtain previous data
        df_old = pd.read_csv('climate_data.csv', index_col=0)
        # add new data on to previous data
        merge = [df_old, df]
        df = pd.concat(merge)
        df.reset_index(drop = True)
        # drop column added by concat
        #df.drop(columns =['Unnamed: 0'], inplace = True)
        # convert observation to date time
        #df['observation_time'] =  pd.to_datetime(df['observation_time'])
        # Filtering out any repeated time observations 
        df.sort_values("observation_time", inplace = True) 
        # dropping any duplicte values 
        df.drop_duplicates(subset ="observation_time", 
                     keep = False, inplace = True) 
        # write new data to csv
        df.to_csv('climate_data.csv')
        # return new data frame
    else:
        df = pd.read_csv('climate_data.csv', index_col=0)
    return df

def formatt_data(df):
    # convert observation to date time
    df['observation_time'] =  pd.to_datetime(df['observation_time'])
    # add daily column to data frame
    df['day'] = df.observation_time.dt.day
    # add month column to data frame
    df['month'] = df.observation_time.dt.month
    # set date as index
    df = df.set_index('observation_time').sort_index()
    return df
