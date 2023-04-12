# Regularly Polls Purpleair api for outdoor sensor data for sensors within a deined rectangular geographic region at a defined interval.
# Appends data to Google Sheets
# James S. Lucas - 20230411

import sys
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import pandas as pd
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from datetime import datetime
from time import sleep
import logging
import config

logging.basicConfig(filename='error.log')

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.headers.update({'X-API-Key': config.PURPLEAIR_READ_KEY})
session.mount('http://', adapter)
session.mount('https://', adapter)

interval_duration = 1200
write_csv = False
file_name = 'pa_log_test.csv'
if sys.platform == 'win32':
    output_pathname = os.path.join(config.matrix5, file_name)
elif sys.platform == 'linux':
    cwd = os.getcwd()
    output_pathname = os.path.join(cwd, file_name)

# set the name of the Google Sheets document
document_name = 'pa_data'

# set the name of the worksheet in the Google Sheets document
worksheet_name = 'TV'

# set the credentials for the Google Sheets service account
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(config.gspread_service_account_json_path, scope)
client = gspread.authorize(creds)

# open the Google Sheets worksheet
sheet = client.open(document_name).worksheet(worksheet_name)

root_url = 'https://api.purpleair.com/v1/sensors/?fields={fields}&max_age=0&location_type=0&nwlng={nwlng}&nwlat={nwlat}&selng={selng}&selat={selat}'

bbox = config.bbox

params = {
    'fields': "name,latitude,longitude,altitude,rssi,uptime,humidity,temperature,pressure,voc,"
              "pm1.0_atm_a,pm1.0_atm_b,pm2.5_atm_a,pm2.5_atm_b,pm10.0_atm_a,pm10.0_atm_b,"
              "pm1.0_cf_1_a,pm1.0_cf_1_b,pm2.5_cf_1_a,pm2.5_cf_1_b,pm10.0_cf_1_a,pm10.0_cf_1_b,"
              "0.3_um_count,0.5_um_count,1.0_um_count,2.5_um_count,5.0_um_count,10.0_um_count",
    'nwlng': bbox[0],
    'selat': bbox[1],
    'selng': bbox[2],
    'nwlat': bbox[3]
}
url = root_url.format(**params)

cols = params['fields'].split(',')
cols.insert(0, 'sensor_index')
cols.insert(0, 'time_stamp')


def get_data(url, cols):
    try:
        response = session.get(url)
    except Exception as e:
        print(e)
        logging.exception("get_data error:\n%s" % e)
    url_data = response.content
    json_data = json.loads(url_data)
    df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
    df['time_stamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # convert the float values to strings
    df['latitude'] = df['latitude'].astype(str)
    df['longitude'] = df['longitude'].astype(str)
    #df['humidity'] = df['humidity'].astype(str)
    #df['pm1.0_atm_a'] = df['pm1.0_atm_a'].astype(str)
    #df['pm1.0_atm_b'] = df['pm1.0_atm_b'].astype(str)
    #df['pm2.5_atm_a'] = df['pm2.5_atm_a'].astype(str)
    #df['pm2.5_atm_b'] = df['pm2.5_atm_b'].astype(str)
    #df['time_stamp'] = df['time_stamp'].astype(str)

    df = df[cols]
    print(df)
    print(" ")
    return df

def write_data(df, write_csv):
    # append the data to Google Sheets 
    try:
        sheet.append_rows(df.values.tolist())
        if write_csv:
            df.to_csv(output_pathname, index=True, header=True)
    except Exception as e:
        print(e)
        logging.exception("write_data error:\n%s" % e)

df = get_data(url, cols)
write_data(df, write_csv)

interval_start = datetime.now()
while True:
    try:
        sleep(1)
        interval_td = datetime.now() - interval_start
        if interval_td.total_seconds() >= interval_duration:
            df = get_data(url, cols)
            write_data(df, write_csv)
            interval_start = datetime.now()
    except KeyboardInterrupt:
        sys.exit()