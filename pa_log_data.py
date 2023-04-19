# Regularly Polls Purpleair api for outdoor sensor data for sensors within a deined rectangular geographic region at a defined interval.
# Appends data to Google Sheets
# James S. Lucas - 20230411

import sys
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
#import simplejson as json
import pandas as pd
#import decimal
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

local_interval_duration = 1200
regional_interval_duration = 3600
write_csv = False
file_name = 'pa_log_test.csv'
if sys.platform == 'win32':
    output_pathname = os.path.join(config.matrix5, file_name)
elif sys.platform == 'linux':
    cwd = os.getcwd()
    output_pathname = os.path.join(cwd, file_name)

# set the name of the Google Sheets document
document_name = 'pa_data'

# set the names of the worksheets in the Google Sheets document
local_worksheet_name = 'TV'
regional_keys = ['OC', 'RS', 'CEP']


# set the credentials for the Google Sheets service account
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(config.gspread_service_account_json_path, scope)
client = gspread.authorize(creds)


def get_data(bbox):
    root_url = 'https://api.purpleair.com/v1/sensors/?fields={fields}&max_age=1100&location_type=0&nwlng={nwlng}&nwlat={nwlat}&selng={selng}&selat={selat}'
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
    try:
        response = session.get(url)
    except Exception as e:
        print(e)
        logging.exception("get_data error:\n%s" % e)
        df = pd.DataFrame()
        return df
    if response.ok:
        url_data = response.content
        json_data = json.loads(url_data)
        #for i, row in enumerate(json_data['data']):
            #json_data['data'][i] = [np.nan if x is None else x for x in row]
        #print(json_data)
        #print(url_data)
        #print(" ")
        df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
        df = df.fillna('')
        df['time_stamp'] = datetime.now().strftime('%m/%d/%Y %H:%M:%S')

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
        #print(df)
        #print(" ")
    else:
        df = df=pd.DataFrame()
    return df

def write_data(df, client, document_name, worksheet_name, write_csv):
    # open the Google Sheets worksheet
    sheet = client.open(document_name).worksheet(worksheet_name)
    # append the data to Google Sheets 
    try:
        sheet.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')
        if write_csv:
            df.to_csv(output_pathname, index=True, header=True)
    except Exception as e:
        print(e)
        logging.exception("write_data error:\n%s" % e)


for k, v in config.bbox_dict.items():
    df = get_data(config.bbox_dict.get(k)[0])
    if df.empty:
        print('URL Response Error')
    else:
        write_data(df, client, document_name, config.bbox_dict.get(v)[1], write_csv)

local_interval_start = datetime.now()
regional_interval_start = datetime.now()
while True:
    try:
        sleep(1)
        local_interval_td = datetime.now() - local_interval_start
        regional_interval_td = datetime.now() - regional_interval_start
        if local_interval_td.total_seconds() >= local_interval_duration:
            df = get_data(config.bbox_dict.get("TV")[0])
            if not df.empty:
                write_data(df, client, document_name, local_worksheet_name, write_csv)
            local_interval_start = datetime.now()
        if regional_interval_td.total_seconds() > regional_interval_duration:
            for regional_key in regional_keys:
                df = get_data(config.bbox_dict.get(regional_key)[0]) 
                if not df.empty:
                    write_data(df, client, document_name, config.bbox_dict.get(regional_key)[1], write_csv)
            regional_interval_start = datetime.now()
    except KeyboardInterrupt:
        sys.exit()