# Regularly Polls Purpleair api for outdoor sensor data for sensors within deined rectangular geographic regions at a defined interval.
# Appends data to Google Sheets
# Processes data
# James S. Lucas - 20230419

import sys
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import json
import pandas as pd
from pathlib import Path
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from datetime import datetime
from time import sleep
import logging
from typing import Dict, List
import config

format_string = "%(name)s - %(asctime)s : %(message)s"
logging.basicConfig(filename='error.log',
                    format = format_string)

session = requests.Session()
retry = Retry(connect=5, backoff_factor=1.0)
adapter = HTTPAdapter(max_retries=retry)
session.headers.update({'X-API-Key': config.PURPLEAIR_READ_KEY})
session.mount('http://', adapter)
session.mount('https://', adapter)
file_name: str = 'pa_log_test.csv'
if sys.platform == 'win32':
    output_pathname: str = Path(config.matrix5, file_name)
elif sys.platform == 'linux':
    cwd: str = Path.cwd()
    output_pathname: str = Path(cwd, file_name)
# set the credentials for the Google Sheets service account
scope: List[str] = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive'
                    ]
creds = ServiceAccountCredentials.from_json_keyfile_name(config.gspread_service_account_json_path, scope)
client = gspread.authorize(creds)


def get_data(bbox: List[float]) -> pd.DataFrame:
    root_url: str = 'https://api.purpleair.com/v1/sensors/?fields={fields}&max_age=1100&location_type=0&nwlng={nwlng}&nwlat={nwlat}&selng={selng}&selat={selat}'
    params: Dict[str, str] = {
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
    cols = ['time_stamp', 'sensor_index'] + [col for col in params['fields'].split(',')]
    try:
        response = session.get(url)
    except Exception as e:
        logging.exception("get_data error:\n%s" % e)
        df = pd.DataFrame()
        return df
    if response.ok:
        url_data = response.content
        json_data = json.loads(url_data)
        df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
        df = df.fillna('')
        df['time_stamp'] = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
        # convert the float values to strings
        df['latitude'] = df['latitude'].astype(str)
        df['longitude'] = df['longitude'].astype(str)
        df = df[cols]
    else:
        df = df=pd.DataFrame()
        logging.exception("get_data() response not ok:\n%s")
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
        logging.exception("write_data error:\n%s" % e)


def calc_aqi(PM2_5):
    # Function takes the 24-hour rolling average PM2.5 value and calculates
    # "AQI". "AQI" in quotes as this is not an official methodology. AQI is 
    # 24 hour midnight-midnight average. May change to NowCast or other
    # methodology in the future.
    # Truncate to one decimal place.
    PM2_5 = int(PM2_5 * 10) / 10.0
    if PM2_5 < 0:
        PM2_5 = 0
    #AQI breakpoints [0,    1,     2,    3    ]
    #                [Ilow, Ihigh, Clow, Chigh]
    pm25_aqi = {
        'good': [0, 50, 0, 12],
        'moderate': [51, 100, 12.1, 35.4],
        'sensitive': [101, 150, 35.5, 55.4],
        'unhealthy': [151, 200, 55.5, 150.4],
        'very': [201, 300, 150.5, 250.4],
        'hazardous': [301, 500, 250.5, 500.4],
        'beyond_aqi': [301, 500, 250.5, 500.4]
        }
    try:
        if (0.0 <= PM2_5 <= 12.0):
            aqi_cat = 'good'
        elif (12.1 <= PM2_5 <= 35.4):
            aqi_cat = 'moderate'
        elif (35.5 <= PM2_5 <= 55.4):
            aqi_cat = 'sensitive'
        elif (55.5 <= PM2_5 <= 150.4):
            aqi_cat = 'unhealthy'
        elif (150.5 <= PM2_5 <= 250.4):
            aqi_cat = 'very'
        elif (250.5 <= PM2_5 <= 500.4):
            aqi_cat = 'hazardous'
        elif (PM2_5 >= 500.5):
            aqi_cat = 'beyond_aqi'
        Ihigh = pm25_aqi.get(aqi_cat)[1]
        Ilow = pm25_aqi.get(aqi_cat)[0]
        Chigh = pm25_aqi.get(aqi_cat)[3]
        Clow = pm25_aqi.get(aqi_cat)[2]
        Ipm25 = int(round(
            ((Ihigh - Ilow) / (Chigh - Clow) * (PM2_5 - Clow) + Ilow)
            ))
        return Ipm25
    except Exception as e:
        logging.exception("calc_aqi() error:\n%s" % e)


def calc_epa(PM2_5, RH):
    #0-250 ug/m3 range (>250 may underestimate true PM2.5):
    #PM2.5 (µg/m³) = 0.534 x PA(cf_1) - 0.0844 x RH + 5.604
    #PM2_5_epa = 0.534 * PM2_5 - 0.0844 * RH + 5.604
    try: 
        if any(isinstance(x, str) for x in (PM2_5, RH)):
            PM2_5_epa = 0
        elif PM2_5 <= 343:
            PM2_5_epa = 0.52 * PM2_5 - 0.086 * RH + 5.75
        elif PM2_5 > 343:
            PM2_5_epa = 0.46 * PM2_5 + 3.93 * 10 ** -4 * PM2_5 ** 2 + 2.97
        else:
            PM2_5_epa = 0
        return PM2_5_epa
    except Exception as e:
        logging.exception("calc_epa() error:\n%s" % e)


def process_data(document_name, client):
    for k, v in config.bbox_dict.items():
        # open the Google Sheets input worksheet
        in_worksheet_name: str = k
        out_worksheet_name: str = k + " Proc"
        in_sheet = client.open(document_name).worksheet(in_worksheet_name)
        df = pd.DataFrame(in_sheet.get_all_records())
        if k == "TV":
            df_tv = df.copy()
        df_proc = df.copy()
        df_proc['pm2.5_atm_avg'] = df_proc[['pm2.5_atm_a','pm2.5_atm_b']].mean(axis=1)
        df_proc['Ipm25'] = df_proc.apply(
            lambda x: calc_aqi(x['pm2.5_atm_avg']),
            axis=1
            )
        df_proc['pm2.5_cf_1_avg'] = df_proc[['pm2.5_cf_1_a','pm2.5_cf_1_b']].mean(axis=1)
        df_proc['pm25_epa'] = df_proc.apply(
                    lambda x: calc_epa(x['pm2.5_cf_1_avg'], x['humidity']),
                    axis=1
                        )
        df_summarized = df_proc.copy()
        df_summarized['time_stamp'] = pd.to_datetime(
            df_summarized['time_stamp'],
            format='%m/%d/%Y %H:%M:%S'
        )
        df_summarized.set_index('time_stamp', inplace=True)
        df_summarized = df_summarized.groupby('name').resample('1H').mean(numeric_only=True)
        df_summarized.reset_index(inplace=True)
        df_summarized['time_stamp'] = df_summarized['time_stamp'].dt.strftime('%m/%d/%Y %H:%M:%S')
        df_summarized['pm2.5_atm_a'] = pd.to_numeric(df_summarized['pm2.5_atm_a'], errors='coerce').astype(float)
        df_summarized['pm2.5_atm_b'] = pd.to_numeric(df_summarized['pm2.5_atm_b'], errors='coerce').astype(float)
        df_summarized = df_summarized.dropna(subset=['pm2.5_atm_a', 'pm2.5_atm_b'])
        df_summarized = df_summarized.fillna('')
        #Clean data when PM ATM 2.5 channels differ by 5 or 70%
        df_summarized.drop(df_summarized[abs(df_summarized['pm2.5_atm_a'] - df_summarized['pm2.5_atm_b']) >= 5].index, inplace=True)
        df_summarized.drop(
            df_summarized[abs(df_summarized['pm2.5_atm_a'] - df_summarized['pm2.5_atm_b']) /
                ((df_summarized['pm2.5_atm_a'] + df_summarized['pm2.5_atm_b'] + 1e-6) / 2) >= 0.7
            ].index,
            inplace=True
        )
        df_summarized = df_summarized.drop(columns=['pm2.5_atm_avg', 'pm2.5_cf_1_avg']) 
        # Humidity, temperature and pressure are in the RS dataframe at this point
        #cols = {'time_stamp': 'time_stamp', 'sensor_index': 'sensor_index', 'name': 'name', 'latitude': 'latitdue', 'longitude': 'longitude', 'altitude': 'altitude',
                #'rssi': 'rssi', 'uptime': 'uptime', 'humidity': 'humidity', 'temperature': 'temperature', 'pressure': 'pressure',
                #'pm1.0_atm_a': 'pm1.0_atm_a', 'pm1.0_atm_b': 'pm1.0_atm_b', 'pm2.5_atm_a': 'pm2.5_atm_a', 'pm2.5_atm_b': 'pm2.5_atm_b', 'pm10.0_atm_a': 'pm10.0_atm_a', 'pm10.0_atm_b': 'pm10.0_atm_b',
                #'pm1.0_cf_1_a': 'pm1.0_cf_1_a', 'pm1.0_cf_1_b': 'pm1.0_cf_1_b', 'pm2.5_cf_1_a': 'pm2.5_cf_1_a', 'pm2.5_cf_1_b': 'pm2.5_cf_1_b', 'pm10.0_cf_1_a': 'pm10.0_cf_1_a', 'pm10.0_cf_1_b': 'pm10.0_cf_1_b',
                #'0.3_um_count': '0.3_um_count', '0.5_um_count': '0.5_um_count', '1.0_um_count': '1.0_um_count', '2.5_um_count': '2.5_um_count', '5.0_um_count': '5.0_um_count', '10.0_um_count': '10.0_um_count',
                #'Ipm25': 'Ipm25', 'pm25_epa': 'pm25_epa'
                #}
        cols = ['time_stamp', 'sensor_index', 'name', 'latitude', 'longitude', 'altitude',
                'rssi', 'uptime', 'humidity', 'temperature', 'pressure',
                'pm1.0_atm_a', 'pm1.0_atm_b', 'pm2.5_atm_a', 'pm2.5_atm_b', 'pm10.0_atm_a', 'pm10.0_atm_b',
                'pm1.0_cf_1_a', 'pm1.0_cf_1_b', 'pm2.5_cf_1_a',  'pm2.5_cf_1_b', 'pm10.0_cf_1_a', 'pm10.0_cf_1_b',
                '0.3_um_count', '0.5_um_count', '1.0_um_count', '2.5_um_count', '5.0_um_count', '10.0_um_count',
                'Ipm25', 'pm25_epa'
                ]
        df_summarized = df_summarized[cols]
        print(" ")
        print(k)
        print(df_proc[['humidity', 'temperature', 'pressure']])
        print(" ")
        # open the Google Sheets output worksheet
        out_sheet = client.open(document_name).worksheet(out_worksheet_name)
        out_sheet.update([df_summarized.columns.values.tolist()] + df_summarized.values.tolist(), value_input_option="USER_ENTERED")
        sleep(30)
    return df_tv


def sensor_health(df, document_name, out_worksheet_health_name):
    # Compare the A&B channels and calculate percent good data.
    # Remove data when channels differ by >= +- 5 ug/m^3 and >= +- 70%
    sensor_health_list = []
    df['pm2.5_atm_dif'] = abs(df['pm2.5_atm_a'] - df['pm2.5_atm_b'])
    df_good = df[(
        df['pm2.5_atm_a']-df['pm2.5_atm_b']
        ).abs() >= 5.0]
    df_good = df_good[((
        (df_good['pm2.5_atm_a'] - df_good['pm2.5_atm_b']).abs()
        ) / ((df_good['pm2.5_atm_a'] + df_good['pm2.5_atm_b']) / 2)) >= 0.7]
    df_grouped = df.groupby('name')
    df_good_grouped = df_good.groupby('name')
    for k, v in df_grouped:
        try:
            pct_good = (df_grouped.get_group(k).shape[0] - df_good_grouped.get_group(k).shape[0]) / df_grouped.get_group(k).shape[0]
        except KeyError as e:
            pct_good = 1.00
        max_delta = df_grouped.get_group(k)['pm2.5_atm_dif'].max()
        sensor_health_list.append([k.upper(), pct_good, max_delta])
    out_sheet_health = client.open(document_name).worksheet(out_worksheet_health_name)
    df_health = pd.DataFrame(sensor_health_list)
    df_health = df_health.rename({0: 'NAME', 1: 'CONFIDENCE', 2: 'MAX ERROR'}, axis=1)
    df_health['CONFIDENCE'] = df_health['CONFIDENCE'].round(2)
    df_health = df_health.sort_values(by=['NAME'])
    out_sheet_health.update([df_health.columns.values.tolist()] + df_health.values.tolist())


def regional_stats(document_name):
    data_list = []
    df_regional_stats = pd.DataFrame(columns=['Region', 'Mean', 'Max'])
    for k, v in config.bbox_dict.items():
        worksheet_name = v[1] + " Proc"
        # open the Google Sheets input worksheet
        in_sheet = client.open(document_name).worksheet(worksheet_name)
        data = in_sheet.get_all_records()
        data_list.append(data) 
        df_combined = pd.concat([pd.DataFrame(data) for data in data_list])
        df_combined['Ipm25'] = pd.to_numeric(df_combined['Ipm25'], errors='coerce')
        df_combined = df_combined.dropna(subset=['Ipm25'])
        df_combined['Ipm25'] = df_combined['Ipm25'].astype(float)
        mean_value = df_combined['Ipm25'].mean().round(2)
        max_value = df_combined['Ipm25'].max().round(2)
        df_regional_stats.loc[len(df_regional_stats)] = [v[2], mean_value, max_value]
        df_combined = pd.DataFrame()
        data_list = []
        sleep(30)
    out_sheet_regional = client.open(document_name).worksheet("Regional")
    out_sheet_regional.update([df_regional_stats.columns.values.tolist()] + df_regional_stats.values.tolist())


def main():
    for k, v in config.bbox_dict.items():
        df = get_data(config.bbox_dict.get(k)[0])
        if df.empty:
            pass
        else:
            write_data(df, client, config.document_name, config.bbox_dict.get(k)[1], config.write_csv)
    local_interval_start = datetime.now()
    regional_interval_start = local_interval_start
    process_interval_start = local_interval_start
    while True:
        try:
            sleep(1)
            local_interval_td = datetime.now() - local_interval_start
            regional_interval_td = datetime.now() - regional_interval_start
            process_interval_td = datetime.now() - process_interval_start
            if local_interval_td.total_seconds() >= config.local_interval_duration:
                df_local = get_data(config.bbox_dict.get("TV")[0])
                if len (df_local.index) > 0:
                    write_data(df_local, client, config.document_name, config.local_worksheet_name, config.write_csv)
                local_interval_start = datetime.now()
            if regional_interval_td.total_seconds() > config.regional_interval_duration:
                for regional_key in config.regional_keys:
                    df = get_data(config.bbox_dict.get(regional_key)[0]) 
                    if len(df.index) > 0:
                        write_data(df, client, config.document_name, config.bbox_dict.get(regional_key)[1], config.write_csv)
                    sleep(10)
                regional_interval_start = datetime.now()
            if process_interval_td.total_seconds() > config.process_interval_duration:
                df = process_data(config.document_name, client)
                process_interval_td = datetime.now() - process_interval_start
                if len(df.index) > 0:
                    sensor_health(df, config.document_name, config.out_worksheet_health_name)
                    regional_stats(config.document_name)
        except KeyboardInterrupt:
            sys.exit()


if __name__ == "__main__":
    main()