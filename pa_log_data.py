# Regularly Polls Purpleair api for outdoor sensor data for sensors within deined rectangular geographic regions at a defined interval.
# Appends data to Google Sheets
# Processes data
# James S. Lucas - 20230507

import sys
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import json
import pandas as pd
from pathlib import Path
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from datetime import datetime, timedelta
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
    output_pathname: str = Path(config.MATRIX5, file_name)
elif sys.platform == 'linux':
    cwd: str = Path.cwd()
    output_pathname: str = Path(cwd, file_name)
# set the credentials for the Google Sheets service account
scope: List[str] = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive'
                    ]
creds = ServiceAccountCredentials.from_json_keyfile_name(config.GSPREAD_SERVICE_ACCOUNT_JSON_PATH, scope)
client = gspread.authorize(creds)


def get_data(previous_time, bbox: List[float]) -> pd.DataFrame:
    root_url: str = 'https://api.purpleair.com/v1/sensors/?fields={fields}&max_age={et}&location_type=0&nwlng={nwlng}&nwlat={nwlat}&selng={selng}&selat={selat}'
    et_since = int((datetime.now() - previous_time + timedelta(seconds=20)).total_seconds())
    params: Dict[str, str] = {
        'fields': "name,latitude,longitude,altitude,rssi,uptime,humidity,temperature,pressure,voc,"
                "pm1.0_atm_a,pm1.0_atm_b,pm2.5_atm_a,pm2.5_atm_b,pm10.0_atm_a,pm10.0_atm_b,"
                "pm1.0_cf_1_a,pm1.0_cf_1_b,pm2.5_cf_1_a,pm2.5_cf_1_b,pm10.0_cf_1_a,pm10.0_cf_1_b,"
                "0.3_um_count,0.5_um_count,1.0_um_count,2.5_um_count,5.0_um_count,10.0_um_count",
        'nwlng': bbox[0],
        'selat': bbox[1],
        'selng': bbox[2],
        'nwlat': bbox[3],
        'et': et_since
    }
    url = root_url.format(**params)
    cols = ['time_stamp', 'sensor_index'] + [col for col in params['fields'].split(',')]
    try:
        response = session.get(url)
    except Exception as e:
        logging.exception("get_data error")
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
        logging.exception("get_data() response not ok")
    return df


def write_data(df, client, DOCUMENT_NAME, worksheet_name, write_mode, WRITE_CSV=False):
    max_attempts = 3
    attempts = 0
    while attempts < max_attempts:
        try:
            # open the Google Sheets output worksheet
            sheet = client.open(DOCUMENT_NAME).worksheet(worksheet_name)
            if write_mode == 'append':
                sheet.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')
            elif write_mode == 'update':
                sheet.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option="USER_ENTERED")
            break
        except gspread.exceptions.APIError as e:
            logging.exception("gspread error in write_data()")
            attempts += 1
            if attempts < max_attempts:
                sleep(60)
            else:
                logging.exception("gspread error in write_data() max attempts reached")  
    # append the data to Google Sheets 
    if WRITE_CSV is True:
        try:
            df.to_csv(output_pathname, index=True, header=True)
        except Exception as e:
            logging.exception("write_data() error writing csv")


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
        logging.exception("calc_aqi() error")


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
        logging.exception("calc_epa() error")


def process_data(DOCUMENT_NAME, client):
    write_mode = 'update'
    cols_1 = ['time_stamp']
    cols_2 = ['sensor_index', 'name', 'latitude', 'longitude']
    cols_3 = ['altitude']
    cols_4 = ['rssi']
    cols_5 = ['uptime']
    cols_6 = ['humidity', 'temperature', 'pressure', 'voc']
    cols_7 = ['pm1.0_atm_a', 'pm1.0_atm_b', 'pm2.5_atm_a', 'pm2.5_atm_b', 'pm10.0_atm_a', 'pm10.0_atm_b',
            'pm1.0_cf_1_a', 'pm1.0_cf_1_b', 'pm2.5_cf_1_a',  'pm2.5_cf_1_b', 'pm10.0_cf_1_a', 'pm10.0_cf_1_b',
            '0.3_um_count', '0.5_um_count', '1.0_um_count', '2.5_um_count', '5.0_um_count', '10.0_um_count']
    cols_8 = ['pm25_epa', 'Ipm25']
    cols = cols_1 + cols_2 + cols_3 + cols_4 + cols_5 + cols_6 + cols_7 + cols_8
    for k, v in config.BBOX_DICT.items():
        # open the Google Sheets input worksheet
        in_worksheet_name: str = k
        out_worksheet_name: str = k + " Proc"
        in_sheet = client.open(DOCUMENT_NAME).worksheet(in_worksheet_name)
        df = pd.DataFrame(in_sheet.get_all_records())
        if k == "TV":
            df_tv = df.copy()
        df['pm2.5_atm_avg'] = df[['pm2.5_atm_a','pm2.5_atm_b']].mean(axis=1)
        df['Ipm25'] = df.apply(
            lambda x: calc_aqi(x['pm2.5_atm_avg']),
            axis=1
            )
        df['pm2.5_cf_1_avg'] = df[['pm2.5_cf_1_a','pm2.5_cf_1_b']].mean(axis=1)
        df['pm25_epa'] = df.apply(
                    lambda x: calc_epa(x['pm2.5_cf_1_avg'], x['humidity']),
                    axis=1
                        )
        df['time_stamp'] = pd.to_datetime(
            df['time_stamp'],
            format='%m/%d/%Y %H:%M:%S'
        )
        df = df.set_index('time_stamp')
        df[cols_6] = df[cols_6].replace('', 0)
        df[cols_6] = df[cols_6].astype(float)
        df_summarized = df.groupby('name').resample(config.PROCESS_RESAMPLE_RULE).mean(numeric_only=True)
        df_summarized = df_summarized.reset_index()
        df_summarized['time_stamp'] = df_summarized['time_stamp'].dt.strftime('%m/%d/%Y %H:%M:%S')
        df_summarized['pm2.5_atm_a'] = pd.to_numeric(df_summarized['pm2.5_atm_a'], errors='coerce').astype(float)
        df_summarized['pm2.5_atm_b'] = pd.to_numeric(df_summarized['pm2.5_atm_b'], errors='coerce').astype(float)
        df_summarized = df_summarized.dropna(subset=['pm2.5_atm_a', 'pm2.5_atm_b'])
        df_summarized = df_summarized.fillna('')
        #Clean data when PM ATM 2.5 channels differ by 5 or 70%
        df_summarized = df_summarized.drop(df_summarized[abs(df_summarized['pm2.5_atm_a'] - df_summarized['pm2.5_atm_b']) >= 5].index)
        df_summarized = df_summarized.drop(
            df_summarized[abs(df_summarized['pm2.5_atm_a'] - df_summarized['pm2.5_atm_b']) /
                ((df_summarized['pm2.5_atm_a'] + df_summarized['pm2.5_atm_b'] + 1e-6) / 2) >= 0.7
            ].index
        )
        df_summarized = df_summarized.drop(columns=['pm2.5_atm_avg', 'pm2.5_cf_1_avg']) 
        df_summarized[cols_4] = df_summarized[cols_4].round(2)
        df_summarized[cols_5] = df_summarized[cols_5].astype(int)
        df_summarized[cols_6] = df_summarized[cols_6].round(2)
        df_summarized[cols_7] = df_summarized[cols_7].round(2)
        df_summarized[cols_8] = df_summarized[cols_8].astype(int)
        df_summarized = df_summarized[cols]
        write_data(df_summarized, client, DOCUMENT_NAME, out_worksheet_name, write_mode)
        sleep(90)
    return df_tv


def sensor_health(client, df, DOCUMENT_NAME, OUT_WORKSHEET_HEALTH_NAME):
    # Compare the A&B channels and calculate percent good data.
    # Remove data when channels differ by >= +- 5 ug/m^3 and >= +- 70%
    sensor_health_list = []
    write_mode = 'update'
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
        signal_strength = df_grouped.get_group(k)['rssi'].mean()
        uptime = df_grouped.get_group(k)['uptime'].max()
        sensor_health_list.append([k.upper(), pct_good, max_delta, signal_strength, uptime])
    df_health = pd.DataFrame(sensor_health_list)
    df_health = df_health.rename({0: 'NAME', 1: 'CONFIDENCE', 2: 'MAX ERROR', 3: 'RSSI', 4: 'UPTIME'}, axis=1)
    df_health['CONFIDENCE'] = df_health['CONFIDENCE'].round(2)
    df_health = df_health.sort_values(by=['NAME'])
    write_data(df_health, client, DOCUMENT_NAME, OUT_WORKSHEET_HEALTH_NAME, write_mode)
    sleep(20)


def regional_stats(client, DOCUMENT_NAME):
    data_list = []
    write_mode = 'update'
    out_worksheet_regional_name = 'Regional'
    df_regional_stats = pd.DataFrame(columns=['Region', 'Mean', 'Max'])
    for k, v in config.BBOX_DICT.items():
        worksheet_name = v[1] + " Proc"
        # open the Google Sheets input worksheet
        in_sheet = client.open(DOCUMENT_NAME).worksheet(worksheet_name)
        data = in_sheet.get_all_records()
        if data:
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
            write_data(df_regional_stats, client, DOCUMENT_NAME, out_worksheet_regional_name, write_mode)
            sleep(90)


def main():
    five_min_ago = datetime.now() - timedelta(minutes=5)
    for k, v in config.BBOX_DICT.items():
        df = get_data(five_min_ago, config.BBOX_DICT.get(k)[0])
        if len(df.index) > 0:
            write_mode = 'append'
            write_data(df, client, config.DOCUMENT_NAME, config.BBOX_DICT.get(k)[1], write_mode, config.WRITE_CSV)
        else:
            pass
    local_interval_start = datetime.now()
    regional_interval_start = datetime.now()
    process_interval_start = datetime.now()
    while True:
        try:
            sleep(1)
            local_interval_et = (datetime.now() - local_interval_start).total_seconds()
            regional_interval_et = (datetime.now() - regional_interval_start).total_seconds()
            process_interval_et = (datetime.now() - process_interval_start).total_seconds()
            if local_interval_et >= config.LOCAL_INTERVAL_DURATION:
                df_local = get_data(local_interval_start, config.BBOX_DICT.get("TV")[0])
                if len (df_local.index) > 0:
                    write_mode = 'append'
                    write_data(df_local, client, config.DOCUMENT_NAME, config.LOCAL_WORKSHEET_NAME, write_mode, config.WRITE_CSV)
                local_interval_start = datetime.now()
            if regional_interval_et > config.REGIONAL_INTERVAL_DURATION:
                for regional_key in config.REGIONAL_KEYS:
                    df = get_data(regional_interval_start, config.BBOX_DICT.get(regional_key)[0]) 
                    if len(df.index) > 0:
                        write_mode = 'append'
                        write_data(df, client, config.DOCUMENT_NAME, config.BBOX_DICT.get(regional_key)[1], write_mode, config.WRITE_CSV)
                    sleep(10)
                regional_interval_start = datetime.now()
            if process_interval_et > config.PROCESS_INTERVAL_DURATION:
                df = process_data(config.DOCUMENT_NAME, client)
                process_interval_start = datetime.now()
                if len(df.index) > 0:
                    sensor_health(client, df, config.DOCUMENT_NAME, config.OUT_WORKSHEET_HEALTH_NAME)
                    regional_stats(client, config.DOCUMENT_NAME)
        except KeyboardInterrupt:
            sys.exit()


if __name__ == "__main__":
    main()