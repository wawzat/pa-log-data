#!/usr/bin/env python3
# Regularly Polls Purpleair api for outdoor sensor data for sensors within defined rectangular geographic regions at a defined interval.
# Appends data to Google Sheets
# Processes data
# James S. Lucas - 20230625

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
from tabulate import tabulate
import logging
from typing import List
from conversions import AQI, EPA
import constants
from configparser import ConfigParser

# Read config file
config = ConfigParser()
config.read('config.ini')

# Setup exception logging
format_string = '%(name)s - %(asctime)s : %(message)s'
logging.basicConfig(filename='error.log',
                    format = format_string)

session = requests.Session()
retry = Retry(connect=5, backoff_factor=1.0)
adapter = HTTPAdapter(max_retries=retry)
PURPLEAIR_READ_KEY = config.get('purpleair', 'PURPLEAIR_READ_KEY')
if PURPLEAIR_READ_KEY == '':
    logging.error('Error: PURPLEAIR_READ_KEY not set in config.ini')
    print('ERROR: PURPLEAIR_READ_KEY not set in config.ini')
    sys.exit(1)
session.headers.update({'X-API-Key': PURPLEAIR_READ_KEY})
session.mount('http://', adapter)
session.mount('https://', adapter)
file_name: str = 'pa_log_test.csv'
if sys.platform == 'win32':
    output_pathname: str = Path(constants.MATRIX5, file_name)
elif sys.platform == 'linux':
    cwd: str = Path.cwd()
    output_pathname: str = Path(cwd, file_name)
# set the credentials for the Google Sheets service account
scope: List[str] = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive'
                    ]
GSPREAD_SERVICE_ACCOUNT_JSON_PATH = config.get('google', 'GSPREAD_SERVICE_ACCOUNT_JSON_PATH')
if GSPREAD_SERVICE_ACCOUNT_JSON_PATH == '':
    logging.error('Error: GSPREAD_SERVICE_ACCOUNT_JSON_PATH not set in config.ini, exiting...')
    print('Error: GSPREAD_SERVICE_ACCOUNT_JSON_PATH not set in config.ini, exiting...')
    sys.exit(1)
creds = ServiceAccountCredentials.from_json_keyfile_name(GSPREAD_SERVICE_ACCOUNT_JSON_PATH, scope)
client = gspread.authorize(creds)


def status_update(local_et, regional_et, process_et):
    """
    A function that calculates the time remaining for each interval and prints it in a table format.

    Args:
        local_et (int): The elapsed time for the local interval in seconds.
        regional_et (int): The elapsed time for the regional interval in seconds.
        process_et (int): The elapsed time for the process interval in seconds.

    Returns:
        A datetime object representing the current time.
    """

    local_minutes = int((constants.LOCAL_INTERVAL_DURATION - local_et) / 60)
    local_seconds = int((constants.LOCAL_INTERVAL_DURATION - local_et) % 60)
    regional_minutes = int((constants.REGIONAL_INTERVAL_DURATION - regional_et) / 60)
    regional_seconds = int((constants.REGIONAL_INTERVAL_DURATION - regional_et) % 60)
    process_minutes = int((constants.PROCESS_INTERVAL_DURATION - process_et) / 60)
    process_seconds = int((constants.PROCESS_INTERVAL_DURATION - process_et) % 60)
    table_data = [
        ['Local:', f"{local_minutes:02d}:{local_seconds:02d}"],
        ['Regional:', f"{regional_minutes:02d}:{regional_seconds:02d}"],
        ['Process:', f"{process_minutes:02d}:{process_seconds:02d}"]
    ]
    print(tabulate(table_data, headers=['Interval', 'Time Remaining (MM:SS)'], tablefmt='orgtbl'))
    print("\033c", end="")
    return datetime.now()


def elapsed_time(local_start, regional_start, process_start, status_start):
    """
    Calculates the elapsed time for each interval since the start time.

    Args:
        local_start (datetime): The start time for the local interval.
        regional_start (datetime): The start time for the regional interval.
        process_start (datetime): The start time for the process interval.
        status_start (datetime): The start time for the status interval.

    Returns:
        A tuple containing the elapsed time for each interval in seconds.
    """
    local_et: int = (datetime.now() - local_start).total_seconds()
    regional_et: int = (datetime.now() - regional_start).total_seconds()
    process_et: int = (datetime.now() - process_start).total_seconds()
    status_et: int = (datetime.now() - status_start).total_seconds()
    return local_et, regional_et, process_et, status_et


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes rows from the input DataFrame where the difference between the PM2.5 atmospheric concentration readings
    from two sensors is either greater than or equal to 5 or greater than or equal to 70% of the average of the two readings.

    Args:
        df (pd.DataFrame): The input DataFrame containing the PM2.5 atmospheric concentration readings from two sensors.

    Returns:
        A new DataFrame with the rows removed where the difference between the PM2.5 atmospheric concentration readings
        from two sensors is either greater than or equal to 5 or greater than or equal to 70% of the average of the two readings.
    """
    df = df.drop(df[abs(df['pm2.5_atm_a'] - df['pm2.5_atm_b']) >= 5].index)
    df = df.drop(
        df[abs(df['pm2.5_atm_a'] - df['pm2.5_atm_b']) /
            ((df['pm2.5_atm_a'] + df['pm2.5_atm_b'] + 1e-6) / 2) >= 0.7
        ].index
    )
    return df


def format_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Formats the input DataFrame by rounding the values in certain columns and converting the values in other columns to integers.

    Args:
        df (pd.DataFrame): The input DataFrame to be formatted.

    Returns:
        A new DataFrame with the specified columns rounded or converted to integers.
    """
    df[constants.cols_4] = df[constants.cols_4].round(2)
    df[constants.cols_5] = df[constants.cols_5].astype(int)
    df[constants.cols_6] = df[constants.cols_6].round(2)
    df[constants.cols_7] = df[constants.cols_7].round(2)
    df[constants.cols_8] = df[constants.cols_8].round(2)
    df[constants.cols_9] = df[constants.cols_9].astype(int)
    df = df[constants.cols]
    return df


def get_data(previous_time, bbox: List[float]) -> pd.DataFrame:
    """
    A function that queries the PurpleAir API for sensor data within a given bounding box and time frame.

    Args:
        previous_time (datetime): A datetime object representing the time of the last query.
        bbox (List[float]): A list of four floats representing the bounding box of the area of interest.
            The order is [northwest longitude, southeast latitude, southeast longitude, northwest latitude].

    Returns:
        A pandas DataFrame containing sensor data for the specified area and time frame. The DataFrame will contain columns
        for the timestamp of the data, the index of the sensor, and various sensor measurements such as temperature,
        humidity, and PM2.5 readings.
    """
    et_since = int((datetime.now() - previous_time + timedelta(seconds=20)).total_seconds())
    root_url: str = 'https://api.purpleair.com/v1/sensors/?fields={fields}&max_age={et}&location_type=0&nwlng={nwlng}&nwlat={nwlat}&selng={selng}&selat={selat}'
    params = {
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
    url: str = root_url.format(**params)
    cols: List[str] = ['time_stamp', 'sensor_index'] + [col for col in params['fields'].split(',')]
    try:
        response = session.get(url)
    except Exception as e:
        logging.exception('get_data error')
        df = pd.DataFrame()
        return df
    if response.ok:
        url_data = response.content
        json_data = json.loads(url_data)
        df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
        df = df.fillna('')
        df['time_stamp'] = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
        # convert the lat and lon values to strings
        df['latitude'] = df['latitude'].astype(str)
        df['longitude'] = df['longitude'].astype(str)
        df = df[cols]
    else:
        df = pd.DataFrame()
        logging.exception('get_data() response not ok')
    return df


def write_data(df, client, DOCUMENT_NAME, worksheet_name, write_mode, WRITE_CSV=False):
    """
    Writes the given Pandas DataFrame to a Google Sheets worksheet with the specified name in the specified document.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to be written.
        client (gspread.client.Client): A client object for accessing the Google Sheets API.
        DOCUMENT_NAME (str): The name of the Google Sheets document to write to.
        worksheet_name (str): The name of the worksheet to write the data to.
        write_mode (str): The mode for writing the data to the worksheet. Either "append" or "update".
        WRITE_CSV (bool, optional): Whether to also write the DataFrame to a CSV file. Defaults to False.

    Raises:
        Exception: If an error occurs during the writing process.

    Returns:
        None
    """
    MAX_ATTEMPTS: int = 3
    attempts: int = 0
    while attempts < MAX_ATTEMPTS:
        try:
            # open the Google Sheets output worksheet and write the data
            sheet = client.open(DOCUMENT_NAME).worksheet(worksheet_name)
            if write_mode == 'append':
                sheet.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')
            elif write_mode == 'update':
                sheet.clear()
                sheet.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option='USER_ENTERED')
            break
        except gspread.exceptions.APIError as e:
            logging.exception('gspread error in write_data()')
            attempts += 1
            if attempts < MAX_ATTEMPTS:
                sleep(60)
            else:
                logging.exception('gspread error in write_data() max attempts reached')  
    # Write the data to local csv file 
    if WRITE_CSV is True:
        try:
            df.to_csv(output_pathname, mode='a', index=True, header=True)
        except Exception as e:
            logging.exception('write_data() error writing csv')


def current_process(df):
    """
    This function takes a pandas DataFrame as input, performs some processing on it and saves it as a Google Sheet.
    The sheet will contain only the most recent data from each sensor
    
    Args:
        df (pandas.DataFrame): The DataFrame to be processed.
        
    Returns:
        df (pandas.DataFrame): The processed DataFrame.
    
    Notes:
        - This function modifies the input DataFrame in place.
        - The following columns are added to the DataFrame:
            - Ipm25 (AQI)
            - pm25_epa
            - time_stamp_pacific
        - Data is cleaned according to EPA criteria.
    """
    df['Ipm25'] = df.apply(
        lambda x: AQI.calculate(x['pm2.5_atm_a'], x['pm2.5_atm_b']),
        axis=1
        )
    df['pm25_epa'] = df.apply(
                lambda x: EPA.calculate(x['humidity'], x['pm2.5_cf_1_a'], x['pm2.5_cf_1_b']),
                axis=1
                )
    df['time_stamp'] = pd.to_datetime(
        df['time_stamp'],
        format='%m/%d/%Y %H:%M:%S'
    )
    df['time_stamp_pacific'] = df['time_stamp'].dt.tz_localize('UTC').dt.tz_convert('US/Pacific')
    df['time_stamp'] = df['time_stamp'].dt.strftime('%m/%d/%Y %H:%M:%S')
    df['time_stamp_pacific'] = df['time_stamp_pacific'].dt.strftime('%m/%d/%Y %H:%M:%S')
    df = clean_data(df)
    df = format_data(df)
    return df


def process_data(DOCUMENT_NAME, client):
    """
    Process data from Google Sheets sheets for each region. Data is cleaned, summarized and various values are calculated. Data are saved to
    different worksheets in the same Google Sheets document.

    Args:
        DOCUMENT_NAME (str): The name of the Google Sheets document to be processed.
        client: The Google Sheets client object.

    Returns:
        A cleaned and summarized pandas DataFrame with the following columns:
        'time_stamp', 'sensor_index', 'name', 'latitude', 'longitude', 'altitude', 'rssi',
        'uptime', 'humidity', 'temperature', 'pressure', 'voc', 'pm1.0_atm_a',
        'pm1.0_atm_b', 'pm2.5_atm_a', 'pm2.5_atm_b', 'pm10.0_atm_a', 'pm10.0_atm_b',
        'pm1.0_cf_1_a', 'pm1.0_cf_1_b', 'pm2.5_cf_1_a', 'pm2.5_cf_1_b', 'pm10.0_cf_1_a',
        'pm10.0_cf_1_b', '0.3_um_count', '0.5_um_count', '1.0_um_count', '2.5_um_count',
        '5.0_um_count', '10.0_um_count', 'pm25_epa', 'Ipm25'.
    """
    write_mode: str = 'update'
    for k, v in constants.BBOX_DICT.items():
        # open the Google Sheets input worksheet and read in the data
        in_worksheet_name: str = k
        out_worksheet_name: str = k + ' Proc'
        MAX_ATTEMPTS: int = 3
        attempts: int = 0
        while attempts < MAX_ATTEMPTS:
            try:
                in_sheet = client.open(DOCUMENT_NAME).worksheet(in_worksheet_name)
                df = pd.DataFrame(in_sheet.get_all_records())
                break
            except gspread.exceptions.APIError as e:
                attempts += 1
                message = f'process_data() gspread error attempt #{attempts}'
                logging.exception(message)
                if attempts < MAX_ATTEMPTS:
                    sleep(90)
                else:
                    logging.exception('process_data() gspread error max attempts reached')
        if constants.LOCAL_REGION == k:
            # Save the dataframe for later use by the regional_stats() and sensor_health() functions
            df_local = df.copy()
        df['Ipm25'] = df.apply(
            lambda x: AQI.calculate(x['pm2.5_atm_a'], x['pm2.5_atm_b']),
            axis=1
            )
        df['pm25_epa'] = df.apply(
                    lambda x: EPA.calculate(x['humidity'], x['pm2.5_cf_1_a'], x['pm2.5_cf_1_b']),
                    axis=1
                    )
        df['time_stamp'] = pd.to_datetime(
            df['time_stamp'],
            format='%m/%d/%Y %H:%M:%S'
        )
        df = df.set_index('time_stamp')
        df[constants.cols_6] = df[constants.cols_6].replace('', 0)
        df[constants.cols_6] = df[constants.cols_6].astype(float)
        df_summarized = df.groupby('name').resample(constants.PROCESS_RESAMPLE_RULE).mean(numeric_only=True)
        df_summarized = df_summarized.reset_index()
        df_summarized['time_stamp_pacific'] = df_summarized['time_stamp'].dt.tz_localize('UTC').dt.tz_convert('US/Pacific')
        df_summarized['time_stamp'] = df_summarized['time_stamp'].dt.strftime('%m/%d/%Y %H:%M:%S')
        df_summarized['time_stamp_pacific'] = df_summarized['time_stamp_pacific'].dt.strftime('%m/%d/%Y %H:%M:%S')
        df_summarized['pm2.5_atm_a'] = pd.to_numeric(df_summarized['pm2.5_atm_a'], errors='coerce').astype(float)
        df_summarized['pm2.5_atm_b'] = pd.to_numeric(df_summarized['pm2.5_atm_b'], errors='coerce').astype(float)
        df_summarized = df_summarized.dropna(subset=['pm2.5_atm_a', 'pm2.5_atm_b'])
        df_summarized = df_summarized.fillna('')
        df_summarized = clean_data(df_summarized)
        df_summarized = format_data(df_summarized)
        write_data(df_summarized, client, DOCUMENT_NAME, out_worksheet_name, write_mode)
        sleep(90)
    return df_local


def sensor_health(client, df, DOCUMENT_NAME, OUT_WORKSHEET_HEALTH_NAME):
    """
    Analyzes air quality sensor data and calculates the performance of each sensor.

    Args:
        client (object): A client object used to access a Google Sheets API.
        df (pandas.DataFrame): A DataFrame containing air quality sensor data.
        DOCUMENT_NAME (str): The name of the Google Sheets document to write the output to.
        OUT_WORKSHEET_HEALTH_NAME (str): The name of the worksheet to write the health data to.

    Returns:
        None

    This function compares the readings from two PurpleAir sensor channels (A and B) and removes data points where the difference
    is greater than or equal to 5 ug/m^3 or 70%. For each sensor, it calculates the percentage of "good" data points,
    which is defined as the percentage of data points that passed the threshold check. 
    It also calculates the maximum difference between the A and B channels,
    the mean signal strength (RSSI), and the maximum uptime for each sensor. 
    The output is written to a specified Google Sheets document worksheet.
    """
    sensor_health_list = []
    write_mode: str = 'update'
    df['pm2.5_atm_dif'] = abs(df['pm2.5_atm_a'] - df['pm2.5_atm_b'])
    df_good = clean_data(df)
    df_grouped = df.groupby('name')
    df_good_grouped = df_good.groupby('name')
    for k, v in df_grouped:
        try:
            pct_good = 1 - ((df_grouped.get_group(k).shape[0] - df_good_grouped.get_group(k).shape[0]) / df_grouped.get_group(k).shape[0])
        except KeyError as e:
            pct_good = 1.00
        max_delta = df_grouped.get_group(k)['pm2.5_atm_dif'].max()
        signal_strength = df_grouped.get_group(k)['rssi'].mean()
        uptime = df_grouped.get_group(k)['uptime'].max()
        sensor_health_list.append([k.upper(), pct_good, max_delta, signal_strength, uptime])
    df_health = pd.DataFrame(sensor_health_list)
    df_health = df_health.rename({0: 'NAME', 1: 'CONFIDENCE', 2: 'MAX ERROR', 3: 'RSSI', 4: 'UPTIME'}, axis=1)
    df_health['CONFIDENCE'] = df_health['CONFIDENCE'].round(2)
    df_health['RSSI'] = df_health['RSSI'].round(2)
    df_health = df_health.sort_values(by=['NAME'])
    write_data(df_health, client, DOCUMENT_NAME, OUT_WORKSHEET_HEALTH_NAME, write_mode)
    sleep(20)


def regional_stats(client, DOCUMENT_NAME):
    """
    Retrieves air quality data from a Google Sheets document and calculates the mean and maximum values for each region.

    Args:
        client (object): A client object used to access a Google Sheets API.
        DOCUMENT_NAME (str): The name of the Google Sheets document to retrieve data from.

    Returns:
        None

    This function retrieves air quality data from a Google Sheets document for each region specified in the BBOX_DICT dictionary.
    It calculates the mean and maximum values for each region and writes the output to a specified worksheet in the same Google Sheets document.
    """
    data_list = []
    write_mode: str = 'update'
    out_worksheet_regional_name: str = 'Regional'
    df_regional_stats = pd.DataFrame(columns=['Region', 'Mean', 'Max'])
    MAX_ATTEMPTS: int = 3
    attempts: int = 0
    for k, v in constants.BBOX_DICT.items():
        worksheet_name = v[1] + ' Proc'
        while attempts < MAX_ATTEMPTS:
            try:
                # open the Google Sheets input worksheet
                in_sheet = client.open(DOCUMENT_NAME).worksheet(worksheet_name)
                data = in_sheet.get_all_records()
                break
            except gspread.exceptions.APIError as e:
                attempts += 1
                message = f'regional_stats() gspread error attempt #{attempts}'
                logging.exception(message)
                if attempts < MAX_ATTEMPTS:
                    sleep(90)
                else:
                    logging.exception('regional_stats() gspread error max attempts reached')
            except requests.exceptions.ConnectionError as e:
                attempts += 1
                message = f'regional_stats() requests error attempt #{attempts}'
                logging.exception(message)
                if attempts < MAX_ATTEMPTS:
                    sleep(90)
                else:
                    logging.exception('regional_stats() requests error max attempts reached')
        if len(data) > 0:
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
            sleep(90)
        write_data(df_regional_stats, client, DOCUMENT_NAME, out_worksheet_regional_name, write_mode)


def main():
    five_min_ago: datetime = datetime.now() - timedelta(minutes=5)
    for k, v in constants.BBOX_DICT.items():
        df = get_data(five_min_ago, constants.BBOX_DICT.get(k)[0])
        if len(df.index) > 0:
            write_mode = 'append'
            write_data(df, client, constants.DOCUMENT_NAME, constants.BBOX_DICT.get(k)[1], write_mode, constants.WRITE_CSV)
        else:
            pass
    local_start, regional_start, process_start, status_start = datetime.now(), datetime.now(), datetime.now(), datetime.now()
    while True:
        try:
            sleep(.1)
            local_et, regional_et, process_et, status_et = elapsed_time(local_start, regional_start, process_start, status_start)
            if status_et >= constants.STATUS_INTERVAL_DURATION:
                status_start = status_update(local_et, regional_et, process_et)
            if local_et >= constants.LOCAL_INTERVAL_DURATION:
                df_local = get_data(local_start, constants.BBOX_DICT.get(constants.LOCAL_REGION)[0])
                if len (df_local.index) > 0:
                    write_mode: str = 'append'
                    write_data(df_local, client, constants.DOCUMENT_NAME, constants.LOCAL_WORKSHEET_NAME, write_mode, constants.WRITE_CSV)
                    sleep(10)
                    df_current = current_process(df_local)
                    write_mode: str = 'update'
                    write_data(df_current, client, constants.DOCUMENT_NAME, constants.CURRENT_WORKSHEET_NAME, write_mode, constants.WRITE_CSV)
                local_start: datetime = datetime.now()
            if regional_et > constants.REGIONAL_INTERVAL_DURATION:
                for regional_key in constants.REGIONAL_KEYS:
                    df = get_data(regional_start, constants.BBOX_DICT.get(regional_key)[0]) 
                    if len(df.index) > 0:
                        write_mode: str = 'append'
                        write_data(df, client, constants.DOCUMENT_NAME, constants.BBOX_DICT.get(regional_key)[1], write_mode, constants.WRITE_CSV)
                    sleep(10)
                regional_start: datetime = datetime.now()
            if process_et > constants.PROCESS_INTERVAL_DURATION:
                df = process_data(constants.DOCUMENT_NAME, client)
                process_start: datetime = datetime.now()
                if len(df.index) > 0:
                    sensor_health(client, df, constants.DOCUMENT_NAME, constants.OUT_WORKSHEET_HEALTH_NAME)
                    regional_stats(client, constants.DOCUMENT_NAME)
        except KeyboardInterrupt:
            sys.exit()


if __name__ == "__main__":
    main()