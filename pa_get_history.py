"""
This program retrieves historical data from PurpleAir sensors and adds it to a Google Sheets document. 
It uses the PurpleAir API to retrieve sensor data for a given sensor ID and time frame, and saves the data to a CSV file. 
The CSV file is then uploaded to a Google Sheets document using the Google Sheets API.

The program requires a Google Sheets service account JSON file and a PurpleAir API key to function properly. 
The service account JSON file should be stored in a secure location and the path to the file should be specified in the `config.py` file. 
The PurpleAir API key should also be specified in the `config.py` file.

The program can be run from the command line with the following arguments:
    -m, --month: Integer of the month to get data for.
    -y, --year: The year to get data for.
    -s, --sensor: The name of the sensor to get data for.

The program contains the following functions:
    - get_arguments(): Parses command line arguments and returns them as a Namespace object.
    - get_data(sensor_id, yr, mnth): Queries the PurpleAir API for sensor data for a given sensor ID and time frame, and returns the data as a pandas DataFrame.
"""
# James S. Lucas - 20230603

import sys
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import json
import pandas as pd
import argparse
from pathlib import Path
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from datetime import datetime
from time import sleep
import calendar
import math
import logging
from typing import List
from conversions import EPA, AQI
import config

# Setup exception logging
format_string = '%(name)s - %(asctime)s : %(message)s'
logging.basicConfig(filename='pa_get_history_error.log',
                    format = format_string)

session = requests.Session()
retry = Retry(connect=5, backoff_factor=1.0)
adapter = HTTPAdapter(max_retries=retry)
session.headers.update({'X-API-Key': config.PURPLEAIR_READ_KEY})
session.mount('http://', adapter)
session.mount('https://', adapter)
file_name: str = 'pa_log_test.csv'
if sys.platform == 'win32':
    output_pathname = Path(config.MATRIX5, file_name)
elif sys.platform == 'linux':
    cwd: str = Path.cwd()
    output_pathname: str = Path(cwd, file_name)

# set the credentials for the Google Sheets service account
scope: List[str] = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive'
                    ]
creds = ServiceAccountCredentials.from_json_keyfile_name(config.GSPREAD_SERVICE_ACCOUNT_JSON_PATH, scope)
client = gspread.authorize(creds)


def get_arguments():
    parser = argparse.ArgumentParser(
    description='Get PurpleAir Sensor Historical Data.',
    prog='pa_get_history.py',
    usage='%(prog)s [-m <month>] [-y <year>] [-s <sensor>]',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    g=parser.add_argument_group(title='arguments',
            description='''    -m, --month  Integer of the month to get data for.
            -y, --year   The year to get data for.
            -s, --sensor  The name of the sensor to get data for.                           ''')
    g.add_argument('-m', '--month',
                    type=int,
                    default=1,
                    dest='mnth',
                    help=argparse.SUPPRESS)
    g.add_argument('-y', '--year',
                    type=int,
                    default=2023,
                    dest='yr',
                    help=argparse.SUPPRESS)
    g.add_argument('-s', '--sensor',
                    type=str,
                    default=None,
                    dest='sensor_name',
                    help=argparse.SUPPRESS)

    args = parser.parse_args()
    return(args)


def get_data(sensor_id, yr, mnth) -> pd.DataFrame:
    """
    A function that queries the PurpleAir API for sensor data for a given sensor_id.

    Args:
        sensor_id.

    Returns:
        A pandas DataFrame containing sensor data for the specified sensor_id and time frame. The DataFrame will contain columns
        for the timestamp of the data, the index of the sensor, and various sensor measurements such as temperature,
        humidity, and PM2.5 readings.
    """
    last_day_of_month = calendar.monthrange(yr, mnth)[1]
    average = 30
    average_limits = {
        2: 2,
        10: 3,
        30: 7,
        60: 14
    }
    root_url: str = 'https://api.purpleair.com/v1/sensors/{ID}/history?start_timestamp={start_timestamp}&end_timestamp={end_timestamp}&average={average}&fields={fields}'
    df_list = []  # List to store dataframes
    latest_end_timestamp = 0  # Track the latest end timestamp
    num_iterations = math.ceil(last_day_of_month / average_limits.get(average))
    for loop_num in range(1, num_iterations + 1):
        start_day = int((last_day_of_month / num_iterations) * (loop_num - 1) + 1)
        end_day = int((last_day_of_month / num_iterations) * loop_num)
        message = f'sensor id: {sensor_id} from day {start_day} to {end_day}, loop {loop_num} of {num_iterations}'
        print(message)
        # Adjust end_day if it exceeds the actual last day of the month
        if end_day > last_day_of_month:
            end_day = last_day_of_month
        start_timestamp = int(datetime(yr, mnth, start_day, 0, 0, 1).timestamp())
        end_timestamp = int(datetime(yr, mnth, end_day, 23, 59, 59).timestamp())
        # Adjust end_timestamp based on the latest end_timestamp
        if latest_end_timestamp > start_timestamp:
            start_timestamp = latest_end_timestamp + 1
        params = {
            'fields': "rssi,uptime,humidity,temperature,pressure,voc,"
                        "pm1.0_atm_a,pm1.0_atm_b,pm2.5_atm_a,pm2.5_atm_b,pm10.0_atm_a,pm10.0_atm_b,"
                        "pm1.0_cf_1_a,pm1.0_cf_1_b,pm2.5_cf_1_a,pm2.5_cf_1_b,pm10.0_cf_1_a,pm10.0_cf_1_b,"
                        "0.3_um_count,0.5_um_count,1.0_um_count,2.5_um_count,5.0_um_count,10.0_um_count",
            'average': average,
            'ID': sensor_id,
            'start_timestamp': start_timestamp,
            'end_timestamp': end_timestamp
        }
        url: str = root_url.format(**params)
        cols: List[str] = ['time_stamp', 'time_stamp_pacific'] + [col for col in params['fields'].split(',')] + ['pm25_epa'] + ['Ipm25']
        try:
            response = session.get(url)
        except Exception as e:
            logging.exception('get_data error')
            return pd.DataFrame()
        if response.ok:
            url_data = response.content
            json_data = json.loads(url_data)
            df_temp = pd.DataFrame(json_data['data'], columns=json_data['fields'])
            if loop_num < (num_iterations) and df_temp.empty:
                continue
            elif loop_num == (num_iterations) and df_temp.empty:
                return pd.DataFrame()
            else:
                df_temp = df_temp.fillna('')
                df_temp['time_stamp'] = pd.to_datetime(df_temp['time_stamp'], unit='s')
                df_temp['time_stamp_pacific'] = df_temp['time_stamp'].dt.tz_localize('UTC').dt.tz_convert('US/Pacific')
                df_temp['time_stamp'] = df_temp['time_stamp'].dt.strftime('%m/%d/%Y %H:%M:%S')
                df_temp['time_stamp_pacific'] = df_temp['time_stamp_pacific'].dt.strftime('%m/%d/%Y %H:%M:%S')
                df_temp['Ipm25'] = df_temp.apply(
                    lambda x: AQI.calculate(x['pm2.5_atm_a'], x['pm2.5_atm_b']),
                    axis=1
                    )
                df_temp['pm25_epa'] = df_temp.apply(
                            lambda x: EPA.calculate(x['humidity'], x['pm2.5_cf_1_a'], x['pm2.5_cf_1_b']),
                            axis=1
                            )
                df_list.append(df_temp)  # Append dataframe to the list
                latest_end_timestamp = end_timestamp  # Update the latest end timestamp
        else:
            logging.exception('get_data() response not ok')
            sleep(10)
        if len(df_list) > 0:
            df = pd.concat(df_list, ignore_index=True)  # Concatenate dataframes
            df = df[cols]  # Reorder columns
            df = df.sort_values('time_stamp')  # Sort by time_stamp
    return df


def write_data(df, client, DOCUMENT_NAME, k, WRITE_CSV=False):
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
        worksheet_name = k
        try:
            # open the Google Sheets output worksheet and write the data
            spreadsheet = client.open(DOCUMENT_NAME)
        except gspread.exceptions.SpreadsheetNotFound as e:
            message = f'Creatimg Google Spreadsheet "{DOCUMENT_NAME}"'
            print(message)
            client.create(DOCUMENT_NAME)
            spreadsheet = client.open(DOCUMENT_NAME)
            spreadsheet.share(config.google_account, perm_type='user', role='writer')
        try:
            sheet = spreadsheet.worksheet(worksheet_name)
            sheet.clear()
            sheet.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option='USER_ENTERED')
            break
        except gspread.exceptions.WorksheetNotFound as e:
            message = f'Creating Google Sheet "{worksheet_name}"'
            print(message)
            print()
            spreadsheet = client.open(DOCUMENT_NAME)
            sheet = spreadsheet.add_worksheet(title=worksheet_name, rows=100, cols=31)
            sheet.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option='USER_ENTERED')
            break
        except gspread.exceptions.APIError as e:
            logging.exception('gspread error in write_data()')
            attempts += 1
            if attempts < MAX_ATTEMPTS:
                sleep(60)
            else:
                logging.exception('gspread error in write_data() max attempts reached')
    try:
        sheet = spreadsheet.worksheet('Sheet1')
        spreadsheet.del_worksheet(sheet)
    except gspread.exceptions.WorksheetNotFound as e:
        pass
    # Write the data to Google Sheets 
    if WRITE_CSV is True:
        try:
            df.to_csv(output_pathname, index=True, header=True)
        except Exception as e:
            logging.exception('write_data() error writing csv')


def main():
    args = get_arguments()
    yr = args.yr
    mnth = args.mnth
    start_time = datetime.now()
    if args.sensor_name is not None:
        try:
            df = get_data(config.sensors_current[args.sensor_name]['ID'], yr, mnth)
        except KeyError as e:
            message = f'Invalid sensor name: {args.sensor_name}, exiting...'
            print(message)
            print()
            exit()
        if len(df.index) > 0:
            DOCUMENT_NAME = f'pa_history_single_{args.sensor_name}_{yr}_{mnth}'
            write_data(df, client, DOCUMENT_NAME, args.sensor_name, config.WRITE_CSV)
    else:
        loop_num = 0
        for k, v in config.sensors_current.items():
            loop_num += 1
            message = f'Getting data for sensor {k} for {calendar.month_name[mnth]} {yr}, {loop_num} of {len(config.sensors_current)}' 
            print(message)
            df = get_data(v['ID'], yr, mnth)
            #print(df)
            print()
            if len(df.index) > 0:
                DOCUMENT_NAME = f'pa_history_{yr}_{mnth}'
                write_data(df, client, DOCUMENT_NAME, k, config.WRITE_CSV)
            sleep(60)
            end_time = datetime.now()
            time_per_loop = (end_time - start_time) / loop_num
            time_remaining = time_per_loop * (len(config.sensors_current) - loop_num)
            print(f'Time per loop: {time_per_loop} / Time remaining: {time_remaining}')


if __name__ == "__main__":
    main()