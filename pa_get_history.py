#!/usr/bin/env python3
"""
This program retrieves historical data from PurpleAir sensors and adds it to a Google Sheets document, CSV, Excel or all three. 
It uses the PurpleAir API to retrieve sensor data for a given sensor ID and time frame. If a single sensor name is not provided the list
of sensors is retrieved from constants.py and the program loops through the list of sensors, retrieving data for each sensor.
The sensor data is then uploaded to a Google Sheets document using the Google Sheets API.

The program requires a Google Sheets service account JSON file and a PurpleAir API key to function properly. 
The service account JSON file should be stored in a secure location and the path to the file should be specified in the `config.ini` file. 
The PurpleAir API key should also be specified in the `config.ini` file.

The program can be run from the command line with the following arguments:
    -m, --month: Integer of the month to get data for.
    -y, --year: The year to get data for.
    -s, --sensor: Optional. The name of a sensor to get data for.
    -o, --output: Optional. The output format. If not provided, output will be written to a CSV file.
    -a, --average: Optional. The number of minutes to average. If not provided, 30 minutes will be used.

The program contains the following functions:
    - get_arguments(): Parses command line arguments and returns them as a Namespace object.
    - get_data(sensor_id, yr, mnth): Queries the PurpleAir API for sensor data for a given sensor ID and time frame, and returns the data as a pandas DataFrame.
"""
# James S. Lucas - 20230629

import sys
import os
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
import constants
from configparser import ConfigParser

# Read the configuration file
config = ConfigParser()
config.read('config.ini')

# Setup exception logging
format_string = '%(name)s - %(asctime)s : %(message)s'
logging.basicConfig(filename='pa_get_history_error.log',
                    format = format_string)

session = requests.Session()
retry = Retry(connect=5, backoff_factor=1.0)
adapter = HTTPAdapter(max_retries=retry)
PURPLEAIR_READ_KEY = config.get('purpleair', 'PURPLEAIR_READ_KEY')
if PURPLEAIR_READ_KEY == '':
    logging.error('Error: PurpleAir API read key not set in config.ini. Exiting.')
    print('Error: PurpleAir API read key not set in config.ini. Exiting.')
    sys.exit(1)
session.headers.update({'X-API-Key': PURPLEAIR_READ_KEY})
session.mount('http://', adapter)
session.mount('https://', adapter)


# set the credentials for the Google Sheets service account
scope: List[str] = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive'
                    ]
GSPREAD_SERVICE_ACCOUNT_JSON_PATH = config.get('google', 'GSPREAD_SERVICE_ACCOUNT_JSON_PATH')
if GSPREAD_SERVICE_ACCOUNT_JSON_PATH == '':
    logging.error('Error: Google Sheets service account JSON path is not set in config.ini. Exiting.')
    print('Google Sheets service account JSON path is not set in config.ini. Exiting.')
    sys.exit(1)
creds = ServiceAccountCredentials.from_json_keyfile_name(GSPREAD_SERVICE_ACCOUNT_JSON_PATH, scope)
client = gspread.authorize(creds)


# Custom argparse type representing a bounded int
# Credit pallgeuer https://stackoverflow.com/questions/14117415/how-can-i-constrain-a-value-parsed-with-argparse-for-example-restrict-an-integ
class IntRange:

    def __init__(self, imin=None, imax=None):
        self.imin = imin
        self.imax = imax

    def __call__(self, arg):
        try:
            value = int(arg)
        except ValueError:
            raise self.exception()
        if (self.imin is not None and value < self.imin) or (self.imax is not None and value > self.imax):
            raise self.exception()
        return value

    def exception(self):
        if self.imin is not None and self.imax is not None:
            return argparse.ArgumentTypeError(f"Must be an integer in the range [{self.imin}, {self.imax}]")
        elif self.imin is not None:
            return argparse.ArgumentTypeError(f"Must be an integer >= {self.imin}")
        elif self.imax is not None:
            return argparse.ArgumentTypeError(f"Must be an integer <= {self.imax}")
        else:
            return argparse.ArgumentTypeError("Must be an integer")


def get_arguments():
    parser = argparse.ArgumentParser(
    description='Get PurpleAir Sensor Historical Data.',
    prog='pa_get_history.py',
    usage='%(prog)s [-m <month>] [-y <year>] [-s <sensor>] [-o <output>] [-a <average>]',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    g=parser.add_argument_group(title='arguments',
            description='''            -m, --month   Optional. The month to get data for. If not provided, current month will be used.
            -y, --year    Optional. The year to get data for. If not provided, current year will be used.
            -s, --sensor  Optional. Sensor Name. If not provided, constants.py sensors_current will be used.
            -o, --output  Optional. Output format. If not provided, output will be written to a CSV file. Choices = c, s, x, a 
            -a, --average Optional. Number of minutes to average. If not provided, 30 minutes will be used. Choices = 0, 10, 30, 60, 360, 1440     ''')
    g.add_argument('-o', '--output',
                    type=str,
                    default='c',
                    choices = ['c', 's', 'x', 'a'],
                    metavar='',
                    dest='output',
                    help=argparse.SUPPRESS)
    g.add_argument('-m', '--month',
                    type=IntRange(1, 12),
                    default=datetime.now().month,
                    dest='mnth',
                    help=argparse.SUPPRESS)
    g.add_argument('-y', '--year',
                    type=IntRange(2015, datetime.now().year),
                    default=datetime.now().year,
                    dest='yr',
                    help=argparse.SUPPRESS)
    g.add_argument('-s', '--sensor',
                    type=str,
                    default=None,
                    dest='sensor_name',
                    help=argparse.SUPPRESS)
    g.add_argument('-a', '--average',
                    type=int,
                    default=30,
                    choices = [0, 10, 30, 60, 360, 1440],
                    metavar='',
                    dest='average',
                    help=argparse.SUPPRESS)

    args = parser.parse_args()
    return(args)


def format_spreadsheet(writer, sheet):
    # Set the column formats and widths
    workbook = writer.book
    worksheet = writer.sheets[sheet]
    format1 = workbook.add_format({'num_format': 'm-d-Y h:mm:ss'})
    format2 = workbook.add_format({'num_format': '#,##0.00'})
    format3 = workbook.add_format({'num_format': '#,##0.000'})
    format4 = workbook.add_format({'num_format': '#,##0.0000'})
    format5 = workbook.add_format({'num_format': '#,##0'})
    worksheet.set_column('A:A', 19, format1)
    worksheet.set_column('B:B', 19, format1)
    worksheet.set_column('C:C', 13, format5)
    worksheet.set_column('D:D', 27, format5)
    worksheet.set_column('E:E', 4, format5)
    worksheet.set_column('F:F', 9, format5)
    worksheet.set_column('G:G', 9, format3)
    worksheet.set_column('H:H', 12, format3)
    worksheet.set_column('I:I', 10, format3)
    worksheet.set_column('J:J', 6, format2)
    worksheet.set_column('K:K', 13, format3)
    worksheet.set_column('L:L', 13, format3)
    worksheet.set_column('M:M', 13, format3)
    worksheet.set_column('N:N', 13, format3)
    worksheet.set_column('O:O', 14, format3)
    worksheet.set_column('P:P', 14, format3)
    worksheet.set_column('Q:Q', 13, format3)
    worksheet.set_column('R:R', 13, format3)
    worksheet.set_column('S:S', 13, format3)
    worksheet.set_column('T:T', 13, format3)
    worksheet.set_column('U:U', 14, format3)
    worksheet.set_column('V:V', 14, format3)
    worksheet.set_column('W:W', 13, format4)
    worksheet.set_column('X:X', 13, format4)
    worksheet.set_column('Y:Y', 13, format4)
    worksheet.set_column('Z:Z', 13, format4)
    worksheet.set_column('AA:AA', 13, format4)
    worksheet.set_column('AB:AB', 14, format4)
    worksheet.set_column('AC:AC', 10, format3)
    worksheet.set_column('AD:AD', 6, format5)
    worksheet.freeze_panes(1, 0)


def get_data(sensor_name, sensor_id, yr, mnth, average) -> pd.DataFrame:
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
    # minutes: days
    average_limits = {
        0: 2,
        10: 3,
        30: 7,
        60: 14,
        360: 90,
        1440: 365
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
        cols: List[str] = ['time_stamp', 'time_stamp_pacific', 'sensor_index', 'name'] + [col for col in params['fields'].split(',')] + ['pm25_epa'] + ['Ipm25']
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
                df_temp['sensor_index'] = sensor_id
                df_temp['name'] = sensor_name
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
        else:
            df = pd.DataFrame()
            logging.exception('get_data() df_list empty')
            print('df_list empty')
    return df


def write_data(df, client, DOCUMENT_NAME, sensor_id, output, base_output_file_name, yr, mnth):
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

    if output == 's' or output == 'a':
        MAX_ATTEMPTS: int = 4
        attempts: int = 0
        SLEEP_DURATION = 90
        while attempts < MAX_ATTEMPTS:
            worksheet_name = sensor_id
            try:
                # open the Google Sheets output worksheet and write the data
                spreadsheet = client.open(DOCUMENT_NAME)
            except gspread.exceptions.SpreadsheetNotFound as e:
                message = f'Creatimg Google Spreadsheet "{DOCUMENT_NAME}"'
                print(message)
                client.create(DOCUMENT_NAME)
                spreadsheet = client.open(DOCUMENT_NAME)
                google_account = config.get('google', 'google_account')
                if google_account == '':
                    logging.error('Error: Google account not set in config.ini, exiting...')
                    print('Error: Google account not set in config.ini, exiting...')
                    sys.exit(1)
                spreadsheet.share(google_account, perm_type='user', role='writer')
            try:
                sheet = spreadsheet.worksheet(worksheet_name)
                sheet.clear()
                sheet.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option='USER_ENTERED')
                message = f'Writing data to Sheet {worksheet_name} in Google Workbook {DOCUMENT_NAME}'
                print(message)
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
                    sleep(SLEEP_DURATION)
                    SLEEP_DURATION += 90
                else:
                    logging.exception('gspread error in write_data() max attempts reached')
        try:
            sheet = spreadsheet.worksheet('Sheet1')
            spreadsheet.del_worksheet(sheet)
        except gspread.exceptions.WorksheetNotFound as e:
            pass
    if output == 'c' or output == 'a':
        if sys.platform == 'win32':
            output_pathname = Path(constants.MATRIX5) / f'{base_output_file_name}.csv'
        elif sys.platform == 'linux':
            output_pathname = Path.cwd() / f'{base_output_file_name}.csv'
        try:
            df.to_csv(output_pathname, index=False, header=True)
            message = f'Created {output_pathname.name} in {output_pathname.parent}'
            print(message)
        except Exception as e:
            logging.exception('write_data() error writing Excel file')
    if output == 'x' or output == 'a':
        if sys.platform == 'win32':
            folder_name = f'{yr}-{str(mnth).zfill(2)}'
            if not os.path.isdir(Path(constants.MATRIX5) / folder_name):
                os.mkdir(Path(constants.MATRIX5) / folder_name)
            output_pathname = Path(constants.MATRIX5) / folder_name / f'{base_output_file_name}.xlsx'
        elif sys.platform == 'linux':
            output_pathname = Path.cwd() / f'{base_output_file_name}.xlsx'
        try:
            with pd.ExcelWriter(output_pathname,
                                engine='xlsxwriter',
                                engine_kwargs={'options': {'strings_to_numbers': True}}
                                ) as writer:
                # Export the DataFrame to Excel
                df.to_excel(writer, sheet_name=sensor_id, index=False)
                format_spreadsheet(writer, sensor_id)
            message = f'Created or updated {output_pathname.name} in {output_pathname.parent}'
            print(message)
        except Exception as e:
            logging.exception('write_data() error writing Excel file')
            print('Error writing Excel file')


def main():
    args = get_arguments()
    start_time = datetime.now()
    if args.sensor_name is not None:
        try:
            df = get_data(args.sensor_name, constants.sensors_current[args.sensor_name]['ID'], args.yr, args.mnth, args.average)
        except KeyError as e:
            message = f'Invalid sensor name: {args.sensor_name}, exiting...'
            print(message)
            print()
            exit()
        if len(df.index) > 0:
            DOCUMENT_NAME = f'pa_history_single_{args.sensor_name}_{args.yr}_{args.mnth}'
            base_output_file_name = f'pa_history_single_{args.sensor_name}_{args.yr}_{args.mnth}'
            write_data(df, client, DOCUMENT_NAME, args.sensor_name, args.output, base_output_file_name, args.yr, args.mnth)
    else:
        loop_num = 0
        for k, v in constants.sensors_current.items():
            loop_num += 1
            message = f'Getting data for sensor {k} for {calendar.month_name[args.mnth]} {args.yr}, {loop_num} of {len(constants.sensors_current)}' 
            print(message)
            df = get_data(k, v['ID'], args.yr, args.mnth, args.average)
            #print(df)
            print()
            if len(df.index) > 0:
                DOCUMENT_NAME = f'pa_history_{args.yr}_{args.mnth}'
                base_output_file_name = f'pa_history_{k}_{args.yr}_{args.mnth}'
                write_data(df, client, DOCUMENT_NAME, k, args.output, base_output_file_name, args.yr, args.mnth)
            sleep(60)
            end_time = datetime.now()
            time_per_loop = (end_time - start_time) / loop_num
            time_remaining = time_per_loop * (len(constants.sensors_current) - loop_num)
            print(f'Time per loop: {time_per_loop} / Time remaining: {time_remaining}')


if __name__ == "__main__":
    main()