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
    -o, --output: Optional. The output format. CSV, Google Sheets, XL, All. Defaults to CSV.
    -a, --average: Optional. The number of minutes to average. If not provided, 30 minutes will be used.

The program contains the following functions:
    - get_arguments(): Parses command line arguments and returns them as a Namespace object.
    - get_data(sensor_id, yr, mnth): Queries the PurpleAir API for sensor data for a given sensor ID and time frame, and returns the data as a pandas DataFrame.
"""
# James S. Lucas - 20240715

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
logger = logging.getLogger(__name__)  
logger.setLevel(logging.WARNING)
file_handler = logging.FileHandler('pa_get_history_error.log')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

session = requests.Session()
retry = Retry(total=10, backoff_factor=1.0)
adapter = HTTPAdapter(max_retries=retry)
PURPLEAIR_READ_KEY = config.get('purpleair', 'PURPLEAIR_READ_KEY_GET_HISTORY')
if PURPLEAIR_READ_KEY == '':
    logger.error('Error: PurpleAir API read key not set in config.ini. Exiting.')
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
    logger.error('Error: Google Sheets service account JSON path is not set in config.ini. Exiting.')
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


# Argprse action to add a prefix character to an argument
class PrefixCharAction(argparse.Action):
    def __init__(self, option_strings, dest, prefix_char=None, **kwargs):
        self.prefix_char = prefix_char
        super(PrefixCharAction, self).__init__(option_strings, dest, **kwargs)
    
    def __call__(self, parser, namespace, values, option_string=None):
        if self.prefix_char is not None:
            values = f"{self.prefix_char}{values}"
        setattr(namespace, self.dest, values)


def get_arguments():
    parser = argparse.ArgumentParser(
    description='Get PurpleAir Sensor Historical Data.',
    prog='pa_get_history.py',
    usage='%(prog)s [-m <month>] [-y <year>] [-s <sensor>] [-o <output>] [-a <average>]',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    g=parser.add_argument_group(title='arguments',
            description='''            -m, --month      Optional. The month to get data for. If not provided, current month will be used.
            -y, --year       Optional. The year to get data for. If not provided, current year will be used.
            -d, --directory  Optional. A directory suffix to append to the default directory name. An underscore is automatically prefixed. Default YYYY-MM.
            -s, --sensor     Optional. Sensor Name. If not provided, constants.py sensors_current will be used.
            -o, --output     Optional. Output format. Default is CSV file. CSV, Google Sheets, XL, All. Choices = c, s, x, a 
            -a, --average    Optional. Number of minutes to average. If not provided, 30 minutes will be used. Choices = 0, 10, 30, 60, 360, 1440
            -f, --fields     Optional. Fields to retrieve. Default is all fields. Choices are; (a)ll, (c)ustom, (m)inimal          ''')
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
    g.add_argument('-d', '--directory',
                    type=str,
                    default=None,
                    dest='directory',
                    action=PrefixCharAction,
                    prefix_char='_',
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
    g.add_argument('-f', '--fields',
                    type=str,
                    default='a',
                    choices = ['a', 'c', 'm'],
                    metavar='',
                    dest='fields',
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


def get_data(sensor_name, sensor_id, yr, mnth, average, fields_to_get) -> pd.DataFrame:
    """
    Retrieves historical data from the PurpleAir API for a specific sensor.

    Args:
        sensor_name (str): The name of the sensor.
        sensor_id (int): The ID of the sensor.
        yr (int): The year of the data to retrieve.
        mnth (int): The month of the data to retrieve.
        average (int): The time interval (in minutes) over which to average the data.
        fields_to_get (str): The type of fields to retrieve ('a' for all fields, 'c' for custom fields, 'm' for minimal fields).

    Returns:
        pd.DataFrame: A DataFrame containing the retrieved data.

    """
    last_day_of_month = calendar.monthrange(yr, mnth)[1]
    if datetime.now().month == mnth and datetime.now().year == yr and datetime.now().day < last_day_of_month:
        last_day_of_range = datetime.now().day
    else:
        last_day_of_range = calendar.monthrange(yr, mnth)[1]
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
    num_iterations = math.ceil(last_day_of_range / average_limits.get(average))
    for loop_num in range(1, num_iterations + 1):
        start_day = int((last_day_of_range / num_iterations) * (loop_num - 1) + 1)
        end_day = int((last_day_of_range / num_iterations) * loop_num)
        message = f'sensor id: {sensor_id} from day {start_day} to {end_day}, loop {loop_num} of {num_iterations}'
        print(message)
        # Adjust end_day if it exceeds the actual last day of the month
        if end_day > last_day_of_range:
            end_day = last_day_of_range
        start_timestamp = int(datetime(yr, mnth, start_day, 0, 0, 1).timestamp())
        end_timestamp = int(datetime(yr, mnth, end_day, 23, 59, 59).timestamp())
        # Adjust end_timestamp based on the latest end_timestamp
        if latest_end_timestamp > start_timestamp:
            start_timestamp = latest_end_timestamp + 1
        if fields_to_get == 'a':
            fields = constants.ALL_FIELD_LIST
        elif fields_to_get == 'c':
            fields = constants.CUSTOM_FIELD_LIST
        elif fields_to_get == 'm':
            fields = constants.MINIMAL_FIELD_LIST
        params = {
            'fields': fields,
            'average': average,
            'ID': sensor_id,
            'start_timestamp': start_timestamp,
            'end_timestamp': end_timestamp
        }
        url: str = root_url.format(**params)
        cols: List[str] = ['time_stamp', 'time_stamp_pacific', 'sensor_index', 'name'] + [col for col in params['fields'].split(',')] + ['pm25_epa'] + ['Ipm25']
        try:
            response = session.get(url)
        except requests.exceptions.RequestException as req_err:
            logger.exception(f'Request exception: {req_err}')
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
                            lambda x: EPA.calculate(x['humidity_a'], x['pm2.5_cf_1_a'], x['pm2.5_cf_1_b']),
                            axis=1
                            )
                df_list.append(df_temp)  # Append dataframe to the list
                latest_end_timestamp = end_timestamp  # Update the latest end timestamp
        else:
            logger.exception('get_data() response not ok')
            sleep(10)
        if len(df_list) > 0:
            df = pd.concat(df_list, ignore_index=True)  # Concatenate dataframes
            df = df[cols]  # Reorder columns
            df = df.sort_values('time_stamp')  # Sort by time_stamp
        else:
            df = pd.DataFrame()
            logger.exception('get_data() df_list empty')
            print('df_list empty')
    return df


def write_data(df, client, DOCUMENT_NAME, sensor_id, output, BASE_OUTPUT_FILE_NAME, yr, mnth, directory_suffix=None):
    """
    Writes data to Google Sheets, CSV, and/or Excel file.

    Args:
        df (pandas.DataFrame): The DataFrame containing the data to be written.
        client: The Google Sheets client object.
        DOCUMENT_NAME (str): The name of the Google Spreadsheet.
        sensor_id (str): The ID of the sensor.
        output (str): The output format. Possible values are 's' (Google Sheets), 'c' (CSV), 'x' (Excel), or 'a' (all).
        BASE_OUTPUT_FILE_NAME (str): The base name of the output file.
        yr (int): The year.
        mnth (int): The month.
        directory_suffix (str, optional): The suffix to be added to the directory name. Defaults to None.
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
                message = f'Creating Google Spreadsheet "{DOCUMENT_NAME}"'
                print(message)
                client.create(DOCUMENT_NAME)
                spreadsheet = client.open(DOCUMENT_NAME)
                google_account = config.get('google', 'google_account')
                if google_account == '':
                    logger.error('Error: Google account not set in config.ini, exiting...')
                    print('Error: Google account not set in config.ini, exiting...')
                    sys.exit(1)
                spreadsheet.share(google_account, perm_type='user', role='writer')
            except gspread.exceptions.APIError as e:
                attempts += 1
                logger.exception(f'gspread error in write_data() attempt #{attempts} of {MAX_ATTEMPTS}')
                if attempts < MAX_ATTEMPTS:
                    sleep(SLEEP_DURATION)
                    SLEEP_DURATION += 90
                else:
                    logger.exception('gspread error in write_data() max attempts exceeded')
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
                attempts += 1
                logger.exception(f'gspread error in write_data(): attempt #{attempts} of {MAX_ATTEMPTS}')
                if attempts < MAX_ATTEMPTS:
                    sleep(SLEEP_DURATION)
                    SLEEP_DURATION += 90
                else:
                    logger.exception('gspread error in write_data() max attempts exceeded')
        try:
            sheet = spreadsheet.worksheet('Sheet1')
            spreadsheet.del_worksheet(sheet)
        except gspread.exceptions.WorksheetNotFound as e:
            pass
    folder_name = f'{yr}-{str(mnth).zfill(2)}{directory_suffix}'
    if output == 'c' or output == 'a':
        if sys.platform == 'win32':
            os.makedirs(Path(constants.STORAGE_ROOT_PATH) / folder_name, exist_ok=True)
            output_pathname = Path(constants.STORAGE_ROOT_PATH) / folder_name / f'{BASE_OUTPUT_FILE_NAME}.csv'
        elif sys.platform == 'linux':
            output_pathname = Path.cwd() / f'{BASE_OUTPUT_FILE_NAME}.csv'
        try:
            df.to_csv(output_pathname, index=False, header=True)
            message = f'Created {output_pathname.name} in {output_pathname.parent}'
            print(message)
        except Exception as e:
            logger.exception('write_data() error writing Excel file')
    if output == 'x' or output == 'a':
        if sys.platform == 'win32':
            os.makedirs(Path(constants.STORAGE_ROOT_PATH) / folder_name, exist_ok=True)
            output_pathname = Path(constants.STORAGE_ROOT_PATH) / folder_name / f'{BASE_OUTPUT_FILE_NAME}.xlsx'
        elif sys.platform == 'linux':
            output_pathname = Path.cwd() / folder_name / f'{BASE_OUTPUT_FILE_NAME}.xlsx'
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
            logger.exception('write_data() error writing Excel file')
            print('Error writing Excel file')


def main():
    args = get_arguments()
    start_time = datetime.now()
    if args.sensor_name is not None:
        try:
            df = get_data(args.sensor_name, constants.sensors_current[args.sensor_name]['ID'], args.yr, args.mnth, args.average, args.fields)
        except KeyError as e:
            message = f'Invalid sensor name: {args.sensor_name}, exiting...'
            print(message)
            print()
            exit()
        if len(df.index) > 0:
            DOCUMENT_NAME = f'pa_history_single_{args.sensor_name}_{args.yr}_{str(args.mnth).zfill(2)}'
            BASE_OUTPUT_FILE_NAME = f'pa_history_single_{args.sensor_name}_{args.yr}_{str(args.mnth).zfill(2)}'
            write_data(df, client, DOCUMENT_NAME, args.sensor_name, args.output, BASE_OUTPUT_FILE_NAME, args.yr, args.mnth, args.directory)
    else:
        loop_num = 0
        for k, v in constants.sensors_current.items():
            loop_num += 1
            message = f'Getting data for sensor {k} for {calendar.month_name[args.mnth]} {args.yr}, {loop_num} of {len(constants.sensors_current)}' 
            print(message)
            df = get_data(k, v['ID'], args.yr, args.mnth, args.average, args.fields)
            #print(df)
            print()
            if len(df.index) > 0:
                DOCUMENT_NAME = f'pa_history_{args.yr}_{str(args.mnth).zfill(2)}'
                BASE_OUTPUT_FILE_NAME = f'pa_history_{k}_{args.yr}_{str(args.mnth).zfill(2)}'
                write_data(df, client, DOCUMENT_NAME, k, args.output, BASE_OUTPUT_FILE_NAME, args.yr, args.mnth, args.directory)
            sleep(60)
            end_time = datetime.now()
            time_per_loop = (end_time - start_time) / loop_num
            time_remaining = time_per_loop * (len(constants.sensors_current) - loop_num)
            print(f'Time per loop: {time_per_loop} / Time remaining: {time_remaining}')
    session.close()


if __name__ == "__main__":
    main()