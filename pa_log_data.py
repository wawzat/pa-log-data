#!/usr/bin/env python3
# Regularly Polls Purpleair api for outdoor sensor data for sensors within defined rectangular geographic regions at a defined interval.
# Appends data to Google Sheets
# Processes data
# James S. Lucas - 20231106

import sys
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import json
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from datetime import datetime, timedelta
from time import sleep
from tabulate import tabulate
import logging
from conversions import AQI
import constants
from configparser import ConfigParser
import argparse
from urllib3.exceptions import ReadTimeoutError
from google.auth.exceptions import TransportError

# Read config file
config = ConfigParser()
config.read('config.ini')

# Gets or creates a logger
logger = logging.getLogger(__name__)  
# set log level
logger.setLevel(logging.WARNING)
# define file handler and set formatter
file_handler = logging.FileHandler('pa_log_data_error.log')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)
# add file handler to logger
logger.addHandler(file_handler)

# Create a logger for urllib3
urllib3_logger = logging.getLogger('urllib3')
urllib3_logger.setLevel(logging.WARNING)
file_handler = logging.FileHandler('pa_log_data_urllib3_log.txt')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)
urllib3_logger.addHandler(file_handler)

# Setup requests session with retry
session = requests.Session()
retry = Retry(total=12, backoff_factor=1.0, status_forcelist=tuple(range(401, 600)))
adapter = HTTPAdapter(max_retries=retry)
PURPLEAIR_READ_KEY = config.get('purpleair', 'PURPLEAIR_READ_KEY_LOG_DATA')
if PURPLEAIR_READ_KEY == '':
    logger.error('Error: PURPLEAIR_READ_KEY not set in config.ini')
    print('ERROR: PURPLEAIR_READ_KEY not set in config.ini')
    sys.exit(1)
session.headers.update({'X-API-Key': PURPLEAIR_READ_KEY})
session.mount('http://', adapter)
session.mount('https://', adapter)

# set the credentials for the Google Sheets service account
scope: list[str] = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive'
                    ]
GSPREAD_SERVICE_ACCOUNT_JSON_PATH = config.get('google', 'GSPREAD_SERVICE_ACCOUNT_JSON_PATH')
if GSPREAD_SERVICE_ACCOUNT_JSON_PATH == '':
    logger.error('Error: GSPREAD_SERVICE_ACCOUNT_JSON_PATH not set in config.ini, exiting...')
    print('Error: GSPREAD_SERVICE_ACCOUNT_JSON_PATH not set in config.ini, exiting...')
    sys.exit(1)
creds = ServiceAccountCredentials.from_json_keyfile_name(GSPREAD_SERVICE_ACCOUNT_JSON_PATH, scope)
client = gspread.authorize(creds)
client.set_timeout(240)


def get_arguments():
    parser = argparse.ArgumentParser(
    description='Log PurpleAir Data.',
    prog='pa_log_data.py',
    usage='%(prog)s [-r <regional>]',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    g=parser.add_argument_group(title='arguments',
            description='''            -r, --regional   Optional. Get Regional data when program first starts.        ''')
    g.add_argument('-r', '--regional',
                    action='store_true',
                    dest='regional',
                    help=argparse.SUPPRESS)
    args = parser.parse_args()
    return args


def retry(max_attempts=3, delay=2, escalation=10, exception=(Exception,)):
    """
    A decorator function that retries a function call a specified number of times if it raises a specified exception.

    Args:
        max_attempts (int): The maximum number of attempts to retry the function call.
        delay (int): The initial delay in seconds before the first retry.
        escalation (int): The amount of time in seconds to increase the delay by for each subsequent retry.
        exception (tuple): A tuple of exceptions to catch and retry on.

    Returns:
        The decorated function.

    Raises:
        The same exception that the decorated function raises if the maximum number of attempts is reached.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exception as e:
                    adjusted_delay = delay + escalation * attempts
                    attempts += 1
                    logger.exception(f'Error in {func.__name__}(): attempt #{attempts} of {max_attempts}')
                    if attempts < max_attempts:
                        sleep(adjusted_delay)
            logger.exception(f'Error in {func.__name__}: max of {max_attempts} attempts reached')
            print(f'Error in {func.__name__}(): max of {max_attempts} attempts reached')
            sys.exit(1)
        return wrapper
    return decorator


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


def get_pa_data(previous_time, bbox: list[float], local) -> pd.DataFrame:
    """
    A function that queries the PurpleAir API for sensor data within a given bounding box and time frame.

    Args:
        previous_time (datetime): A datetime object representing the time of the last query.
        bbox (list[float]): A list of four floats representing the bounding box of the area of interest.
            The order is [northwest longitude, southeast latitude, southeast longitude, northwest latitude].

    Returns:
        A pandas DataFrame containing sensor data for the specified area and time frame. The DataFrame will contain columns
        for the timestamp of the data, the index of the sensor, and various sensor measurements such as temperature,
        humidity, and PM2.5 readings.
    """
    et_since = int((datetime.now() - previous_time + timedelta(seconds=20)).total_seconds())
    root_url: str = 'https://api.purpleair.com/v1/sensors/?fields={fields}&max_age={et}&location_type=0&nwlng={nwlng}&nwlat={nwlat}&selng={selng}&selat={selat}'
    if local:
        fields = 'name,rssi,uptime,pm2.5_atm_a,pm2.5_atm_b'
    else:
        fields = 'name,pm2.5_atm_a,pm2.5_atm_b'
    params = {
        'fields': fields,
        'nwlng': bbox[0],
        'selat': bbox[1],
        'selng': bbox[2],
        'nwlat': bbox[3],
        'et': et_since
    }
    url: str = root_url.format(**params)
    cols: list[str] = ['time_stamp', 'sensor_index'] + [col for col in params['fields'].split(',')]
    try:
        response = session.get(url)
    except requests.exceptions.RequestException as e:
        logger.exception(f'get_pa_data() error: {e}')
        df = pd.DataFrame()
        return df
    if response.ok:
        url_data = response.content
        json_data = json.loads(url_data)
        df = pd.DataFrame(json_data['data'], columns=json_data['fields'])
        df = df.fillna('')
        df['time_stamp'] = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
        df = df[cols]
    else:
        df = pd.DataFrame()
        logger.exception('get_pa_data() response not ok')
    return df


@retry(max_attempts=9, delay=90, escalation=90, exception=(gspread.exceptions.APIError, requests.exceptions.ConnectionError))
def get_gsheet_data(client, DOCUMENT_NAME, in_worksheet_name) -> pd.DataFrame:
    """
    Retrieves data from a Google Sheet specified by the DOCUMENT_NAME and in_worksheet_name parameters.

    Args:
        client (gspread.client.Client): The authorized Google Sheets API client.
        DOCUMENT_NAME (str): The name of the Google Sheet document.
        in_worksheet_name (str): The name of the worksheet within the Google Sheet document.

    Returns:
        A pandas DataFrame containing the data from the specified worksheet.
    """
    in_sheet = client.open(DOCUMENT_NAME).worksheet(in_worksheet_name)
    df = pd.DataFrame(in_sheet.get_all_records())
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes rows from the input DataFrame where the difference between the PM2.5 atmospheric concentration readings
    from two sensors is either greater than or equal to 5 or greater than or equal to 70% of the average of the two readings,
    or greater than 2000.

    Args:
        df (pd.DataFrame): The input DataFrame containing the PM2.5 atmospheric concentration readings from two sensors.

    Returns:
        A new DataFrame with the rows removed where the difference between the PM2.5 atmospheric concentration readings
        from two sensors is either greater than or equal to 5 or greater than or equal to 70% of the average of the two readings,
        or greater than 2000.
    """
    df = df.drop(df[df['pm2.5_atm_a'] > 2000].index)
    df = df.drop(df[df['pm2.5_atm_b'] > 2000].index)
    df = df.drop(df[abs(df['pm2.5_atm_a'] - df['pm2.5_atm_b']) >= 5].index)
    df = df.drop(
        df[abs(df['pm2.5_atm_a'] - df['pm2.5_atm_b']) /
            ((df['pm2.5_atm_a'] + df['pm2.5_atm_b'] + 1e-6) / 2) >= 0.7
        ].index
    )
    return df


def format_data(df: pd.DataFrame, local: bool) -> pd.DataFrame:
    """
    Formats the input DataFrame by rounding the values in certain columns and converting the values in other columns to integers.

    Args:
        df (pd.DataFrame): The input DataFrame to be formatted.

    Returns:
        A new DataFrame with the specified columns rounded or converted to integers.
    """
    if local:
        df[constants.cols_4] = df[constants.cols_4].astype(int)
        df[constants.cols_5] = df[constants.cols_5].astype(int)
        df[constants.cols_6] = df[constants.cols_6].round(2)
        df[constants.cols_7] = df[constants.cols_7].astype(int)
        df = df[constants.local_cols]
    else:
        df[constants.cols_6] = df[constants.cols_6].round(2)
        df[constants.cols_7] = df[constants.cols_7].astype(int)
        df = df[constants.regional_cols]
    return df


@retry(max_attempts=9, delay=90, escalation=90, exception=(
                        gspread.exceptions.APIError,
                        requests.exceptions.ReadTimeout,
                        requests.exceptions.ConnectionError,
                        ReadTimeoutError,
                        TransportError))
def write_data(df, client, DOCUMENT_NAME, worksheet_name, write_mode):
    """
    Writes the input DataFrame to a Google Sheets worksheet.

    Args:
        df (pd.DataFrame): The DataFrame to be written to the worksheet.
        client (gspread.client.Client): The authorized Google Sheets API client.
        DOCUMENT_NAME (str): The name of the Google Sheets document.
        worksheet_name (str): The name of the worksheet to write to.
        write_mode (str): The write mode to use. Can be 'append' or 'update'.

    Returns:
        None
    """
    # open the Google Sheets output worksheet and write the data
    sheet = client.open(DOCUMENT_NAME).worksheet(worksheet_name)
    if write_mode == 'append':
        sheet.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')
    elif write_mode == 'update':
        sheet.clear()
        #sheet.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option='USER_ENTERED')
        sheet.append_rows([df.columns.values.tolist()], value_input_option='USER_ENTERED')
        sheet.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')


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
            - time_stamp_pacific
        - Data is cleaned according to EPA criteria.
    """
    df['Ipm25'] = df.apply(
        lambda x: AQI.calculate(x['pm2.5_atm_a'], x['pm2.5_atm_b']),
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
    local = True
    df = format_data(df, local)
    return df


def process_data(DOCUMENT_NAME, client):
    """
    Process data from Google Sheets sheets for each region. Data is cleaned, summarized and various values are calculated. Data are saved to
    different worksheets in the same Google Sheets document.

    Args:
        DOCUMENT_NAME (str): The name of the Google Sheets document to be processed.
        client: The Google Sheets client object.

    Returns:
        df (pandas.DataFrame): The processed DataFrame.
    """
    write_mode: str = 'update'
    for k, v in constants.BBOX_DICT.items():
        # open the Google Sheets input worksheet and read in the data
        in_worksheet_name: str = k
        out_worksheet_name: str = k + ' Proc'
        df = get_gsheet_data(client, DOCUMENT_NAME, in_worksheet_name)
        if constants.LOCAL_REGION == k:
            # Save the dataframe for later use by the sensor_health() function
            df_local = df.copy()
            local = True
        else:
            local = False
        df['Ipm25'] = df.apply(
            lambda x: AQI.calculate(x['pm2.5_atm_a'], x['pm2.5_atm_b']),
            axis=1
            )
        df['time_stamp'] = pd.to_datetime(
            df['time_stamp'],
            format='%m/%d/%Y %H:%M:%S'
            )
        df = df.set_index('time_stamp')
        df_summarized = df.groupby('name').resample(constants.PROCESS_RESAMPLE_RULE).mean(numeric_only=True)
        df_summarized = df_summarized.reset_index()
        df_summarized['time_stamp_pacific'] = df_summarized['time_stamp'].dt.tz_localize('UTC').dt.tz_convert('US/Pacific')
        df_summarized['time_stamp'] = df_summarized['time_stamp'].dt.strftime('%m/%d/%Y %H:%M:%S')
        df_summarized['time_stamp_pacific'] = df_summarized['time_stamp_pacific'].dt.strftime('%m/%d/%Y %H:%M:%S')
        df_summarized['pm2.5_atm_a'] = pd.to_numeric(df_summarized['pm2.5_atm_a'], errors='coerce').astype(float)
        df_summarized['pm2.5_atm_b'] = pd.to_numeric(df_summarized['pm2.5_atm_b'], errors='coerce').astype(float)
        df_summarized = df_summarized.dropna(subset=['pm2.5_atm_a', 'pm2.5_atm_b'])
        df_summarized.replace('', 0, inplace=True)
        df_summarized = clean_data(df_summarized)
        df_summarized = format_data(df_summarized, local)
        write_data(df_summarized, client, DOCUMENT_NAME, out_worksheet_name, write_mode)
        sleep(90)
    return df_local


def sensor_health(client, df, DOCUMENT_NAME, OUT_WORKSHEET_HEALTH_NAME):
    """
    Calculates the health of each sensor based on the percentage of good readings. Gets maximum error, average signal strength, and uptime.
    Writes the results to a Google Sheet specified by DOCUMENT_NAME and OUT_WORKSHEET_HEALTH_NAME.

    Args:
        client (gspread.client.Client): The authorized Google Sheets API client.
        df (pandas.DataFrame): The DataFrame containing the sensor data.
        DOCUMENT_NAME (str): The name of the Google Sheet document.
        OUT_WORKSHEET_HEALTH_NAME (str): The name of the worksheet to write the sensor health data to.

    Returns:
        None
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
    write_mode: str = 'update'
    out_worksheet_regional_name: str = 'Regional'
    df_regional_stats = pd.DataFrame(columns=['Region', 'Mean', 'Max'])
    for k, v in constants.BBOX_DICT.items():
        worksheet_name = v[1] + ' Proc'
        df = get_gsheet_data(client, DOCUMENT_NAME, worksheet_name)
        if len(df) > 0:
            df['Ipm25'] = pd.to_numeric(df['Ipm25'], errors='coerce')
            df = df.dropna(subset=['Ipm25'])
            df['Ipm25'] = df['Ipm25'].astype(float)
            mean_value = df['Ipm25'].mean().round(2)
            max_value = df['Ipm25'].max().round(2)
            df_regional_stats.loc[len(df_regional_stats)] = [v[2], mean_value, max_value]
            df = pd.DataFrame()
            sleep(90)
        write_data(df_regional_stats, client, DOCUMENT_NAME, out_worksheet_regional_name, write_mode)


def main():
    args = get_arguments()
    five_min_ago: datetime = datetime.now() - timedelta(minutes=5)
    if args.regional:
        for k, v in constants.BBOX_DICT.items():
            if k == constants.LOCAL_REGION:
                local = True
            else:
                local = False
            df = get_pa_data(five_min_ago, constants.BBOX_DICT.get(k)[0], local)
            if len(df.index) > 0:
                write_mode = 'append'
                write_data(df, client, constants.DOCUMENT_NAME, constants.BBOX_DICT.get(k)[1], write_mode)
            else:
                pass
    else:
        local = True
        df = get_pa_data(five_min_ago, constants.BBOX_DICT.get(constants.LOCAL_REGION)[0], local)
        if len(df.index) > 0:
            write_mode = 'append'
            write_data(df, client, constants.DOCUMENT_NAME, constants.BBOX_DICT.get(constants.LOCAL_REGION)[1], write_mode)
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
                local = True
                df_local = get_pa_data(local_start, constants.BBOX_DICT.get(constants.LOCAL_REGION)[0], local)
                if len (df_local.index) > 0:
                    write_mode: str = 'append'
                    write_data(df_local, client, constants.DOCUMENT_NAME, constants.LOCAL_WORKSHEET_NAME, write_mode)
                    sleep(10)
                    df_current = current_process(df_local)
                    write_mode: str = 'update'
                    write_data(df_current, client, constants.DOCUMENT_NAME, constants.CURRENT_WORKSHEET_NAME, write_mode)
                local_start: datetime = datetime.now()
            if regional_et > constants.REGIONAL_INTERVAL_DURATION:
                local = False
                for regional_key in constants.REGIONAL_KEYS:
                    df = get_pa_data(regional_start, constants.BBOX_DICT.get(regional_key)[0], local) 
                    if len(df.index) > 0:
                        write_mode: str = 'append'
                        write_data(df, client, constants.DOCUMENT_NAME, constants.BBOX_DICT.get(regional_key)[1], write_mode)
                    sleep(10)
                regional_start: datetime = datetime.now()
            if process_et > constants.PROCESS_INTERVAL_DURATION:
                df = process_data(constants.DOCUMENT_NAME, client)
                process_start: datetime = datetime.now()
                if len(df.index) > 0:
                    sensor_health(client, df, constants.DOCUMENT_NAME, constants.OUT_WORKSHEET_HEALTH_NAME)
                    regional_stats(client, constants.DOCUMENT_NAME)
        except KeyboardInterrupt:
            sys.exit(0)


if __name__ == "__main__":
    main()