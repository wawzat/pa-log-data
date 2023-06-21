"""
Program to reduce the size of a Google Sheets file by deleting rows older than a specified number of days.
The program will prompt the user to confirm the deletion of the rows.
The program will not delete rows from the current month.
The program will not delete rows from the current year.

Arguments:
    -m, --month  The number of the month to clean.

Returns:
    None

Notes:
    If the month argument is not specified, the current month will be used.
"""

import gspread
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
import argparse
from time import sleep
import constants
from configparser import ConfigParser

# read the config file
config = ConfigParser()
config.read('config.ini')


# set the credentials for the Google Sheets service account
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
GSPREAD_SERVICE_ACCOUNT_JSON_PATH = config.get('google', 'GSPREAD_SERVICE_ACCOUNT_JSON_PATH')
creds = ServiceAccountCredentials.from_json_keyfile_name(GSPREAD_SERVICE_ACCOUNT_JSON_PATH, scope)
client = gspread.authorize(creds)


def is_continuous(rows_to_delete):
    if not rows_to_delete:
        return False
    for i in range(1, len(rows_to_delete)):
        if rows_to_delete[i] != rows_to_delete[i-1] + 1:
            return False
    return True


def delete_rows(sheet, sheet_name, rows_to_delete, num_rows, first_date, last_date):
    if is_continuous(rows_to_delete) is True and len(rows_to_delete) > 0:
        if len(rows_to_delete) == 1:
            sheet.delete_row(rows_to_delete[0])
        elif len(rows_to_delete) > 1:
            sheet.delete_rows(min(rows_to_delete), max(rows_to_delete))
        message = f'{str(num_rows)} rows from {first_date.strftime("%m/%d/%Y %H:%M:%S")} to {last_date.strftime("%m/%d/%Y %H:%M:%S")} have been deleted from "{sheet_name}".'
        print(message)
    else:
        print('Rows to delete are not continuous, no data deleted.')


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
    description='Reduce Google Sheets file size.',
    prog='sheet_size_reduce.py',
    usage='%(prog)s [-m <month>] [-d <days>] [-s <sheet>] [-a] [-w]',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    g=parser.add_argument_group(title='arguments',
            description='''    -m, --month  Optional. The month to clean. Default is current month.
            -d, --days      Optional. The number of days to keep. Default is 21.
            -s, --sheet     Optional. The name of the sheet to clean. Default is constants.py LOCAL_WORKSHEET_NAME.
            -a, --all       Optional. Clean all sheets (local and regional).
            -w, --warnings  Optional. Don't show warnings.        ''')
    g.add_argument('-m', '--month',
                    type=IntRange(1, 12),
                    default=datetime.now().month,
                    dest='mnth',
                    help=argparse.SUPPRESS)
    g.add_argument('-d', '--days',
                    type=int,
                    default=21,
                    dest='days_to_keep',
                    help=argparse.SUPPRESS)
    g.add_argument('-s', '--sheet',
                    type=str,
                    default=constants.LOCAL_WORKSHEET_NAME,
                    dest='sheet_name',
                    help=argparse.SUPPRESS)
    g.add_argument('-a', '--all', action='store_true',
                    dest='all',
                    help=argparse.SUPPRESS)
    g.add_argument('-w', '--warnings', action='store_true',
                    dest='warnings',
                    help=argparse.SUPPRESS)
    args = parser.parse_args()
    return(args)
args = get_arguments()

sheets = ()
if args.all is True:
    for k, v in constants.BBOX_DICT.items():
        sheets += (k,)
else:
    sheets = (args.sheet_name,)

# Define the current month and year
now = datetime.now()
month_to_clean = args.mnth
year_to_clean = now.year


for index, sheet_name in enumerate(sheets):
    # Set up the worksheet
    MAX_ATTEMPTS: int = 3
    attempts: int = 0
    while attempts < MAX_ATTEMPTS:
        try:
            sheet = client.open(constants.DOCUMENT_NAME).worksheet(sheet_name)
            break
        except gspread.exceptions.WorksheetNotFound as e:
            print(' ')
            message = f'The Google Sheet "{sheet_name}" could not be found, exiting...'
            print(message)
            exit()
        except gspread.exceptions.APIError as e:
            attempts += 1
            if attempts < MAX_ATTEMPTS:
                sleep(60)
            else:
                print(e)
                print(' ')
                print('Maximum gspread read attempts exceeded for sheet "{sheet_name}", exiting...')
                exit()
    # Get all the rows in the worksheet
    attempts: int = 0
    while attempts < MAX_ATTEMPTS:
        try:
            rows = sheet.get_all_records()
            break
        except gspread.exceptions.APIError as e:
            attempts += 1
            if attempts < MAX_ATTEMPTS:
                sleep(60)
            else:
                print(e)
                print(' ')
                print('Maximum gspread read attempts exceeded, exiting...')
                exit()
    # Keep track of the row numbers to delete
    rows_to_delete = []
    # Loop through the rows, starting from the earliest
    for i in range(len(rows)):
        # Get the timestamp for the row
        timestamp = datetime.strptime(rows[i]['time_stamp'], '%m/%d/%Y %H:%M:%S')
        # Check if the row is older than the keep_days threshold
        if now - timestamp > timedelta(days=args.days_to_keep):
            # Check if the row is from a prior month
            if timestamp.month < month_to_clean or timestamp.year < year_to_clean:
                # Add the row number to the list of rows to delete
                rows_to_delete.append(i+2)
            else:
                # Update last_date if the row is within the keep_days threshold
                last_date = datetime.strptime(rows[i]['time_stamp'], '%m/%d/%Y %H:%M:%S')
        else:
            # Stop looping once we reach the first row within the keep_days threshold
            break
    num_rows = len(rows_to_delete)
    if num_rows == 0:
        message = f'No rows to delete from "{sheet_name}".'
        print(message)
        continue
    first_date = datetime.strptime(rows[rows_to_delete[0]-2]['time_stamp'], '%m/%d/%Y %H:%M:%S')
    last_date = datetime.strptime(rows[rows_to_delete[num_rows-1]-2]['time_stamp'], '%m/%d/%Y %H:%M:%S')
    if args.warnings is False:
        message = f'You are about to delete {num_rows} rows from {first_date.strftime("%m/%d/%Y %H:%M:%S")} to {last_date.strftime("%m/%d/%Y %H:%M:%S")} from sheet "{sheet_name}".' 
        print(message)
        print('Do you want to continue? (y/n)')
        answer = input()
        if answer != 'y':
            if index != len(sheets) - 1:
                message = f'No rows deleted from "{sheet_name}", continuing...'
                print(message)
                continue
            else:
                message = f'No rows deleted from "{sheet_name}", no additional sheets to process, exiting...'
                print(message)
                exit()
        else:
            delete_rows(sheet, sheet_name, rows_to_delete, num_rows, first_date, last_date)
    else:
        delete_rows(sheet, sheet_name, rows_to_delete, num_rows, first_date, last_date)
        sleep(30)