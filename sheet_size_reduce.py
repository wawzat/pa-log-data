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
import config


# set the credentials for the Google Sheets service account
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(
    config.GSPREAD_SERVICE_ACCOUNT_JSON_PATH, scope)
client = gspread.authorize(creds)


def get_arguments():
    parser = argparse.ArgumentParser(
    description='Reduce Google Sheets File Size.',
    prog='pa_get_data',
    usage='%(prog)s [-m <month>] [-d <days>]',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    g=parser.add_argument_group(title='arguments',
            description='''    -m, --month  The number of the month to clean.
            -d, --days   The number of days to keep. ''')
    g.add_argument('-m', '--month',
                    type=int,
                    default=0,
                    dest='mnth',
                    help=argparse.SUPPRESS)
    g.add_argument('-d', '--days',
                    type=int,
                    default=21,
                    dest='days_to_keep',
                    help=argparse.SUPPRESS)
    g.add_argument('-s', '--sheet',
                    type=str,
                    default='TV',
                    dest='sheet_name',
                    help=argparse.SUPPRESS)
    args = parser.parse_args()
    return(args)
args = get_arguments()


# Set up the worksheet
try:
    sheet = client.open(config.DOCUMENT_NAME).worksheet(args.sheet_name)
except gspread.exceptions.WorksheetNotFound as e:
    print(' ')
    message = f'The Google Sheet "{args.sheet_name}" could not be found, exiting...'
    print(message)
    exit()


# Define the current month and year
now = datetime.now()
if args.mnth == 0:
    month_to_clean = now.month
    year_to_clean = now.year
else:
    month_to_clean = args.mnth
    year_to_clean = now.year
# Get all the rows in the worksheet
MAX_ATTEMPTS: int = 3
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
def is_continuous(rows_to_delete):
    if not rows_to_delete:
        return False
    for i in range(1, len(rows_to_delete)):
        if rows_to_delete[i] != rows_to_delete[i-1] + 1:
            return False
    return True
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
        # Stop looping once we reach the first row within the keep_days threshold
        break
print(rows_to_delete)
num_rows = len(rows_to_delete)
if num_rows == 0:
    print('No rows to delete, exiting...')
    exit()
first_date = datetime.strptime(rows[rows_to_delete[0]-2]['time_stamp'], '%m/%d/%Y %H:%M:%S')
last_date = datetime.strptime(rows[rows_to_delete[num_rows-1]-2]['time_stamp'], '%m/%d/%Y %H:%M:%S')
message = f'You are about to delete {num_rows} rows from {first_date.strftime("%m/%d/%Y %H:%M:%S")} to {last_date.strftime("%m/%d/%Y %H:%M:%S")} from sheet "{args.sheet_name}".' 
print(message)
print('Do you want to continue? (y/n)')
answer = input()
if answer == 'y':
    if is_continuous(rows_to_delete) is True and len(rows_to_delete) > 0:
        if len(rows_to_delete) == 1:
            sheet.delete_row(rows_to_delete[0])
        elif len(rows_to_delete) > 1:
            sheet.delete_rows(min(rows_to_delete), max(rows_to_delete))
        message = f'{str(num_rows)} rows from {first_date.strftime("%m/%d/%Y %H:%M:%S")} to {last_date.strftime("%m/%d/%Y %H:%M:%S")} have been deleted from "{args.sheet_name}".'
        print(message)
    else:
        print('Rows to delete are not continuous, no data deleted.')
else:
    print('Exiting...')
    exit()