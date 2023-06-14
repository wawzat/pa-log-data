"""
Google Sheets Spreadsheets created by gspread are owned by the Service Account that created them.
They cannot be deleted by the web inteface.
This program lists all the Google Sheets files associated with a given service account and optionally deletes them. 
It requires a JSON key file for the service account and the `gspread` and `oauth2client` libraries to be installed.

Usage:
    python gsheets_mgmt.py [-d]

Arguments:
    -d, --delete: Use this argument to delete files. If not specified, only the list of files will be printed.

Example:
    python gsheets_mgmt.py -d

This will list all the Google Sheets files associated (owned or shared) with the service account and optionally delete 
all the files owned by the Service Account with 'pa_history' in their name, except for those with '401K' or 'data' in their name.
"""
# James S. Lucas 20230612
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import argparse
from configparser import ConfigParser

# read the config file
config = ConfigParser()
config.read('config.ini')

def get_arguments():
    parser = argparse.ArgumentParser(
    description='List and optionally delete files from Google Sheets ServiceAccount.',
    prog='gsheets_mgmt.py',
    usage='%(prog)s [-d <delete>]',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    g=parser.add_argument_group(title='arguments',
            description='''    -d, --delete  Use this argument to delete files.      ''')
    g.add_argument('-d', '--delete', action='store_true',
                    dest='delete',
                    help=argparse.SUPPRESS)

    args = parser.parse_args()
    return(args)
args = get_arguments()


# set the credentials for the Google Sheets service account
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive'
        ]
GSPREAD_SERVICE_ACCOUNT_JSON_PATH = config.get('google', 'GSPREAD_SERVICE_ACCOUNT_JSON_PATH')
if GSPREAD_SERVICE_ACCOUNT_JSON_PATH == '':
    print('Error: GSPREAD_SERVICE_ACCOUNT_JSON_PATH is not set in config.ini, exiting...')
    exit(1)
creds = ServiceAccountCredentials.from_json_keyfile_name(GSPREAD_SERVICE_ACCOUNT_JSON_PATH, scope)
client = gspread.authorize(creds)

spreadsheets = client.list_spreadsheet_files()

print()
print('All Files:')
for spreadsheet in spreadsheets:
    message = f"Spreadsheet ID: {spreadsheet['id']}, Name: {spreadsheet['name']}"
    print(message)
print()
print('History Files:')
for spreadsheet in spreadsheets:
    if 'pa_history' in spreadsheet['name']:
        message = f"Spreadsheet ID: {spreadsheet['id']}, Name: {spreadsheet['name']}"
        print(message)
        if args.delete is True:
            if '401K' not in spreadsheet['name'] or 'data' not in spreadsheet['name']:
                try:
                    client.del_spreadsheet(spreadsheet['id'])
                except gspread.exceptions.APIError as e:
                    if 'insufficientFilePermissions' in str(e):
                        pass
print()