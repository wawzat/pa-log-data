"""
This program provides functionality to list, consolidate, and export Google Sheets data to Excel. 
It uses a Google Sheets service account to access the spreadsheets and the gspread library to interact with them. 
The program takes command line arguments to specify which sheets to copy and how to consolidate them. 
The consolidated data can be exported to an Excel file. 
The program also provides a function to list all available spreadsheets and their IDs. 

Usage:
    python gsheets_to_xl.py [-m <month>] [-y <year>] [-c <consolidate>] [-x <xl>] [-l <list>]

Arguments:
    -m, --month: Integer of the month of the sheet to copy.
    -y, --year: Year of the sheet to copy.
    -c, --consolidate: Argument to consolidate all of the sheets to the all_data sheet.
    -x, --xl: Argument to copy all of the sheets to the all_data sheet and export to Excel. 
    -l, --list: Argument to list all of the sheets.
"""
# James S. Lucas 20230613
import os
from datetime import datetime
from time import sleep
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import argparse
import pathlib
import pandas as pd
import constants
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')


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
    description='List, consolidate and export from Google Sheets ServiceAccount.',
    prog='gsheets_to_xl.py',
    usage='%(prog)s [-m <month>] [-y <year>] [-c <consolidate>] [-x <xl>] [-l <list>]',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    g=parser.add_argument_group(title='arguments',
            description='''            
            -m, --month        Optional. Month of the sheet to process. Process all sheets in the service account if omitted.
            -y, --year         Optional. Year of the sheet to process. Process all sheets in the service account if omitted.
            -c, --Consolidate  Optional. Consolidate all of the sheets to a new "all_data" sheet.
            -x, --xl           Optional. Consolidate all of the sheets to a new "all_data" sheet and then export to Excel. 
            -l, --list         Optional. List the sheets only.                                                      ''')
    g.add_argument('-m', '--month',
                    type=IntRange(1, 12),
                    default=0,
                    dest='mnth',
                    help=argparse.SUPPRESS)
    g.add_argument('-y', '--year',
                    type=IntRange(2015, datetime.now().year),
                    default=0,
                    dest='yr',
                    help=argparse.SUPPRESS)
    g.add_argument('-c', '--consolidate', action='store_true',
                    dest='consolidate',
                    help=argparse.SUPPRESS)
    g.add_argument('-x', '--xl', action='store_true',
                    dest='xl',
                    help=argparse.SUPPRESS)
    g.add_argument('-l', '--list', action='store_true',
                    dest='list_sheets',
                    help=argparse.SUPPRESS)

    args = parser.parse_args()
    return(args)


# set the credentials for the Google Sheets service account
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive'
        ]
GSPREAD_SERVICE_ACCOUNT_JSON_PATH = config.get('google', 'GSPREAD_SERVICE_ACCOUNT_JSON_PATH')
if GSPREAD_SERVICE_ACCOUNT_JSON_PATH == '':
    print('Error: GSPREAD_SERVICE_ACCOUNT_JSON_PATH not set in the config.ini file, exiting...')
    exit(1)
creds = ServiceAccountCredentials.from_json_keyfile_name(GSPREAD_SERVICE_ACCOUNT_JSON_PATH, scope)
client = gspread.authorize(creds)


def list_sheets(spreadsheets):
    print()
    print('All Files:')
    for spreadsheet in spreadsheets:
        message = f"Spreadsheet ID: {spreadsheet['id']}, Name: {spreadsheet['name']}"
        print(message)
    print()
    print('History Files:')
    for spreadsheet in spreadsheets:
        if 'history' in spreadsheet['name']:
            message = f"Spreadsheet ID: {spreadsheet['id']}, Name: {spreadsheet['name']}"
            print(message)
    print()


def format_spreadsheet(writer):
    # Set the column formats and widths
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
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


def consolidate_sheets(args, spreadsheet_dict):
    """
    Consolidates data from all sheets in a Google Spreadsheet and adds name and sensor index columns to each row.
    The consolidated data is then updated in the "all_data" sheet of the same Google Spreadsheet.
    If the -x or --xl argument is passed, the consolidated data is exported to an Excel file.
    
    Args:
    - args: An argparse.Namespace object containing the command line arguments passed to the script.
    - spreadsheet_dict: A dictionary containing the name of the Google Spreadsheet to consolidate.
    
    Returns:
    - None
    
    Raises:
    - None
    """
    spreadsheet = client.open(spreadsheet_dict['name'])
    sheets = spreadsheet.worksheets()
    try:
        all_data_sheet = spreadsheet.add_worksheet(title='all_data', rows='1', cols='1')
        print(f'Created sheet {all_data_sheet.title}.')
    except gspread.exceptions.APIError as e:
        if 'addSheet' in str(e):
            all_data_sheet = spreadsheet.worksheet('all_data')
            all_data_sheet.clear()
    header_row = sheets[0].row_values(1)
    header_row.insert(2, 'sensor_index')
    header_row.insert(3, 'name')
    all_data = [header_row]
    # Consolidate data from all other sheets and add name and sensor index columns
    for sheet in sheets:
        if sheet.title != 'all_data':
            data = sheet.get_all_values()
            sheet_name = sheet.title
            # Exclude the header row after the first occurrence
            data = data[1:]
            # Add the name and sensor index columns to each row
            for row in data:
                sensor_index = constants.sensors_current.get(sheet_name, {}).get('ID', '')
                row.insert(2, sensor_index)
                row.insert(3, sheet_name)
            all_data.extend(data)
    # Update the "all_data" sheet with the combined data
    all_data_sheet.update('A1', all_data)
    print(f"Updated sheet {all_data_sheet.title} in {spreadsheet_dict['name']}.")
    if args.xl is True:
            sheet_to_xl(spreadsheet_dict, all_data)


def sheet_to_xl(spreadsheet, all_data):
    """
    Converts the data in `all_data` to a pandas DataFrame and exports it to an Excel file.
    
    Args:
    - spreadsheet: A dictionary containing the name of the spreadsheet to export.
    - all_data: A list containing the data to be exported to Excel.
    
    Returns:
    - None
    
    Raises:
    - None
    """
    name_parts = spreadsheet['name'].split('_')
    year = name_parts[2]
    month = name_parts[3]
    folder_name = f'{year}-{month.zfill(2)}'
    full_path = pathlib.Path(constants.STORAGE_ROOT_PATH, folder_name)
    os.makedirs(full_path, exist_ok=True)
    file_name = full_path / 'combined_summarized_xl.xlsx'
    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(all_data[1:], columns=all_data[0])
    with pd.ExcelWriter(file_name,
                            engine='xlsxwriter',
                            engine_kwargs={'options': {'strings_to_numbers': True}}
                            ) as writer:
    # Export the DataFrame to Excel
        df.to_excel(writer, sheet_name='Sheet1', index=False)
        format_spreadsheet(writer)
    print(f'File {file_name} created.')


def check_sheet_exists(spreadsheets, sheet_name):
    sheet_found = False
    for spreadsheet in spreadsheets:
        if sheet_name in spreadsheet.values():
            sheet_found = True
    return sheet_found


def main():
    args = get_arguments()
    spreadsheets = client.list_spreadsheet_files()
    if args.list_sheets is True:
        if args.mnth == 0 or args.yr == 0:
            list_sheets(spreadsheets)
        elif args.mnth != 0 and args.yr != 0:
            sheet_name = f'pa_history_{args.yr}_{str(args.mnth)}'
            sheet_found = check_sheet_exists(spreadsheets, sheet_name)
            if sheet_found is True:
                print(f'Sheet name: {sheet_name}')
            else:
                print(f'Sheet name: {sheet_name} not found.')
    if args.consolidate is True or args.xl is True:
        if args.mnth == 0 or args.yr == 0:
            for spreadsheet in spreadsheets:
                if 'history' in spreadsheet['name']:
                    consolidate_sheets(args, spreadsheet)
                    print('Sleeping for 60 seconds to avoid rate limiting.')
                    sleep(60)
        elif args.mnth != 0 and args.yr != 0:
            sheet_name = f'pa_history_{args.yr}_{str(args.mnth)}'
            sheet_found = check_sheet_exists(spreadsheets, sheet_name)
            if sheet_found is True:
                spreadsheet = {'name': sheet_name}
                consolidate_sheets(args, spreadsheet)
            else:
                print(f'Sheet name: {sheet_name} not found, exiting...')


if __name__ == '__main__':
    main()