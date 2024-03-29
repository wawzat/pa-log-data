'''
Program that scrapes the South Coast Air Quality Management District (SCAQMD) AQ Details - HistoricalData website for air pollutant data.
'''
# James S. Lucas 20230812
import os
from selenium import webdriver
from selenium.webdriver.edge import service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
import pandas as pd
from pathlib import Path
import shutil
from datetime import datetime, timedelta
from time import sleep
import calendar
import argparse
import constants


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
            return argparse.ArgumentTypeError(f'Must be an integer in the range [{self.imin}, {self.imax}]')
        elif self.imin is not None:
            return argparse.ArgumentTypeError(f'Must be an integer >= {self.imin}')
        elif self.imax is not None:
            return argparse.ArgumentTypeError(f'Must be an integer <= {self.imax}')
        else:
            return argparse.ArgumentTypeError('Must be an integer')


def get_arguments():
    parser = argparse.ArgumentParser(
    description='Get SCAQMD Data for a given month and year',
    prog='aqmd_get_data.py',
    usage='%(prog)s [-m <month>] [-y <year>]',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    g=parser.add_argument_group(title='arguments',
            description='''            -m, --month   Optional. The month to get data for. If not provided, current month will be used.
            -y, --year    Optional. The year to get data for. If not provided, current year will be used.   ''')
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

    args = parser.parse_args()
    return(args)


def remove_download_data():
    '''
    Removes any previous leftover AQMD data files in the download directory.

    Args:
    None

    Returns:
    None
    '''
    for p in Path(constants.DOWNLOAD_DIRECTORY).glob("GridViewExport*.csv"):
        p.unlink()


def date_range(args):
    '''
    Calculates "from" and "to" dates that are the last day of the previous month and the first day of the following month for a given month and year.

    Args:
    args (argparse.Namespace): The argparse Namespace object containing the month and year arguments.

    Returns:
    tuple: A tuple containing the last day of the previous month and first day of the following month in the format '%m/%d/%Y'.
    '''
    run_date = datetime.strptime(f'{args.mnth}/01/{args.yr}', '%m/%d/%Y')
    from_date = (run_date - timedelta(days=1)).strftime('%m/%d/%Y')
    to_date = (run_date + timedelta(days=calendar.monthrange(run_date.year, run_date.month)[1])).strftime('%m/%d/%Y')
    return from_date, to_date


def open_site(site):
    '''
    Opens a website using Microsoft Edge browser and returns the driver object.

    Args:
    site (str): The URL of the website to be opened.

    Returns:
    webdriver.Edge: The driver object for the opened website.
    '''
    edgeOption = webdriver.EdgeOptions()
    edgeOption.add_experimental_option('detach', True)
    edgeOption.add_experimental_option('prefs', {'download.default_directory': constants.DOWNLOAD_DIRECTORY, 'download.prompt_for_download': False})
    edgeOption.use_chromium = True
    edgeOption.add_argument('--headless=new')
    edgeOption.add_argument("--guest")
    #edgeOption.add_argument('start-maximized')
    edgeOption.binary_location = r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe'
    s=service.Service(constants.EDGE_WEBDRIVER_PATH)
    # The following line suppresses an error: usb_service_win.cc:415...
    edgeOption.add_experimental_option('excludeSwitches', ['enable-logging'])
    driver = webdriver.Edge(service=s, options=edgeOption)
    driver.get(site)
    return driver


def get_data(driver, args, station, pollutant):
    '''
    Scrapes the South Coast Air Quality Management District (SCAQMD) AQ Details - HistoricalData website for air pollutant data.

    Args:
    driver (webdriver.Edge): The driver object for the opened website.
    args (argparse.Namespace): The arguments parsed from the command line.
    pollutant (str): The name of the pollutant to be scraped.

    Returns:
    None
    '''
    from_date, to_date = date_range(args)

    pollutant_dropdown = Select(driver.find_element('name', 'AQIVar'))
    pollutant_dropdown.select_by_visible_text(pollutant)

    station_dropdown = Select(driver.find_element('name', 'stationDropDownList'))
    station_dropdown.select_by_visible_text(station)

    from_date_picker = driver.find_element('name', 'fdate')
    from_date_picker.clear()
    from_date_picker.send_keys(from_date)

    to_date_picker = driver.find_element('name', 'tdate')
    to_date_picker.clear()
    to_date_picker.send_keys(to_date)

    driver.find_element('name', 'searchVariButn').click()
    sleep(1)
    driver.find_element('name', 'toExcel').click()


def check_download_exists(download_path):
    """
    The scraper is supposed to download a CSV file named 'GridViewExport.csv' to the specified download path.
    This function checks if the file 'GridViewExport.csv' exists (has succesfully finished downloading).
    It will attempt to check for the file a maximum of 100 times, with a 0.1 second delay between each attempt.
    If the file is not found after all attempts, it raises a FileNotFoundError.

    Parameters:
    download_path (Path): The path where the file is expected to be downloaded.

    Returns:
    file_found (bool): True if the file is found, False otherwise.
    """
    file_found = False
    attempts = 0
    max_attempts = 100
    # Ensure file has completed downloading before moving it
    while not file_found and attempts < max_attempts:
        if Path(download_path / 'GridViewExport.csv').is_file():
            file_found = True
        else:
            attempts += 1
            sleep(.1)
    if not file_found:
        raise FileNotFoundError('Data file download not found')
    return file_found


def move_data(args, pollutant):
    '''
    Moves the downloaded CSV file to the appropriate storage folder.

    Args:
    args (argparse.Namespace): The arguments parsed from the command line.
    pollutant (str): The name of the pollutant to be scraped.

    Returns:
    None
    '''
    download_path = Path(constants.DOWNLOAD_DIRECTORY)
    download_file_exists = check_download_exists(download_path)
    if download_file_exists:
        storage_folder = f'{args.yr}-{str(args.mnth).zfill(2)}'
        storage_path = Path(constants.STORAGE_ROOT_PATH) / storage_folder
        os.makedirs(storage_path, exist_ok=True)
        shutil.move(download_path / 'GridViewExport.csv', storage_path / f'LE_REF_{pollutant}.csv')


def main():
    args = get_arguments()
    remove_download_data()
    driver = open_site(constants.SCAQMD_SITE)
    pollutants = ('PM2.5', 'WD', 'WS', 'T', 'O3', 'NO2', 'CO')
    for pollutant in pollutants:
        get_data(driver, args, constants.SCAQMD_STATION, pollutant)
        move_data(args, pollutant)
        print(f'{pollutant} data downloaded for {args.mnth}/{args.yr}')
        sleep(.4)
    driver.quit()


if __name__ == "__main__":
    main()