'''
Program that scrapes the South Coast Air Quality Management District (SCAQMD) AQ Details - HistoricalData website for air pollutant data.
'''
# James S. Lucas 20230712
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


def date_range(args):
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
    edgeOption.use_chromium = True
    edgeOption.add_argument('start-maximized')
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
    sleep(2)
    driver.find_element('name', 'toExcel').click()
    sleep(3)


def move_data(args, pollutant):
    '''
    Moves the downloaded CSV file to the appropriate storage folder.

    Args:
    args (argparse.Namespace): The arguments parsed from the command line.
    pollutant (str): The name of the pollutant to be scraped.

    Returns:
    None
    '''
    sleep(2)
    download_path = Path(constants.DOWNLOAD_DIRECTORY)
    storage_folder = f'{args.yr}-{str(args.mnth).zfill(2)}'
    storage_path = Path(constants.MATRIX5) / storage_folder
    os.makedirs(storage_path, exist_ok=True)
    shutil.move(download_path / 'GridViewExport.csv', storage_path / f'LE_REF_{pollutant}.csv')


def main():
    args = get_arguments()
    driver = open_site(constants.SCAQMD_SITE)
    pollutants = ('PM2.5', 'WD', 'WS', 'T', 'O3', 'NO2', 'CO')
    for pollutant in pollutants:
        get_data(driver, args, constants.SCAQMD_STATION, pollutant)
        move_data(args, pollutant)
        sleep(4)
    driver.quit()


if __name__ == "__main__":
    main()