#!/usr/bin/env python3
"""
Combine and merge multiple spreadsheets into one.
"""
# James S. Lucas - 20230701

import os
import pandas as pd
from pathlib import Path
import constants

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
    description='Combine and merge multiple spreadsheets into one.',
    prog='xl_merge.py',
    usage='%(prog)s [-m <month>] [-y <year>]',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    g=parser.add_argument_group(title='arguments',
            description='''            -m, --month   Optional. The month to get data for. If not provided, current month will be used.
            -y, --year    Optional. The year to get data for. If not provided, current year will be used.
            -s, --sensor  Optional. Sensor Name. If not provided, constants.py sensors_current will be used.    ''')
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


def main():
    args = get_arguments()
    root_path = Path(constants.MATRIX5) / f'{args.yr}-{args.mnth}'
    file_list = list(root_path.iterdir())
    exclude_list = ('combined_summarized_xl.xlsx', 'combined_sheets_xl.xlsx')
    for filename in exclude_list:
        if os.path.exists(root_path / filename):
            file_list.remove(root_path / filename)
    df_list = []
    for filename in file_list:
        print(filename)
        df = pd.read_excel(filename)
        df_list.append(df)
    with pd.ExcelWriter(root_path / "combined_sheets_xl.xlsx",
                            engine='xlsxwriter',
                            engine_kwargs={'options': {'strings_to_numbers': True}}
                            ) as writer:
        df_combined = pd.concat(df_list, ignore_index=True)
        df_combined.to_excel(writer, sheet_name='combined', index=False)
        format_spreadsheet(writer, 'combined')
        for df in df_list:
            if not df.empty:
                sheet_name = root_path.name
                sensor_index = str(df['name'].iloc[0])
                df.to_excel(writer, sheet_name=sensor_index, index=False)
                format_spreadsheet(writer, sensor_index)
    print()
    print(f'Combined {len(df_list)} Excel files into {root_path / "combined_sheets_xl.xlsx"}')

if __name__ == '__main__':
    main()