import csv
import datetime
from helper import time_helper, symbol_status_helper
import constant
import re


def filter_date(file, ignore_symbols):
    result = []
    result_except_current_time = []
    date_filter = datetime.datetime.strptime('2020-06-03', constant.DATE_FORMAT)
    with open(f'1D_copy/{file}.csv', 'r') as f1:
        reader = csv.reader(f1)
        for i, row in enumerate(reader):
            row_str = ','.join(row) + "\n"
            if i == 0:
                print(row)
                result.append(row_str)
                continue
            date = datetime.datetime.strptime(row[0], constant.DATE_TIME_FORMAT)
            if date < date_filter:
                if row[2] in ignore_symbols:
                    continue
                row[0] = time_helper.get_date_of_date_time(row[0])
                # print(row)
                row_str = ','.join(row) + "\n"
                result.append(row_str)
                # row_except_current_time = ','.join(row[:-1])
                # if row[2] == "ADP":
                #     print("Start follow")
                # if row_except_current_time not in result_except_current_time:
                #     result.append(row_str)
                #     result_except_current_time.append(row_except_current_time)
                # else:
                #     print("detected duplicate", row_str)

    with open(f'1D_copy/{file}_filter.csv', 'w') as f:
        f.write(''.join(result))


def format_date(symbol):
    result = ""
    with open(f'1D_copy/{symbol}.csv', 'r') as f1:
        reader = csv.reader(f1)
        for i, row in enumerate(reader):
            if i == 0:
                print(row)
                row_str = ','.join(row) + "\n"
                result += row_str
                # f.write(row_str)
                continue
            row[0] = time_helper.get_date_of_date_time(row[0])
            print(row)
            row_str = ','.join(row) + "\n"
            result += row_str

    with open(f'1D_copy/{symbol}_format_date.csv', 'w') as f:
        f.write(result)


ignore = ['DNC', 'SIP', 'VIX', 'VNM', 'CTB', 'GKM', 'PVI', 'SSI', 'TDB', 'DXG', 'GIC', 'RTB', 'TBC', 'TTN', 'VIH',
          'HAP', 'SZL', 'TCH', 'VGR', 'HTC', 'ADP']
# symbol_status_helper.mark_symbol_processing(ignore)
# symbol_status_helper.mark_symbol_processing()

filter_date('total', ignore)
