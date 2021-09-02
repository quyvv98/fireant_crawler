import csv
import datetime

import constant

my_list = []
result = ""
date_filter = datetime.datetime.strptime('2020-06-03', constant.DATE_FORMAT)

# with open('1D/total_filter.csv', 'a') as f:
# with open('1D/total.csv', 'r') as f1:
with open('1D/total.csv', 'r') as f1:
    reader = csv.reader(f1)
    for i, row in enumerate(reader):
        if i == 0:
            print(row)
            row_str = ','.join(row) + "\n"
            result += row_str
            # f.write(row_str)
            continue
        date = datetime.datetime.strptime(row[0], constant.DATE_TIME_FORMAT)
        if date < date_filter:
            print(row)
            row_str = ','.join(row) + "\n"
            result += row_str
            # f.write(row_str)

# with open('1D/total_filter.csv', 'w') as f:
with open('1D/total_filter.csv', 'w') as f:
    f.write(result)
