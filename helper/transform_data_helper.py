import datetime

import constant
from helper import time_helper


def convert_fireant_2_entrade_data(stocks, symbol, resolution):
    entrade_stocks = []
    for stock in stocks:
        print(stock['date'])
        created_time, created_time_str = time_helper.format_utc_time(stock['date'])

        created_time_stamp = datetime.datetime.timestamp(created_time)
        entrade = stock
        entrade['timestamp'] = int(created_time_stamp)
        entrade['time'] = created_time_str
        now = datetime.datetime.now()
        entrade['last_updated'] = int(datetime.datetime.timestamp(now))
        entrade['symbol'] = symbol
        entrade['resolution'] = resolution
        entrade['open'] = round(entrade['open'], 2)
        entrade['high'] = round(entrade['high'], 2)
        entrade['low'] = round(entrade['low'], 2)
        entrade['close'] = round(entrade['close'], 2)

        del entrade['date']
        entrade_stocks.append(entrade)
    return entrade_stocks


def convert_json_to_csv_type(data):
    csv_data = f"time,timestamp,symbol,open,high,low,close,volume,resolution,last_updated\n"
    body = ""
    for stock in data:
        res = 'DAY' if stock['resolution'] == '1D' else 'MIN1'
        row = f"{stock['time']},{stock['timestamp']},{stock['symbol']}," \
              f"{stock['open']},{stock['high']},{stock['low']},{stock['close']},{stock['volume']}," \
              f"{res},{stock['last_updated']}\n"
        body += row
    csv_data += body
    return csv_data, body
