import json

import constant
from helper import time_helper


def mark_symbol_done(symbol, resolution, start, end, data_crawled):
    f = open('./data/success_symbols.json', "r")
    stocks = json.load(f)
    stocks[resolution].append({
        "symbol": symbol,
        "start": start,
        "end": end,
        "data_crawled": data_crawled,
        "time": time_helper.get_current_time_str(constant.DATE_TIME_FORMAT)
    })

    with open('./data/success_symbols.json', "w") as outfile:
        # raw_data = json.dumps(stocks)
        outfile.write(json.dumps(stocks))


def mark_symbol_processing(symbol, resolution):
    f = open('./data/success_symbols.json', "r")
    stocks = json.load(f)

    for i, stock in enumerate(stocks[resolution]):
        if stock['code'] == symbol:
            del stocks[resolution][i]

    with open('./data/success_symbols.json', "w") as outfile:
        # raw_data = json.dumps(stocks)
        outfile.write(json.dumps(stocks))


def is_symbol_done(symbol, resolution):
    f = open('./data/success_symbols.json', "r")
    stocks = json.load(f)
    is_existed = any(symbol == stock['symbol'] for stock in stocks[resolution])
    return is_existed
