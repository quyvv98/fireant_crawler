import time

import constant
from helper import time_helper, transform_data_helper, symbol_status_helper
import json
# from dms import postgres_dms
from log import logger


class CrawlFireAntHandler:

    def __init__(self, hub_connection, symbol, /, end_time, time_start, distance_time, res, time_sleep=5, debug=False):
        self.hub_connection = hub_connection
        self.symbol = symbol
        self.res = res
        self.distance_time = distance_time
        self.end_time = end_time
        self.time_start = time_start
        # self.dms = postgres_dms.Connector()
        self.time_sleep = time_sleep
        self.debug = debug
        if res == "1D":
            self.time_format = constant.DATE_FORMAT
        else:
            self.time_format = constant.DATE_TIME_FORMAT

        self.current_time_processing = ''
        self.end_time_processing = ''
        self.is_finished = False
        self.data_crawled = []
        self.MAX_REQ_MSG_IN_CONNECTION = 3
        self.processing = False

    def insert_stock_ohlcs(self, msg):
        logger.info(f"Finish crawl a part of data with symbol: {self.symbol}. start: {self.current_time_processing},"
                    f"end: {self.end_time_processing}")
        self.processing = True
        if msg.error is not None:
            logger.error(f"Fireant return error: {msg.error}")
            raise ValueError(f'Fireant return error: {msg.error}')

        bars = msg.result['bars']
        if len(bars) == 0 and self.end_time_processing >= self.end_time:
            self.is_finished = True

        entrade_data = transform_data_helper.convert_fireant_2_entrade_data(bars, self.symbol, self.res)
        # self.dms.insert_stock_ohlcs(self.dms, entrade_data)

        self.data_crawled += entrade_data

        logger.info(
            f"Success handle a part of data with symbol: {self.symbol}. start: {self.current_time_processing},"
            f"end: {self.end_time_processing}, added_count: {len(bars)}, total: {len(self.data_crawled)}")
        # f" added_data: {bars}")

    def get_stock_ohlcs(self):
        # log.info("[INFO] Start get stock ohlcs of symbol", symbol)
        self.current_time_processing = self.time_start

        while True:
            logger.info("Start hub connection")
            self.hub_connection.start()
            # Do login

            for i in range(self.MAX_REQ_MSG_IN_CONNECTION):
                self.end_time_processing = time_helper.next_time(self.current_time_processing, self.time_format,
                                                                 distance=self.distance_time, res=self.res)
                logger.info(
                    f"Start crawl a part of data with symbol: {self.symbol}. start: {self.current_time_processing}, "
                    f"end: {self.end_time_processing}")
                self.processing = False
                try:
                    time.sleep(self.time_sleep)
                    self.hub_connection.send("GetBars",
                                             [self.symbol, self.res, self.current_time_processing,
                                              self.end_time_processing],
                                             self.insert_stock_ohlcs)
                    time.sleep(self.time_sleep)

                except Exception as e:
                    logger.error(f"Exception with {e}")
                    break
                if not self.processing:
                    logger.info("Detected missing handle. Restart handle time")
                    break
                if self.res == "1" and time_helper.is_over_close_time(self.end_time_processing):
                    self.current_time_processing = time_helper.next_exchange_day(self.end_time_processing)
                else:
                    self.current_time_processing = time_helper.next_time(self.end_time_processing, self.time_format,
                                                                         res=self.res, distance=1)
                if self.current_time_processing > self.end_time:
                    self.is_finished = True
                    break
                logger.log(f"{' ' * 50}^.^")

            time.sleep(self.time_sleep)
            logger.info("Stop hub connection ")
            self.hub_connection.stop()
            if self.is_finished:
                # Writing to csv file
                data_format_csv, data = transform_data_helper.convert_json_to_csv_type(self.data_crawled)
                mode = "a" if bool(constant.CRAWL_START_TIME) else "w"
                with open(f"data/{self.res}/{self.symbol}_{self.res}.csv", mode) as outfile:
                    outfile.write(data_format_csv)
                if not self.debug:
                    with open(f"data/{self.res}/total.csv", "a") as outfile:
                        outfile.write(data)

                    symbol_status_helper.mark_symbol_done(self.symbol, self.res, self.time_start,
                                                          self.end_time_processing,
                                                          len(self.data_crawled))
                break

        logger.info(f"[SUCCESS] Got all stock ohlcs of symbol {self.symbol}. Total: {len(self.data_crawled)} \n")


def get_all_stock_ohlcs(hub_connection):
    logger.log(f"\n{'=' * 120}" * 3)
    logger.info(f"Start crawl ohlc stocks")
    priority_stocks = []
    stocks = []
    try:
        f = open('./data/priority_stocks.json')
        priority_stocks = json.load(f)
    except:
        logger.info("Don't have priority stocks")
    if len(priority_stocks) > 0:
        stocks += priority_stocks

    f = open('./data/stocks.json')
    all_stocks = json.load(f)

    stocks += all_stocks

    for i, stock in enumerate(stocks):
        symbol = stock['code']
        start_time = stock['listedDate']
        if bool(constant.CRAWL_START_TIME):
            start_time = constant.CRAWL_START_TIME
        logger.info(f" Loading {i + 1}/{len(stocks)}")
        resolution = 1
        prev_year = time_helper.get_prev_year_time_str()
        # distance time = 1 Day
        # get_stock_ohlcs_with_resolution(hub_connection, symbol=symbol, res="1",
        #                                 start_time=f"{prev_year} {constant.TIME_OPEN}",
        #                                 distance_time=60 * 8, time_sleep=5,
        #                                 end_time=time_helper.get_current_time_str(constant.DATE_TIME_FORMAT))

        # resolution = 1D
        # distance time = 10 years
        get_stock_ohlcs_with_resolution(hub_connection, symbol=symbol, res="1D",
                                        start_time=start_time,
                                        distance_time=30 * 12 * 10, time_sleep=5,
                                        end_time=time_helper.get_current_time_str(constant.DATE_FORMAT))

    logger.log(f"\n{'=' * 120}" * 2)
    logger.log(f"\n{'=' * 55} HAPPY NEW YEAR {'=' * 50} ")
    logger.log(f"\n{'=' * 120}" * 2)


# time_helper.get_current_time_str()

def get_stock_ohlcs_with_resolution(hub_connection, /, symbol, distance_time, time_sleep, res, start_time, end_time):
    logger.info(f"Checking symbol {symbol} with resolution {res}")
    if symbol_status_helper.is_symbol_done(symbol, res):
        return

    logger.log(f"{'-' * 119}")
    logger.info(f"Start handle stock {symbol} with resolution = {res}, start time = {start_time}")
    crawl_handler = CrawlFireAntHandler(hub_connection, symbol, res=res, distance_time=distance_time,
                                        end_time=end_time,
                                        time_start=start_time, time_sleep=time_sleep)
    crawl_handler.get_stock_ohlcs()
