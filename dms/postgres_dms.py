import psycopg2
from configparser import ConfigParser
from typing import Dict


def get_config(filename='./dms/database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db: dict[str, str] = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


class Connector:
    conn = None
    cursor = None

    def __new__(cls, *args, **kwargs):
        # read connection parameters
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        cls.conn, cls.cursor = cls.connect()
        return cls

    def insert_stock_ohlcs(self, stocks):
        print(f"Start insert stock ohlcs with data: {stocks}")
        params = [tuple(stock.values()) for stock in stocks]
        sql = "INSERT INTO stock_ohlc( open, high, low, close, volume,timestamp, time,last_updated,symbol,resolution ) " \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self.cursor.executemany(sql, params)
        self.conn.commit()
        print(f"Success insert {self.cursor.rowcount} stock ohlcs")
        pass

    def close_connector(self):

        if self.cursor is not None:
            self.cursor.close()

        if self.conn is not None:
            self.conn.close()

    @staticmethod
    def connect():
        params = get_config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        return conn, cursor


def test():
    data = [
        {
            "open": 48.5,
            "high": 48.55,
            "low": 48.45,
            "close": 48.55,
            "volume": 168000,
            "timestamp": 1630305540.0,
            "time": "2021-08-30 13:39:00",
            "last_updated": 1630305660.0,
            "symbol": "HPG",
            "resolution": "1"
        },
        {
            "open": 48.55,
            "high": 48.55,
            "low": 48.5,
            "close": 48.55,
            "volume": 97800,
            "timestamp": 1630305600.0,
            "time": "2021-08-30 13:40:00",
            "last_updated": 1630305660.0,
            "symbol": "HPG",
            "resolution": "1"
        },
        {
            "open": 48.55,
            "high": 48.55,
            "low": 48.5,
            "close": 48.55,
            "volume": 194600,
            "timestamp": 1630305660.0,
            "time": "2021-08-30 13:41:00",
            "last_updated": 1630305660.0,
            "symbol": "HPG",
            "resolution": "1"
        }
    ]

    db = Connector()

    db.insert_stock_ohlcs(db,  data)
    print("Success")


# test()
