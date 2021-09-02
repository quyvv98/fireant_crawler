import logging
import sys
from signalrcore.hub_connection_builder import HubConnectionBuilder

import constant
import crawl_handler


def input_with_default(input_text, default_value):
    value = input(input_text.format(default_value))
    return default_value if value is None or value.strip() == "" else value


def subcribe_connection():
    # server_url = input_with_default('Enter your server url(default: {0}): ', local_url)
    server_url = constant.SERVER_URL
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    hub_connection = HubConnectionBuilder() \
        .with_url(server_url, options={"verify_ssl": False}) \
        .configure_logging(logging.INFO, socket_trace=True, handler=handler) \
        .with_automatic_reconnect({
        "type": "interval",
        "keep_alive_interval": 10,
        "intervals": [1, 3, 5, 6, 7, 87, 3]
    }).build()
    hub_connection.on_open(lambda: print("connection opened and handshake received ready to send messages"))
    hub_connection.on_close(lambda: print("connection closed"))

    return hub_connection


hub_connection = subcribe_connection()

crawl_handler.get_all_stock_ohlcs(hub_connection)

sys.exit(0)
