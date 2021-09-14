import logging
import sys
import time
from signalrcore.hub_connection_builder import HubConnectionBuilder

import constant


def react_to_message(msg):
    bars = msg.result
    print(bars)


def input_with_default(input_text, default_value):
    value = input(input_text.format(default_value))
    return default_value if value is None or value.strip() == "" else value


# server_url = input_with_default('Enter your server url(default: {0}): ', local_url)
# username = input_with_default('Enter your username (default: {0}): ', "mandrewcito")

server_url = constant.SERVER_URL
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
hub_connection = HubConnectionBuilder() \
    .with_url(server_url, options={"verify_ssl": False}) \
    .configure_logging(logging.DEBUG, socket_trace=True, handler=handler) \
    .with_automatic_reconnect({
    "type": "interval",
    "keep_alive_interval": 10,
    "intervals": [1, 3, 5, 6, 7, 87, 3]
}).build()

hub_connection.on_open(lambda: print("connection opened and handshake received ready to send messages"))
hub_connection.on_close(lambda: print("connection closed"))

hub_connection.start()
message = None

# Do login

time.sleep(10)
hub_connection.send("GetBars", ["HPG", "1", "2018-03-05 08:30:00", "2018-03-6 16:30:00"], react_to_message)
# hub_connection.send("GetBars", ["HPG", "1D", "2010-03-05", "2020-03-11"], react_to_message)

time.sleep(300)
hub_connection.stop()

sys.exit(0)
