import os, sys, threading
from dotenv import load_dotenv
from datetime import datetime as dt
import logging
from binance.client import Client
from binance.websockets import BinanceSocketManager

# config log level
logging.basicConfig(filename='bot.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filemode='w')

# read .env props
logging.info("welcome! started bot ...........")
load_dotenv()
api_key = os.getenv('APIKEY')
api_secret = os.getenv('APISECRET')
symbols = os.getenv('SUBSCRIPTIONS').split(',')
timeout = int(os.getenv('WSTIMEOUT'))
logging.info("auth config successfully read")

# create client and test REST API
client = Client(api_key, api_secret)
if not client.ping() == {}:
    logging.error("failed to ping the server. check network!")
    sys.exit(-1)
logging.debug("server ping successful")
time_res = client.get_server_time()
logging.debug("server time %s", str(dt.fromtimestamp(int(time_res['serverTime'])/1000)))
status = client.get_system_status()
if not status['status'] == 0:
    logging.error("server not in operational level. received status %s", status['msg'])
    sys.exit(-1)
logging.debug("server operating normally")

# create the websocket client
bm = BinanceSocketManager(client, user_timeout=timeout)

klines = dict()
processors = dict()
listeners =  dict()


def process_klines(s):
    # todo: build the data structure here
    klines[s] = client.get_historical_klines(s, Client.KLINE_INTERVAL_1MINUTE, "99 minutes ago UTC")
    logging.info('processing %d candles', len(klines[s]))


def process_ws_message(msg):
    print("stream: {}-{} data: {}".format(msg['s'], msg['e'], str(msg)))


# fetch 1 minute klines for the last day up until now for each symbol
for s in symbols:
    processors[s+'_klines'] = threading.Thread(target=process_klines, args=(s,))
    processors[s+'_klines'].start()
    processors[s+'_klines'].join()
    listeners[s] = bm.start_symbol_ticker_socket(s, process_ws_message)

logging.debug('processed klines')
# then start the socket manager
bm.start()
