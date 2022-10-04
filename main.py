import asyncio
import os, sys, logging
from datetime import datetime as dt

from dotenv import load_dotenv
from binance.client import Client
from binance.streams import BinanceSocketManager

# config logging
logging.basicConfig(filename='bot.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filemode='w')


######################################
# data structures for containing data 
######################################
klines      = dict()
processors  = dict()
listeners   = dict()


###################
# helper functions
###################
def process_ws_message(msg):
    """place holder method for processing the event sent by the exchange. 
    you can use this function as a starting point to implement your trading strategy

    Args:
        msg (dict): event sent by the exchange
    """
    logging.info("stream: {}-{} data: {}".format(msg['s'], msg['e'], str(msg)))


def load_config():
    """loads the configuration from the .env file

    Returns:
        api_key (str): KEY of the binance API secret
        api_secret (str): SECRET of the binance API secret
        subscriptions (list[str]): list of trading pairs to subscribe to 
        timeout (int): socket timeout
    """
    # read .env properties
    load_dotenv()
    # read secrets from the environment
    return os.getenv('APIKEY'), os.getenv('APISECRET'), os.getenv('SUBSCRIPTIONS').split(','), int(os.getenv('WSTIMEOUT'))


async def main():
    # greet
    logging.info("welcome! started bot. reading config ...........")

    # load config
    key, secret, symbols, timeout = load_config()

    if not (key and secret and symbols and timeout):
        logging.error("failed to read auth config")
        sys.exit(1)
    logging.info("auth config successfully read")

    # create client and exit if a connection cannot be established
    client = Client(key, secret)
    if not client.ping() == {}:
        logging.error("failed to ping the server. check network!")
        sys.exit(1)
    logging.debug("server ping successful")

    # check server health
    time_res = client.get_server_time()
    logging.debug("server time %s", str(dt.fromtimestamp(int(time_res['serverTime'])/1000)))

    status = client.get_system_status()
    if not status['status'] == 0:
        logging.error("server not in operational level. received status %s", status['msg'])
        sys.exit(1)
    logging.debug("server operating normally")

    # create the websocket client
    bm = BinanceSocketManager(client, user_timeout=timeout)

    # for each trading pair configured, open a connection and process using the defined function
    for s in symbols:
        # start any sockets here, i.e a trade socket
        ts = bm.trade_socket(s)
        # then start receiving messages
        async with ts as tscm:
            while True:
                res = await tscm.recv()
                process_ws_message(res)
    # finally start the socket manager
    bm.start()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

