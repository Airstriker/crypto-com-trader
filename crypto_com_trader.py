# ------------------------ NOTE !!!! ---------------------------------
# Copyright: Julian Sychowski
# USE venv (create it by running install_crypto_com_trader_pc.sh / install_crypto_com_trader_raspberry.sh !

import os
import json
import sys
import getopt
import traceback
import ntpath
from webhook_bot import WebhookBot
from crypto_com_user_api_worker import CryptoComUserApiWorker
from crypto_com_client import CryptoComClient
from crypto_com_market_data_worker import CryptoComMarketDataWorker
from multiprocessing import Process, Manager
from pushover import Client
from periodic import PeriodicNormal


pushover_clients = []
crypto_com_clients = []
shared_market_data = None
shared_user_api_data_collection = {}
buy_sell_requests_queues_collection = {}


def get_user_specific_log_from_general_one(path, user):
    head, tail = ntpath.split(path)
    filename = tail or ntpath.basename(head)
    directory = path.replace(filename, "")
    return directory + user + "_" + filename

def find_matching_pushover_client_for_crypto_com_user(pushover_clients, pushover_user_keys, crypto_com_user):
    pushover_user_key = pushover_user_keys[crypto_com_user]
    for pushover_client in pushover_clients:
        if pushover_client.user_key == pushover_user_key:
            return pushover_client
    return None

def print_periodically_shared_data():
    print("Market data: {}".format(shared_market_data))
    for crypto_com_client in crypto_com_clients:
        print("User data collection for user {}: {}".format(crypto_com_client.crypto_com_user, shared_user_api_data_collection[crypto_com_client.crypto_com_user]))

if __name__ == '__main__':
    try:
        print("##########################################################")
        print("*******************Crypto.com trader**********************")
        print("Check for additional options: crypto_com_trader.py --help")
        print("##########################################################")
        print("")

        raspberry = False
        debug = False
        configfile = None  # Must be specified
        try:
            opts, args = getopt.getopt(sys.argv[1:], "hrdsc:n", ["help", "raspberry", "config", "debug", "supervisord"])
        except getopt.GetoptError:
            print("crypto_com_trader.py --help")
            sys.exit(2)
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                print("-r   or   --raspberry                     Use when running on Raspberry Pi!")
                print("-d   or   --debug                         Ignore api keys definition for crypto.com. Usefull for debugging with no real transactions.")
                print("-c   or   --config   <filename>           Use configuration file (in json format)")
                print("")
                sys.exit()
            elif opt in ("-r", "--raspberry"):
                print("Running on Raspberry Pi...")
                print("NOTE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print("When using with supervisord, make sure the correct config file is provided in crypto_com_trader args in supervisord.conf file !")
                print("###################################################################################################")
                raspberry = True
            elif opt in ("-d",  "--debug"):
                print("crypto.com SANDBOX will be used! No real transactions will be made. API keys will be ignored where possible.")
                debug = True
            elif opt in ("-c", "--config"):
                if arg:
                    configfile = arg
                else:
                    if len(args) != 1:
                        print("Config File not specified in the arguments or more args provided than needed! Run: python crypto_com_trader.py --help")
                        exit()
                    else:
                        configfile = args[0]
                print("Using configuration file: {}".format(configfile))

        if not configfile:
            print("Config File not specified in the arguments! Run: python crypto_com_trader.py --help")
            exit()
        else:
            print("Reading configuration file...")
            try:
                with open(configfile) as json_data_file:
                    configdata = json.load(json_data_file)
                #print(configdata)

                # INITIAL VARIABLES
                local_webhook_server_pin = configdata["local_webhook_server_pin"]
                logging_path = configdata["logging_path"]
                transactions_log_path = configdata["transactions_log_path"]
                crypto_com_log_path = configdata["crypto_com_log_path"]
                crypto_com_transactions_log_path = configdata["crypto_com_transactions_log_path"]

                pushover_application_token = configdata["pushover_application_token"]
                pushover_user_keys = configdata["pushover_user_keys"]

                crypto_com_users_api_stuff = configdata["crypto_com_users_api_stuff"]

                exchange_variables = configdata["exchange_variables"]

            except Exception as e:
                print("Error while loading config file: {}".format(str(e)))
                exit()

        # Creating pushover clients...
        for pushover_user, pushover_user_key in pushover_user_keys.items():
            pushover_client = Client(pushover_user_key, api_token=pushover_application_token)
            pushover_clients.append(pushover_client)

        for crypto_com_user, crypto_com_api_stuff in crypto_com_users_api_stuff.items():
            user_crypto_com_log_path = get_user_specific_log_from_general_one(crypto_com_log_path, crypto_com_user)
            user_crypto_com_transactions_log_path = get_user_specific_log_from_general_one(crypto_com_transactions_log_path, crypto_com_user)
            pushover_client = find_matching_pushover_client_for_crypto_com_user(pushover_clients, pushover_user_keys, crypto_com_user)
            crypto_com_clients.append(CryptoComClient(crypto_com_api_stuff["api_key"], crypto_com_api_stuff["secret_key"], crypto_com_user, user_crypto_com_log_path, user_crypto_com_transactions_log_path, pushover_client))

        # **************************************************************************************************************
        # Shared data definition
        # **************************************************************************************************************
        manager = Manager()
        shared_market_data = manager.dict({
            "taker_fee": exchange_variables["taker_fee"],
            "CRO_holding_backup": exchange_variables["CRO_holding_backup"],
            "balance_USDT": 0,
            "balance_BTC": 0,
            "balance_CRO": 0,
            "price_BTC_sell_to_USDT": 0,
            "price_CRO_buy_for_BTC": 0,
            "fee_BTC_sell_in_USDT": 0,
            "price_CRO_buy_for_USDT": 0,
            "last_CRO_price_in_USDT": 0,
            "last_CRO_price_in_BTC": 0,
            "fee_BTC_sell_in_CRO": 0,
            "price_BTC_buy_for_USDT": 0,
            "fee_BTC_buy_in_BTC": 0,
            "fee_BTC_buy_in_CRO": 0
        })  # Data shared between processes

        for crypto_com_client in crypto_com_clients:
            shared_user_api_data = manager.dict({
                "tickers": {
                    "BTC_USDT": {
                        "price_decimals": 0,
                        "quantity_decimals": 0
                    },
                    "CRO_USDT": {
                        "price_decimals": 0,
                        "quantity_decimals": 0
                    },
                    "CRO_BTC": {
                        "price_decimals": 0,
                        "quantity_decimals": 0
                    }
                },
                "balance_USDT": 0,
                "balance_BTC": 0,
                "balance_CRO": 0
            })
            shared_user_api_data_collection[crypto_com_client.crypto_com_user] = shared_user_api_data
            buy_sell_requests_queue = manager.Queue()
            buy_sell_requests_queues_collection[crypto_com_client.crypto_com_user] = buy_sell_requests_queue

        # **************************************************************************************************************

        if debug:
            periodic_printer = PeriodicNormal(5, print_periodically_shared_data)
        try:
            print("Starting crypto.com market data worker...")
            crypto_com_market_data_worker = CryptoComMarketDataWorker(shared_market_data, debug=False)
            crypto_com_market_data_worker_process = Process(target=crypto_com_market_data_worker.run_forever, args=())
            crypto_com_market_data_worker_process.start()

            print("Starting crypto.com user api workers...")
            crypto_com_user_api_worker_processes = {}
            for crypto_com_client in crypto_com_clients:
                crypto_com_user_api_worker = CryptoComUserApiWorker(crypto_com_client=crypto_com_client, shared_user_api_data=shared_user_api_data_collection[crypto_com_client.crypto_com_user], shared_market_data=shared_market_data, buy_sell_requests_queue=buy_sell_requests_queues_collection[crypto_com_client.crypto_com_user], debug=False)
                crypto_com_user_api_worker_process = Process(target=crypto_com_user_api_worker.run_forever, args=())
                crypto_com_user_api_worker_processes[crypto_com_client.crypto_com_user] = crypto_com_user_api_worker_process
                crypto_com_user_api_worker_process.start()

            print("Starting webhook bot...")
            webhook_bot = WebhookBot(local_webhook_server_pin, buy_sell_requests_queues_collection)
            webhook_bot.start_bot()

            # Wait for processes to finish their jobs
            for crypto_com_user_api_worker_process in crypto_com_user_api_worker_processes.values():
                crypto_com_user_api_worker_process.join()
            crypto_com_market_data_worker_process.join()

        finally:
            if debug:
                print("Workers finished their job - cleaning up periodics...")
                periodic_printer.stop()

    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
    except Exception:
        traceback.print_exc(file=sys.stdout)
    sys.exit(0)
