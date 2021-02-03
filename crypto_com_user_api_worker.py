import os
import sys
import asyncio
import logging
import multiprocessing.queues
from event_dispatcher import EventDispatcher
from crypto_com_client import CryptoComClient
from crypto_com_lib import CryptoComApiClient
from queue import Empty
from periodic import PeriodicNormal
from pid import PidFile
from pushover_notifier import PushoverNotifier


class CryptoComUserApiWorker(object):

    def __init__(self, crypto_com_client: CryptoComClient, shared_user_api_data: dict, shared_market_data: dict, buy_sell_requests_queue: multiprocessing.queues.Queue, debug: bool = True, log_file: str = None, transactions_log_file: str = None, pushover_notifier: PushoverNotifier = None):
        print("Initializing crypto.com user api worker for user: {}".format(crypto_com_client.crypto_com_user))
        self.debug = debug
        self.log_file = log_file if log_file else "./logs/crypto_com_user_api_worker_{}.log".format(crypto_com_client.crypto_com_user)
        self.logger = logging.getLogger("crypto_com_user_api_worker_{}".format(crypto_com_client.crypto_com_user))
        CryptoComUserApiWorker.setup_logger(self.logger, self.log_file)
        self.transactions_log_file = transactions_log_file if transactions_log_file else "./logs/transactions_{}.log".format(crypto_com_client.crypto_com_user)
        self.transactions_logger = logging.getLogger("transactions_logger_{}".format(crypto_com_client.crypto_com_user))
        CryptoComUserApiWorker.setup_logger(self.transactions_logger, self.transactions_log_file, mode="a")
        self.crypto_com_client = crypto_com_client
        self.shared_market_data = shared_market_data
        self.shared_user_api_data = shared_user_api_data
        self.buy_sell_requests_queue = buy_sell_requests_queue
        self.crypto_com_api_client = None
        self.initializing = False
        self.initial_requests_list = []
        self.initialized = False
        self.periodic_calls = []
        self.pushover_notifier = pushover_notifier

    @staticmethod
    def setup_logger(logger, log_file, mode="w"):
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(log_file, mode=mode)
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        ch.setFormatter(console_formatter)
        fh.setFormatter(file_formatter)
        logger.addHandler(ch)
        logger.addHandler(fh)

    def pushover_notify(self, message, priority=2):
        message = self.logger.name + ": " + message
        self.pushover_notifier.notify(message, priority)

    def get_instruments(self):
        '''
        Provides information on all supported instruments (e.g. BTC_USDT)
        '''
        self.crypto_com_api_client.send(
            self.crypto_com_api_client.build_message(
                method="public/get-instruments"
            )
        )

    def handle_response_get_instruments(self, response: dict):
        '''
        Update the decimals for each used ticker
        '''
        self.logger.info("Received response for public/get-instruments method with id: {}".format(response["id"]))
        for ticker, decimals in self.shared_user_api_data["tickers"].items():
            try:
                for instrument in response["result"]["instruments"]:
                    if instrument["instrument_name"] == ticker and instrument.keys() >= decimals.keys():
                        for decimal in decimals.keys():
                            # update the decimal values for the ticker - NOTE! the shared data is updated this way on purpose!
                            # Note! Modifications to mutable values or items in dict and list proxies will not be propagated through the manager, because the proxy has no way of knowing when its values or items are modified. To modify such an item, you can re-assign the modified object to the container proxy
                            copied_dict = self.shared_user_api_data["tickers"]
                            copied_dict[ticker][decimal] = instrument[decimal]
                            self.shared_user_api_data["tickers"] = copied_dict

            except Exception as e:
                raise Exception("Cannot get ticker decimals for ticker: {}. Exception: {}".format(ticker, repr(e)))

            # Check if the decimals have been updated for the ticker
            for decimal, value in self.shared_user_api_data["tickers"].items():
                if value == 0:
                    raise Exception("The decimals for ticker: {} have not been updated.".format(ticker))

    def get_user_balances(self):
        '''
        Returns the account balance of a user for all tokens/coins.
        To be triggered only after websockets disconnection.
        '''
        self.crypto_com_api_client.send(
            self.crypto_com_api_client.build_message(
                method="private/get-account-summary",
                params={}
            )
        )

    def handle_response_get_user_balances(self, response: dict):
        '''
        "accounts": [
            {
                "balance": 99999999.905000000000000000,
                "available": 99999996.905000000000000000,
                "order": 3.000000000000000000,
                "stake": 0,
                "currency": "CRO"
            }
        ]
        '''
        try:
            self.logger.info("Received response for private/get-account-summary method with id: {}".format(response["id"]))
            for balance in response["result"]["accounts"]:
                if balance["currency"] == "USDT":
                    self.logger.info("Updated USDT balance.")
                    self.shared_user_api_data["balance_USDT"] = balance["available"]
                elif balance["currency"] == "BTC":
                    self.logger.info("Updated BTC balance.")
                    self.shared_user_api_data["balance_BTC"] = balance["available"]
                elif balance["currency"] == "CRO":
                    self.logger.info("Updated CRO balance.")
                    self.shared_user_api_data["balance_CRO"] = balance["available"]
        except Exception as e:
            raise Exception("Wrong data structure in private/get-account-summary response: {}. Exception: {}".format(response, repr(e)))

    def handle_channel_event_user_balance(self, event: dict):
        '''
        "data": [
            {
                "currency": "CRO",
                "balance": 99999999947.99626,
                "available": 99999988201.50826,
                "order": 11746.488,
                "stake": 0
            }
        ]
        '''
        try:
            self.logger.info("Received balance update. Event: {}".format(event["data"]))
            for balance in event["data"]:
                if balance["currency"] == "USDT":
                    self.logger.info("Updated USDT balance.")
                    self.shared_user_api_data["balance_USDT"] = balance["available"]
                elif balance["currency"] == "BTC":
                    self.logger.info("Updated BTC balance.")
                    self.shared_user_api_data["balance_BTC"] = balance["available"]
                elif balance["currency"] == "CRO":
                    self.logger.info("Updated CRO balance.")
                    self.shared_user_api_data["balance_CRO"] = balance["available"]
        except Exception as e:
            raise Exception("Wrong data structure in user.balance channel event. Exception: {}".format(repr(e)))

    def handle_buy_request(self, request: dict):
        # Compare the price from request with current market price from crypto.com
        price_in_request = request["price"]
        self.shared_user_api_data["last_transaction_BTC_buy_price_in_fiat"] = price_in_request
        price_on_crypto_com = self.shared_market_data["price_BTC_buy_for_USDT"]
        self.shared_user_api_data["last_transaction_BTC_buy_price_in_USDT"] = price_on_crypto_com
        fiat = request["fiat"]
        eur_usd_exchange_rate = self.shared_market_data["EUR_USD_exchange_rate"]
        if fiat == "EUR" and eur_usd_exchange_rate != 0:
            price_in_request_in_usd = float(price_in_request) * float(eur_usd_exchange_rate)
            self.logger.info("[BUY REQUEST] received! Price in request: {} [{}] ({} [USD]). Price on crypto.com: {} [USDT]".format(price_in_request, fiat, price_in_request_in_usd, price_on_crypto_com))
            message = "[BUY] Price in request: {} [{}] ({} [USD]). Price on crypto.com: {} [USDT]".format(price_in_request, fiat, price_in_request_in_usd, price_on_crypto_com)
            self.transactions_logger.info(message)
            if self.pushover_notifier:
                self.pushover_notify(message)
        else:
            self.logger.info("[BUY REQUEST] received! Price in request: {} [{}]. Price on crypto.com: {} [USDT]".format(price_in_request, fiat, price_on_crypto_com))
            message = "[BUY] Price in request: {} [{}]. Price on crypto.com: {} [USDT]".format(price_in_request, fiat, price_on_crypto_com)
            self.transactions_logger.info(message)
            if self.pushover_notifier:
                self.pushover_notify(message)

    def handle_sell_request(self, request: dict):
        # Compare the price from request with current market price from crypto.com
        price_in_request = request["price"]
        self.shared_user_api_data["last_transaction_BTC_sell_price_in_fiat"] = price_in_request
        price_on_crypto_com = self.shared_market_data["price_BTC_sell_to_USDT"]
        self.shared_user_api_data["last_transaction_BTC_sell_price_in_USDT"] = price_on_crypto_com
        fiat = request["fiat"]
        profit_in_fiat = (float(self.shared_user_api_data["last_transaction_BTC_sell_price_in_fiat"]) - float(self.shared_user_api_data["last_transaction_BTC_buy_price_in_fiat"])) if self.shared_user_api_data["last_transaction_BTC_buy_price_in_fiat"] else 0
        profit_in_usdt = (float(self.shared_user_api_data["last_transaction_BTC_sell_price_in_USDT"]) - float(self.shared_user_api_data["last_transaction_BTC_buy_price_in_USDT"])) if self.shared_user_api_data["last_transaction_BTC_buy_price_in_USDT"] else 0
        eur_usd_exchange_rate = self.shared_market_data["EUR_USD_exchange_rate"]
        if fiat == "EUR" and eur_usd_exchange_rate != 0:
            price_in_request_in_usd = float(price_in_request) * float(eur_usd_exchange_rate)
            profit_in_fiat_in_usd = float(profit_in_fiat) * float(eur_usd_exchange_rate)
            self.logger.info("[SELL REQUEST] received! Price in request: {} [{}] ({} [USD]). Price on crypto.com: {} [USDT]. Profit in fiat: {} [{}] ({} [USD]). Profit on crypto.com: {} [USDT].".format(price_in_request, fiat, price_in_request_in_usd, price_on_crypto_com, profit_in_fiat, fiat, profit_in_fiat_in_usd, profit_in_usdt))
            message = "[SELL] Price in request: {} [{}] ({} [USD]). Price on crypto.com: {} [USDT]. Profit in fiat: {} [{}] ({} [USD]). Profit on crypto.com: {} [USDT].".format(price_in_request, fiat, price_in_request_in_usd, price_on_crypto_com, profit_in_fiat, fiat, profit_in_fiat_in_usd, profit_in_usdt)
            self.transactions_logger.info(message)
            if self.pushover_notifier:
                self.pushover_notify(message)
        else:
            self.logger.info("[SELL REQUEST] received! Price in request: {} [{}]. Price on crypto.com: {} [USDT]. Profit in fiat: {} [{}]. Profit on crypto.com: {} [USDT].".format(price_in_request, fiat, price_on_crypto_com, profit_in_fiat, fiat, profit_in_usdt))
            message = "[SELL] Price in request: {} [{}]. Price on crypto.com: {} [USDT]. Profit in fiat: {} [{}]. Profit on crypto.com: {} [USDT].".format(price_in_request, fiat, price_on_crypto_com, profit_in_fiat, fiat, profit_in_usdt)
            self.transactions_logger.info(message)
            if self.pushover_notifier:
                self.pushover_notify(message)

    def handle_buy_sell_requests(self):
        '''
        The incoming request should be a dict with the following keys:
        {
            'price': '25420',
            'type': 'sell'
        }
        '''
        try:
            request = self.buy_sell_requests_queue.get_nowait()
        except (Empty, BrokenPipeError):
            pass
        else:
            if request:
                if "type" in request and "price" in request and "fiat" in request:
                    if request["type"] == "buy":
                        self.handle_buy_request(request)
                    elif request["type"] == "sell":
                        self.handle_sell_request(request)
                    else:
                        raise Exception("Unknown 'type' key value in buy/sell request! Request: {}".format(request))
                else:
                    raise Exception("The incoming buy/sell request doesn't contain required keys! Request: {}".format(request))

    async def run(self):

        '''
        shared_market_data = {
            "taker_fee": exchange_variables["taker_fee"], -> done
            "CRO_holding_backup": exchange_variables["CRO_holding_backup"], -> done
            "balance_USDT": 0,                  -> done
            "balance_BTC": 0,                   -> done
            "balance_CRO": 0,                   -> done
            "price_BTC_sell_to_USDT": 0,        -> done
            "price_CRO_buy_for_BTC": 0,         -> done
            "fee_BTC_sell_in_USDT": 0,
            "price_CRO_buy_for_USDT": 0,        -> done
            "last_CRO_price_in_USDT": 0,        -> done
            "last_CRO_price_in_BTC": 0,         -> done
            "fee_BTC_sell_in_CRO": 0,
            "price_BTC_buy_for_USDT": 0,        -> done
            "fee_BTC_buy_in_BTC": 0,
            "fee_BTC_buy_in_CRO": 0,
        }
        '''

        self.crypto_com_api_client = CryptoComApiClient(
            client_type=CryptoComApiClient.USER,
            debug=self.debug,
            logger=self.logger,
            pushover_notifier=self.pushover_notifier,
            api_key=self.crypto_com_client.crypto_com_api_key,
            api_secret=self.crypto_com_client.crypto_com_secret_key,
            channels=[
                "user.balance"
            ],
            channels_handling_map={
                "user.balance": self.handle_channel_event_user_balance
            },
            responses_handling_map={
                "public/get-instruments": self.handle_response_get_instruments,
                "private/get-account-summary": self.handle_response_get_user_balances
            },
            initial_requests_handling_map={
                "private/get-account-summary": self.get_user_balances,
                "public/get-instruments": self.get_instruments
            },
            # periodic_requests_handling_map={
            #     "public/get-instruments": self.get_instruments
            # }
        )
        if self.pushover_notifier:
            self.pushover_notify("Started!", 1)

        while True:
            await asyncio.sleep(0)  # This line is VERY important: In the case of trying to concurrently run two looping Tasks (here handle_requests() and handle_events_and_responses()), unless the Task has an internal await expression, it will get stuck in the while loop, effectively blocking other tasks from running (much like a normal while loop). However, as soon the Tasks have to (a)wait, they run concurrently without an issue. Check this: https://stackoverflow.com/questions/29269370/how-to-properly-create-and-run-concurrent-tasks-using-pythons-asyncio-module
            # Handle externally injected buy/sell requests
            if self.crypto_com_api_client.initialized:
                try:
                    self.handle_buy_sell_requests()
                except Exception as e:
                    message = "Exception during handling buy/sell request: {}".format(repr(e))
                    self.logger.exception(message)
                    if self.pushover_notifier:
                        self.pushover_notify(message)
                    await asyncio.sleep(1)

    async def cleanup(self):
        self.logger.info("Cleanup before closing worker...")

    def run_forever(self):
        # executor = ProcessPoolExecutor(2)  # Alternatively ThreadPoolExecutor
        # boo = asyncio.create_task(loop.run_in_executor(executor, say_boo))  # say_boo should be ordinary functions
        # baa = asyncio.create_task(loop.run_in_executor(executor, say_baa))
        # loop.run_forever()
        with PidFile(pidname="crypto_com_user_api_worker", piddir="./logs") as pidfile:
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(self.run())
            except KeyboardInterrupt:
                self.logger.info("Interrupted")
                asyncio.get_event_loop().run_until_complete(self.cleanup())
                pidfile.close(fh=pidfile.fh, cleanup=True)
                if self.pushover_notifier:
                    self.pushover_notify("Interrupted! Bye bye!")
                self.logger.info("Bye bye!")
                try:
                    sys.exit(0)
                except SystemExit:
                    os._exit(0)
            except Exception as e:
                if self.pushover_notifier:
                    self.pushover_notify(repr(e))
                self.logger.exception(repr(e))
            finally:
                asyncio.get_event_loop().run_until_complete(self.cleanup())
                pidfile.close(fh=pidfile.fh, cleanup=True)
                if self.pushover_notifier:
                    self.pushover_notify("Bye bye!")
                self.logger.info("Bye bye!")

