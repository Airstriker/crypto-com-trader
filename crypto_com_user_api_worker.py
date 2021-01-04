import os
import sys
import asyncio
import logging
import websockets
import socket
import multiprocessing.queues
from event_dispatcher import EventDispatcher
from crypto_com_client import CryptoComClient
from crypto_com_lib import CryptoComApiClient
from queue import Empty, Queue
from periodic import PeriodicAsync
from pid import PidFile


class CryptoComUserApiWorker(object):

    def __init__(self, crypto_com_client: CryptoComClient, shared_user_api_data: dict, shared_market_data: dict, buy_sell_requests_queue: multiprocessing.queues.Queue, debug: bool = True, log_file: str = None, transactions_log_file: str = None):
        print("Initializing crypto.com user api worker for user: {}".format(crypto_com_client.crypto_com_user))
        self.debug = debug
        self.log_file = log_file if log_file else "./logs/crypto_com_user_api_worker_{}.log".format(crypto_com_client.crypto_com_user)
        self.logger = logging.getLogger("crypto_com_user_api_worker_{}".format(crypto_com_client.crypto_com_user))
        CryptoComUserApiWorker.setup_logger(self.logger, self.log_file)
        self.transactions_log_file = transactions_log_file if transactions_log_file else "./logs/transactions_{}.log".format(crypto_com_client.crypto_com_user)
        self.transactions_logger = logging.getLogger("transactions_logger_{}".format(crypto_com_client.crypto_com_user))
        CryptoComUserApiWorker.setup_logger(self.transactions_logger, self.transactions_log_file)
        self.crypto_com_client = crypto_com_client
        self.shared_market_data = shared_market_data
        self.shared_user_api_data = shared_user_api_data
        self.buy_sell_requests_queue = buy_sell_requests_queue
        self.postponed_requests_queue = Queue()
        self.crypto_com_api_client = None
        self.initializing = False
        self.initial_requests_list = []
        self.initialized = False
        self.periodic_calls = []

    @staticmethod
    def setup_logger(logger, log_file):
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(log_file, mode="w")
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        ch.setFormatter(console_formatter)
        fh.setFormatter(file_formatter)
        logger.addHandler(ch)
        logger.addHandler(fh)

    async def get_instruments(self):
        '''
        Provides information on all supported instruments (e.g. BTC_USDT)
        '''
        if self.crypto_com_api_client.authenticated:
            try:
                await self.crypto_com_api_client.send(
                    self.crypto_com_api_client.build_message(
                        method="public/get-instruments"
                    )
                )
            except (websockets.ConnectionClosed, websockets.ConnectionClosedOK, websockets.ConnectionClosedError, socket.gaierror, OSError) as e:
                pass  # That's not a critical request - no need to retransmit
            except asyncio.TimeoutError as e:
                pass  # That's not a critical request - no need to retransmit
        # else:
        #     self.logger.warning("Websocket connection not authenticated! Cannot send public/get-instruments request.")

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

    async def get_user_balances(self):
        '''
        Returns the account balance of a user for all tokens/coins.
        To be triggered only after websockets disconnection.
        '''
        if self.crypto_com_api_client.authenticated:
            request = self.crypto_com_api_client.build_message(
                method="private/get-account-summary",
                params={}
            )
            try:
                await self.crypto_com_api_client.send(request)
            except (websockets.ConnectionClosed, websockets.ConnectionClosedOK, websockets.ConnectionClosedError, socket.gaierror, OSError) as e:
                # Put the request to the retransmission queue and retry when the websocket is again authenticated
                self.logger.warning("Websocket disconnected - putting request with id {} to postponed requests queue!".format(request["id"]))
                self.postponed_requests_queue.put(request)
            except asyncio.TimeoutError as e:
                # Put the request to the retransmission queue and retry when the websocket is again authenticated
                self.logger.warning("Timeout on sending request with id {} - putting request to postponed requests queue!".format(request["id"]))
                self.postponed_requests_queue.put(request)
        else:
            self.logger.warning("Websocket connection not authenticated! Cannot send private/get-account-summary request.")

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
        else:
            # Mark the method as initialized in self.initial_requests_list
            for method_dict in self.initial_requests_list:
                if method_dict["method"] == self.get_user_balances:
                    method_dict["initialized"] = True

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

    async def handle_buy_request(self, request: dict):
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
            self.transactions_logger.info("[BUY] Price in request: {} [{}] ({} [USD]). Price on crypto.com: {} [USDT]".format(price_in_request, fiat, price_in_request_in_usd, price_on_crypto_com))
        else:
            self.logger.info("[BUY REQUEST] received! Price in request: {} [{}]. Price on crypto.com: {} [USDT]".format(price_in_request, fiat, price_on_crypto_com))
            self.transactions_logger.info("[BUY] Price in request: {} [{}]. Price on crypto.com: {} [USDT]".format(price_in_request, fiat, price_on_crypto_com))

    async def handle_sell_request(self, request: dict):
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
            self.transactions_logger.info("[SELL] Price in request: {} [{}] ({} [USD]). Price on crypto.com: {} [USDT]. Profit in fiat: {} [{}] ({} [USD]). Profit on crypto.com: {} [USDT].".format(price_in_request, fiat, price_in_request_in_usd, price_on_crypto_com, profit_in_fiat, fiat, profit_in_fiat_in_usd, profit_in_usdt))
        else:
            self.logger.info("[SELL REQUEST] received! Price in request: {} [{}]. Price on crypto.com: {} [USDT]. Profit in fiat: {} [{}]. Profit on crypto.com: {} [USDT].".format(price_in_request, fiat, price_on_crypto_com, profit_in_fiat, fiat, profit_in_usdt))
            self.transactions_logger.info("[SELL] Price in request: {} [{}]. Price on crypto.com: {} [USDT]. Profit in fiat: {} [{}]. Profit on crypto.com: {} [USDT].".format(price_in_request, fiat, price_on_crypto_com, profit_in_fiat, fiat, profit_in_usdt))

    async def handle_buy_sell_requests(self):
        '''
        The incoming request should be a dict with the following keys:
        {
            'price': '25420',
            'type': 'sell'
        }
        '''
        try:
            request = self.buy_sell_requests_queue.get_nowait()
        except Empty:
            pass
        else:
            if request:
                if "type" in request and "price" in request and "fiat" in request:
                    if request["type"] == "buy":
                        await self.handle_buy_request(request)
                    elif request["type"] == "sell":
                        await self.handle_sell_request(request)
                    else:
                        raise Exception("Unknown 'type' key value in buy/sell request! Request: {}".format(request))
                else:
                    raise Exception("The incoming buy/sell request doesn't contain required keys! Request: {}".format(request))

    async def send_postponed_requests(self):
        if self.crypto_com_api_client.authenticated:
            try:
                request = self.postponed_requests_queue.get_nowait()
            except Empty:
                pass
            else:
                if request:
                    try:
                        await self.crypto_com_api_client.send(request)
                    except (websockets.ConnectionClosed, websockets.ConnectionClosedOK, websockets.ConnectionClosedError, socket.gaierror, OSError) as e:
                        # Put the request back for retransmission - shit happens ;]
                        self.logger.warning("Websocket disconnected - putting request with id {} to postponed requests queue - again!".format(request["id"]))
                        self.postponed_requests_queue.put(request)
                    except asyncio.TimeoutError as e:
                        # Put the request back for retransmission - shit happens ;]
                        self.logger.warning("Timeout on sending request with id {} - putting request to postponed requests queue!".format(request["id"]))
                        self.postponed_requests_queue.put(request)
                    except Exception as e:
                        self.logger.exception("Exception during sending postponed request with id: {}. Exception: {}".format(request["id"], repr(e)))
                        e.args = ("Exception during sending postponed request with id: {}. ".format(request["id"]) + str(e),)
                        raise

    def observer_for_client_authentication_state(self, authenticated):
        if authenticated:
            self.initializing = True
        else:
            self.initialized = False
            if self.initial_requests_list:
                for method_dict in self.initial_requests_list:
                    method_dict["initialized"] = False

    async def send_initial_requests(self):
        for method_dict in self.initial_requests_list:
            if not method_dict["initialized"]:
                await method_dict["method"]()

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

        event_dispatcher = EventDispatcher()
        event_dispatcher.register_channel_handling_method("user.balance", self.handle_channel_event_user_balance)
        event_dispatcher.register_response_handling_method("public/get-instruments", self.handle_response_get_instruments)
        event_dispatcher.register_response_handling_method("private/get-account-summary", self.handle_response_get_user_balances)

        self.crypto_com_api_client = CryptoComApiClient(
                client_type=CryptoComApiClient.USER,
                debug=self.debug,
                logger=self.logger,
                observer_for_authenticated=self.observer_for_client_authentication_state,
                api_key=self.crypto_com_client.crypto_com_api_key,
                api_secret=self.crypto_com_client.crypto_com_secret_key,
                channels=[
                    "user.balance"
                ]
        )

        periodic_call_get_instruments = PeriodicAsync(5, lambda: self.get_instruments())
        self.periodic_calls.append(periodic_call_get_instruments)
        await periodic_call_get_instruments.start()

        self.initial_requests_list.append(
            {
                "method": self.get_user_balances,
                "api_method": "private/get-account-summary",
                "initialized": False
            }
        )

        while True:
            # Send initial requests
            if self.initializing:
                try:
                    await self.send_initial_requests()
                except Exception as e:
                    message = "Exception during initial requests sending: {}".format(repr(e))
                    self.logger.exception(message)
                    # TODO: Send pushover notification here with message
                    await asyncio.sleep(1)
                else:
                    self.initializing = False
            elif not self.initialized:
                # Check if all initial methods are already initialized (the responses already handled)
                initialized = True
                for method_dict in self.initial_requests_list:
                    initialized = initialized and method_dict["initialized"]
                self.initialized = initialized

            # Send postponed requests (eg. due to websocket disconnection)
            try:
                await self.send_postponed_requests()
            except Exception as e:
                message = "Exception during sending postponed request: {}".format(repr(e))
                self.logger.exception(message)
                # TODO: Send pushover notification here with message
                await asyncio.sleep(1)

            # Handle externally injected buy/sell requests
            if self.initialized:
                try:
                    await self.handle_buy_sell_requests()
                except Exception as e:
                    message = "Exception during handling buy/sell request: {}".format(repr(e))
                    self.logger.exception(message)
                    # TODO: Send pushover notification here with message
                    await asyncio.sleep(1)

            # Main response / channel event handling loop
            event_or_response = None
            try:
                event_or_response = await self.crypto_com_api_client.next_event_or_response()
                event_dispatcher.dispatch(event_or_response)
            except Exception as e:
                message = None
                if event_or_response:
                    # Check if that's been a response for one of the initial requests
                    if "method" in event_or_response:
                        for method_dict in self.initial_requests_list:
                            if method_dict["api_method"] == event_or_response["method"]:
                                self.initializing = True
                                break
                        message = "Exception during handling user api response: {}".format(repr(e))
                    else:
                        message = "Exception during handling user api event: {}".format(repr(e))
                    self.logger.exception(message)
                    self.logger.error("Event or response that failed: {}".format(event_or_response))
                else:
                    message = "Exception during parsing user api event or response: {}".format(repr(e))
                    self.logger.exception(message)
                # TODO: Send pushover notification here with message
                await asyncio.sleep(1)

    async def cleanup(self):
        self.logger.info("Cleanup before closing worker...")
        for periodic_call in self.periodic_calls:
            await periodic_call.stop()

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
                self.logger.info("Bye bye!")
                try:
                    sys.exit(0)
                except SystemExit:
                    os._exit(0)
            except Exception as e:
                self.logger.exception(e)
            finally:
                asyncio.get_event_loop().run_until_complete(self.cleanup())
                pidfile.close(fh=pidfile.fh, cleanup=True)
                self.logger.info("Bye bye!")

