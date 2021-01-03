import os
import sys
import asyncio
import logging
import websockets
import socket
from event_dispatcher import EventDispatcher
from crypto_com_client import CryptoComClient
from crypto_com_lib import CryptoClient
from multiprocessing.queues import Queue
from queue import Empty
from periodic import PeriodicAsync
from pid import PidFile


class CryptoComUserApiWorker:

    def __init__(self, crypto_com_client: CryptoComClient, shared_user_api_data: dict, shared_market_data: dict, buy_sell_requests_queue: Queue, debug: bool = True, log_file: str = None):
        print("Initializing crypto.com user api worker for user: {}".format(crypto_com_client.crypto_com_user))
        self.debug = debug
        self.log_file = log_file if log_file else "./logs/crypto_com_user_api_worker_{}.log".format(crypto_com_client.crypto_com_user)
        self.logger = logging.getLogger("crypto_com_user_api_worker_{}".format(crypto_com_client.crypto_com_user))
        self.logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(self.log_file, mode="w")
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.addHandler(fh)
        self.crypto_com_client = crypto_com_client
        self.shared_market_data = shared_market_data
        self.shared_user_api_data = shared_user_api_data
        self.buy_sell_requests_queue = buy_sell_requests_queue

    async def get_instruments(self, client: CryptoClient):
        '''
        Provides information on all supported instruments (e.g. BTC_USDT)
        '''
        if client.authenticated:
            try:
                await client.send(
                    client.build_message(
                        method="public/get-instruments"
                    )
                )
            except (websockets.ConnectionClosed, websockets.ConnectionClosedOK, websockets.ConnectionClosedError, socket.gaierror, OSError) as e:
                pass  # That's not a critical request - no need to retransmit
            except asyncio.TimeoutError as e:
                pass  # That's not a critical request - no need to retransmit
        else:
            self.logger.warning("Websocket connection not authenticated! Cannot send public/get-instruments request.")

    def handle_response_get_instruments(self, event: dict):
        '''
        Update the decimals for each used ticker
        '''
        self.logger.info("Received response for public/get-instruments method with id: {}".format(event["id"]))
        for ticker, decimals in self.shared_user_api_data["tickers"].items():
            try:
                for instrument in event["result"]["instruments"]:
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

    async def handle_buy_request(self, client: CryptoClient, request: dict):
        if client.authenticated:
            # TODO: Send the request to exchange
            pass
        else:
            # TODO: Add to local requests queue - postpone sending
            pass

    async def handle_sell_request(self, client: CryptoClient, request: dict):
        if client.authenticated:
            # TODO: Send the request to exchange
            pass
        else:
            # TODO: Add to local requests queue - postpone sending
            pass

    async def handle_buy_sell_requests(self, client: CryptoClient):
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
                if "type" in request and "price" in request:
                    price_in_request = request["price"]
                    if request["type"] == "buy":
                        # Compare the price from request with current market price from crypto.com
                        price_on_crypto_com = self.shared_market_data["price_BTC_buy_for_USDT"]
                        self.logger.info("[BUY REQUEST] received! Price in request: {}. Price on crypto.com [USDT]: {}".format(price_in_request, price_on_crypto_com))
                        await self.handle_buy_request(client, request)
                    elif request["type"] == "sell":
                        # Compare the price from request with current market price from crypto.com
                        price_on_crypto_com = self.shared_market_data["price_BTC_sell_to_USDT"]
                        self.logger.info("[SELL REQUEST] received! Price in request: {}. Price on crypto.com [USDT]: {}".format(price_in_request, price_on_crypto_com))
                        await self.handle_sell_request(client, request)
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

        event_dispatcher = EventDispatcher()
        event_dispatcher.register_channel_handling_method("user.balance", self.handle_channel_event_user_balance)
        event_dispatcher.register_response_handling_method("public/get-instruments", self.handle_response_get_instruments)

        async with CryptoClient(
                client_type=CryptoClient.USER,
                debug=self.debug,
                logger=self.logger,
                api_key=self.crypto_com_client.crypto_com_api_key,
                api_secret=self.crypto_com_client.crypto_com_secret_key,
                channels=[
                    "user.balance"
                ]
        ) as client:
            periodic_call_get_instruments = PeriodicAsync(5, lambda: self.get_instruments(client))
            await periodic_call_get_instruments.start()
            try:
                while True:
                    # Handle externally injected buy/sell requests
                    try:
                        await self.handle_buy_sell_requests(client)
                    except Exception as e:
                        message = "Exception during handling buy/sell request: {}".format(repr(e))
                        self.logger.exception(message)
                        # TODO: Send pushover notification here with message
                        await asyncio.sleep(1)
                        continue

                    # Main response / channel event handling loop
                    event_or_response = None
                    try:
                        event_or_response = await client.next_event_or_response()
                        event_dispatcher.dispatch(event_or_response)
                    except Exception as e:
                        if event_or_response:
                            message = "Exception during handling user api event or response: {}".format(repr(e))
                            self.logger.exception(message)
                            self.logger.error("Event or response that failed: {}".format(event_or_response))
                        else:
                            message = "Exception during parsing user api event or response: {}".format(repr(e))
                            self.logger.exception(message)
                        # TODO: Send pushover notification here with message
                        await asyncio.sleep(1)
                        continue
            finally:
                self.logger.info("Cleanup before closing worker...")
                await periodic_call_get_instruments.stop()

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
                pidfile.close(fh=pidfile.fh, cleanup=True)
                try:
                    sys.exit(0)
                except SystemExit:
                    os._exit(0)
            except Exception as e:
                self.logger.exception(e)
            finally:
                pidfile.close(fh=pidfile.fh, cleanup=True)
                self.logger.info("Bye bye!")

