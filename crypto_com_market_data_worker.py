import os
import sys
import asyncio
import logging
from event_dispatcher import EventDispatcher
from crypto_com_lib import CryptoClient
from pid import PidFile


class CryptoComMarketDataWorker:

    def __init__(self, shared_market_data: dict, debug: bool = True, log_file: str = None):
        print("Initializing crypto.com market data worker...")
        self.debug = debug
        self.log_file = log_file if log_file else "./logs/crypto_com_market_data_worker.log"
        self.logger = logging.getLogger("crypto_com_market_data_worker")
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
        self.shared_market_data = shared_market_data

    def handle_channel_event_ticker_BTC_USDT(self, event: dict):
        '''
        "data": [
          {
            "h": 1, // Price of the 24h highest trade
            "v": 10232.26315789, // The total 24h traded volume
            "a": 173.60263169, // The price of the latest trade, null if there weren't any trades
            "l": 0.01, // Price of the 24h lowest trade, null if there weren't any trades
            "b": 0.01, // The current best bid price, null if there aren't any bids
            "k": 1.12345680, // The current best ask price, null if there aren't any asks
            "c": -0.44564773, // 24-hour price change, null if there weren't any trades
            "t": 1587523078844 // update time
          }
        ]
        '''
        try:
            self.shared_market_data["price_BTC_sell_to_USDT"] = event["data"][0]["b"]
            self.shared_market_data["price_BTC_buy_for_USDT"] = event["data"][0]["k"]
        except Exception as e:
            raise Exception("Wrong data structure in ticker.BTC_USDT channel event. Exception: {}".format(repr(e)))

    def handle_channel_event_ticker_CRO_USDT(self, event: dict):
        try:
            self.shared_market_data["price_CRO_buy_for_USDT"] = event["data"][0]["k"]
            self.shared_market_data["last_CRO_price_in_USDT"] = event["data"][0]["a"]
        except Exception as e:
            raise Exception("Wrong data structure in ticker.CRO_USDT channel event. Exception: {}".format(repr(e)))

    def handle_channel_event_ticker_CRO_BTC(self, event: dict):
        try:
            self.shared_market_data["price_CRO_buy_for_BTC"] = event["data"][0]["k"]
            self.shared_market_data["last_CRO_price_in_BTC"] = event["data"][0]["a"]
        except Exception as e:
            raise Exception("Wrong data structure in ticker.CRO_BTC channel event. Exception: {}".format(repr(e)))

    async def run(self):
        event_dispatcher = EventDispatcher()
        event_dispatcher.register_channel_handling_method("ticker.BTC_USDT", self.handle_channel_event_ticker_BTC_USDT)
        event_dispatcher.register_channel_handling_method("ticker.CRO_USDT", self.handle_channel_event_ticker_CRO_USDT)
        event_dispatcher.register_channel_handling_method("ticker.CRO_BTC", self.handle_channel_event_ticker_CRO_BTC)

        async with CryptoClient(
                client_type=CryptoClient.MARKET,
                debug=self.debug,
                logger=self.logger,
                channels=[
                    "ticker.BTC_USDT",
                    "ticker.CRO_USDT",
                    "ticker.CRO_BTC",
                ]
        ) as client:
            try:
                while True:
                    # Main response / channel event handling loop
                    event_or_response = None
                    try:
                        event_or_response = await client.next_event_or_response()
                        event_dispatcher.dispatch(event_or_response)
                    except Exception as e:
                        if event_or_response:
                            message = "Exception during handling market data event or response: {}".format(repr(e))
                            self.logger.exception(message)
                            self.logger.error("Event or response that failed: {}".format(event_or_response))
                        else:
                            message = "Exception during parsing market data event or response: {}".format(repr(e))
                            self.logger.exception(message)
                        # TODO: Send pushover notification here with message
                        continue
            finally:
                self.logger.info("Cleanup before closing worker...")

    # Process execution method
    def run_forever(self):
        with PidFile(pidname="crypto_com_market_data_worker", piddir="./logs") as pidfile:
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

