import os
import sys
import asyncio
import logging
from crypto_com_lib import CryptoComApiClient
from pid import PidFile
from pushover_notifier import PushoverNotifier


class CryptoComMarketDataWorker(object):

    def __init__(self, shared_market_data: dict, debug: bool = True, log_file: str = None, pushover_notifier: PushoverNotifier = None):
        print("Initializing crypto.com market data worker...")
        self.debug = debug
        self.log_file = log_file if log_file else "./logs/crypto_com_market_data_worker.log"
        self.logger = logging.getLogger("crypto_com_market_data_worker")
        CryptoComMarketDataWorker.setup_logger(self.logger, self.log_file)
        self.shared_market_data = shared_market_data
        self.crypto_com_api_client = None
        self.pushover_notifier = pushover_notifier

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
        self.crypto_com_api_client = CryptoComApiClient(
            client_type=CryptoComApiClient.MARKET,
            debug=self.debug,
            logger=self.logger,
            pushover_notifier=self.pushover_notifier,
            channels=[
                "ticker.BTC_USDT",
                "ticker.CRO_USDT",
                "ticker.CRO_BTC",
            ],
            channels_handling_map={
                "ticker.BTC_USDT": self.handle_channel_event_ticker_BTC_USDT,
                "ticker.CRO_USDT": self.handle_channel_event_ticker_CRO_USDT,
                "ticker.CRO_BTC": self.handle_channel_event_ticker_CRO_BTC
            }
        )
        if self.pushover_notifier:
            self.pushover_notifier.notify("Started crypto_com_market_data_worker.")
        while True:
            # Main response / channel event handling loop
            await asyncio.sleep(0)  # This line is VERY important: In the case of trying to concurrently run two looping Tasks (here handle_requests() and handle_events_and_responses()), unless the Task has an internal await expression, it will get stuck in the while loop, effectively blocking other tasks from running (much like a normal while loop). However, as soon the Tasks have to (a)wait, they run concurrently without an issue. Check this: https://stackoverflow.com/questions/29269370/how-to-properly-create-and-run-concurrent-tasks-using-pythons-asyncio-module
            pass

    async def cleanup(self):
        self.logger.info("Cleanup before closing worker...")

    # Process execution method
    def run_forever(self):
        with PidFile(pidname="crypto_com_market_data_worker", piddir="./logs") as pidfile:
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(self.run())
            except KeyboardInterrupt:
                self.logger.info("Interrupted")
                asyncio.get_event_loop().run_until_complete(self.cleanup())
                pidfile.close(fh=pidfile.fh, cleanup=True)
                if self.pushover_notifier:
                    self.pushover_notifier.notify("Interrupted. Bye bye!")
                self.logger.info("Bye bye!")
                try:
                    sys.exit(0)
                except SystemExit:
                    os._exit(0)
            except Exception as e:
                if self.pushover_notifier:
                    self.pushover_notifier.notify(repr(e))
                self.logger.exception(repr(e))
            finally:
                asyncio.get_event_loop().run_until_complete(self.cleanup())
                pidfile.close(fh=pidfile.fh, cleanup=True)
                if self.pushover_notifier:
                    self.pushover_notifier.notify("Bye bye!")
                self.logger.info("Bye bye!")

