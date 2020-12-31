import os
import sys
import asyncio
import logging
from event_dispatcher import EventDispatcher
from crypto_com_client import CryptoComClient
from crypto_com_lib import CryptoClient
from periodic import PeriodicAsync

logging.basicConfig(level=logging.INFO)


class CryptoComUserApiWorker:

    def __init__(self, crypto_com_client: CryptoComClient, shared_user_api_data: dict, shared_market_data: dict, debug: bool = True, log_file: str = None):
        print("Initializing crypto.com user api worker for user: {}".format(crypto_com_client.crypto_com_user))
        self.debug = debug
        self.log_file = log_file if log_file else "./logs/crypto_com_user_api_worker_{}.log".format(crypto_com_client.crypto_com_user)
        self.logger = logging.getLogger(log_file) if log_file else logging.getLogger("./logs/crypto_com_user_api_worker_{}.log".format(crypto_com_client.crypto_com_user))
        self.crypto_com_client = crypto_com_client
        self.shared_market_data = shared_market_data
        self.shared_user_api_data = shared_user_api_data

    async def get_instruments(self, client: CryptoClient):
        '''
        Provides information on all supported instruments (e.g. BTC_USDT)
        '''
        await client.send(
            client.build_message(
                method="public/get-instruments",
            )
        )

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
                    print("Updated USDT balance")
                    self.shared_user_api_data["balance_USDT"] = balance["available"]
                elif balance["currency"] == "BTC":
                    print("Updated BTC balance")
                    self.shared_user_api_data["balance_BTC"] = balance["available"]
                elif balance["currency"] == "CRO":
                    print("Updated CRO balance")
                    self.shared_user_api_data["balance_CRO"] = balance["available"]
        except Exception as e:
            raise Exception("Wrong data structure in user.balance channel event. Exception: {}".format(repr(e)))

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
                log_file=self.log_file,
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
                    try:
                        event = await client.next_event()
                        event_dispatcher.dispatch(event)
                    except Exception as e:
                        message = "Exception in handling user api event: {}".format(repr(e))
                        self.logger.error(message)
                        self.logger.error("Event that failed: {}".format(event))
                        # Send pushover notification here
                        continue
            finally:
                self.logger.info("Cleanup before closing worker...")
                await periodic_call_get_instruments.stop()

    def run_forever(self):
        # executor = ProcessPoolExecutor(2)  # Alternatively ThreadPoolExecutor
        # boo = asyncio.create_task(loop.run_in_executor(executor, say_boo))
        # baa = asyncio.create_task(loop.run_in_executor(executor, say_baa))
        # loop.run_forever()
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.run())
        except KeyboardInterrupt:
            self.logger.info("Interrupted")
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
        except Exception as e:
            self.logger.exception(e)
        finally:
            self.logger.info("Bye bye!")

