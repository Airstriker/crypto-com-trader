'''
Based on https://github.com/maxpowel/crypto_com_client
'''

import json
import asyncio
import websockets
import hmac
import hashlib
import time
import logging
import socket
from typing import List, Callable
from queue import Empty, Queue
from periodic import PeriodicNormal
from pushover_notifier import PushoverNotifier


class CryptoComApiClient(object):

    MARKET = 0
    USER = 1

    MARKET_URI = "wss://stream.crypto.com/v2/market"
    SANDBOX_MARKET_URI = "wss://uat-stream.3ona.co/v2/market"
    USER_URI = "wss://stream.crypto.com/v2/user"
    SANDBOX_USER_URI = "wss://uat-stream.3ona.co/v2/user"

    def __init__(self, client_type: int, debug: bool = True, logger: logging.Logger = None, channels: List[str] = None, channels_handling_map: dict = None, responses_handling_map: dict = None, initial_requests_handling_map: dict = None, periodic_requests_handling_map: dict = None, api_secret: str = None, api_key: str = None, observer_for_authenticated: Callable = None, pushover_notifier: PushoverNotifier = None):
        self.api_secret = api_secret.encode() if api_key else None
        self.api_key = api_key
        self._next_id = 1
        self.channels = channels
        self.channels_handling_map = channels_handling_map
        self.responses_handling_map = responses_handling_map
        self.initial_requests_handling_map = initial_requests_handling_map
        self.periodic_requests_handling_map = periodic_requests_handling_map
        self.periodic_calls = []
        self.initial_requests_list = []
        self.initializing = False
        self.initialized = False
        self.prevent_pushover_notifications_regarding_disconnected_websocket = False
        self.last_websocket_connection_exception_pushover_message = None
        self.requests_queue = Queue()
        self.events_and_responses_queue = Queue()
        self.websocket = None
        self.client_type = client_type
        self.debug = debug
        self._authenticated = False
        self._authenticated_observers = []  # Supporting only normal (not async (coroutines)) callbacks
        if observer_for_authenticated:
            self.register_observer_for_authenticated(observer_for_authenticated)
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger("crypto_com_lib")
            CryptoComApiClient.setup_logger(self.logger, "./logs/crypto_com_lib.log")
        self.pushover_notifier = pushover_notifier

        # Self validation
        self.check_channels_handling_map_consistency()
        self.check_responses_handling_map_consistency()

        # Start itself !
        self.run()
        self.start_periodic_requests()

    def __exit__(self):
        self.logger.info("Cleanup...")
        for periodic_call in self.periodic_calls:
            periodic_call.stop()

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

    @property
    def authenticated(self):
        return self._authenticated

    @authenticated.setter
    def authenticated(self, value):
        self._authenticated = value
        if self._authenticated:
            self.initializing = True
        else:
            self.initialized = False
            if self.initial_requests_list:
                for method_dict in self.initial_requests_list:
                    method_dict["initialized"] = False

        # Notify the observers of the value change
        for callback in self._authenticated_observers:
            # Supporting only normal (not async (coroutines)) callbacks
            callback(self._authenticated)

    def pushover_notify(self, message, priority=2):
        if self.pushover_notifier:
            try:
                message = self.logger.name + ": " + message
                self.pushover_notifier.notify(message, priority)
            except Exception as e:
                self.logger.error("Cannot send pushover notification. Probably connection loss.")

    def register_observer_for_authenticated(self, callback):
        self._authenticated_observers.append(callback)

    def authenticate(self):
        self.logger.info("Authenticating using the API key: {}...".format(self.api_key))
        self.send(self.build_message(
            method="public/auth"
        ))

    def check_channels_handling_map_consistency(self):
        for channel in self.channels:
            if not self.channels_handling_map.get(channel, None):
                raise Exception("channels_handling_map dict is missing the handling method definition for channel: {}".format(channel))

    def check_responses_handling_map_consistency(self):
        if self.initial_requests_handling_map:
            for api_method, method in self.initial_requests_handling_map.items():
                self.initial_requests_list.append({
                    "method": method,
                    "api_method": api_method,
                    "initialized": False
                })
                if not self.responses_handling_map.get(api_method, None):
                    raise Exception("responses_handling_map dict is missing the handling method definition for method: {}".format(api_method))

        if self.periodic_requests_handling_map:
            for api_method, method in self.periodic_requests_handling_map.items():
                if not self.responses_handling_map.get(api_method, None):
                    raise Exception("responses_handling_map dict is missing the handling method definition for method: {}".format(api_method))

    def start_periodic_requests(self):
        if self.periodic_requests_handling_map:
            for method in self.periodic_requests_handling_map.values():
                try:
                    periodic_call = PeriodicNormal(5, method)
                    self.periodic_calls.append(periodic_call)
                    periodic_call.start()
                except Exception as e:
                    e.args = ("Exception during periodic function execution: {}. ".format(method) + str(e),)
                    raise

    def get_nonce(self):
        return int(time.time() * 1000)

    def next_id(self):
        i = self._next_id
        self._next_id += 1
        return i

    def current_id(self):
        return self._next_id

    def send(self, request: dict):
        self.requests_queue.put(request)

    def build_message(self, method: str, params: dict = None, **kwargs):
        message = {
            "id": self.next_id(),
            "method": method,
            "nonce": self.get_nonce()
        }
        if params:
            message["params"] = params

        if kwargs:
            message.update(kwargs)

        if self.client_type == self.USER and not self.api_key:
            if not method.startswith("public"):
                raise Exception("Calling USER API private method: {} without providing api_key!".format(method))
        elif self.client_type == self.USER and self.api_key and (method == "public/auth" or method.startswith("private")):
            message["api_key"] = self.api_key
            self.sign_message(message)

        return message

    def sign_message(self, message: dict):
        # First ensure the params are alphabetically sorted by key
        paramString = ""

        if "params" in message:
            for key in sorted(message["params"]):
                paramString += key
                paramString += str(message["params"][key])

        sigPayload = message["method"] + str(message["id"]) + message["api_key"] + paramString + str(message["nonce"])

        message["sig"] = hmac.new(self.api_secret, msg=sigPayload.encode(), digestmod=hashlib.sha256).hexdigest()

        return message

    async def dispatch(self):
        while True:
            await asyncio.sleep(0)  # This line is VERY important: In the case of trying to concurrently run two looping Tasks (here handle_requests() and handle_events_and_responses()), unless the Task has an internal await expression, it will get stuck in the while loop, effectively blocking other tasks from running (much like a normal while loop). However, as soon the Tasks have to (a)wait, they run concurrently without an issue. Check this: https://stackoverflow.com/questions/29269370/how-to-properly-create-and-run-concurrent-tasks-using-pythons-asyncio-module
            event_or_response = await self.get_event_or_response()
            try:
                if "subscription" in event_or_response:
                    self.channels_handling_map[event_or_response["subscription"]](event_or_response)
                else:
                    self.responses_handling_map[event_or_response["method"]](event_or_response)
            except Exception as e:
                if "subscription" in event_or_response:
                    message = "Exception during event handling: {}".format(repr(e))
                    self.logger.exception(message)
                    self.logger.error("Event that failed: {}".format(event_or_response))
                else:
                    # Check if that's been a response for one of the initial requests
                    for method_dict in self.initial_requests_list:
                        if method_dict["api_method"] == event_or_response["method"]:
                            self.initializing = True
                            break
                    message = "Exception during response handling: {}".format(repr(e))
                    self.logger.exception(message)
                    self.logger.error("Response that failed: {}".format(event_or_response))
                self.pushover_notify(message)
            else:
                if "method" in event_or_response:
                    # Mark the method as initialized in self.initial_requests_list
                    for method_dict in self.initial_requests_list:
                        if method_dict["api_method"] == event_or_response["method"]:
                            method_dict["initialized"] = True

    async def send_initial_requests(self):
        '''
        Initial requests are sent on every websocket disconnection event.
        '''
        while True:
            await asyncio.sleep(0)  # This line is VERY important: In the case of trying to concurrently run two looping Tasks (here handle_requests() and handle_events_and_responses()), unless the Task has an internal await expression, it will get stuck in the while loop, effectively blocking other tasks from running (much like a normal while loop). However, as soon the Tasks have to (a)wait, they run concurrently without an issue. Check this: https://stackoverflow.com/questions/29269370/how-to-properly-create-and-run-concurrent-tasks-using-pythons-asyncio-module
            # Send initial requests
            if self.initializing:
                try:
                    for method_dict in self.initial_requests_list:
                        if not method_dict["initialized"]:
                            method_dict["method"]()
                except Exception as e:
                    message = "Exception during initial requests sending: {}".format(repr(e))
                    self.logger.exception(message)
                    self.pushover_notify(message)
                    await asyncio.sleep(1)
                else:
                    self.initializing = False
            elif not self.initialized:
                # Check if all initial methods are already initialized (the responses already handled)
                _initialized = True
                for method_dict in self.initial_requests_list:
                    _initialized = _initialized and method_dict["initialized"]
                self.initialized = _initialized

    async def handle_requests(self):
        '''
        Main loop handling all queued requests
        '''
        while True:
            await asyncio.sleep(0)  # This line is VERY important: In the case of trying to concurrently run two looping Tasks (here handle_requests() and handle_events_and_responses()), unless the Task has an internal await expression, it will get stuck in the while loop, effectively blocking other tasks from running (much like a normal while loop). However, as soon the Tasks have to (a)wait, they run concurrently without an issue. Check this: https://stackoverflow.com/questions/29269370/how-to-properly-create-and-run-concurrent-tasks-using-pythons-asyncio-module
            if self.websocket and self.websocket.open:
                try:
                    request = self.requests_queue.get_nowait()
                except (Empty, BrokenPipeError):
                    pass
                else:
                    # Check if request requires authentication
                    if not self.authenticated and request["method"] not in ["public/respond-heartbeat", "public/auth", "subscribe"]:
                        # Put it back to the queue
                        self.requests_queue.put(request)
                        continue

                    self.logger.info("sending request: {}".format(request))
                    try:
                        await self.websocket.send(json.dumps(request))
                    except (websockets.ConnectionClosed, websockets.ConnectionClosedOK, websockets.ConnectionClosedError, socket.gaierror, OSError) as e:
                        self.logger.error("Websocket NOT connected. Request with id: {} not sent! Putting it back to queue.".format(request["id"]))
                        self.requests_queue.put(request)
                        await asyncio.sleep(1)
                    except Exception as e:
                        message = "Exception during sending request with id: {}. Putting it back to queue. Exception: {}".format(request["id"], repr(e))
                        self.logger.exception(message)
                        self.requests_queue.put(request)
                        self.pushover_notify(message)
                        await asyncio.sleep(1)

    async def get_event_or_response(self):
        while True:
            await asyncio.sleep(0)  # This line is VERY important: In the case of trying to concurrently run two looping Tasks (here handle_requests() and handle_events_and_responses()), unless the Task has an internal await expression, it will get stuck in the while loop, effectively blocking other tasks from running (much like a normal while loop). However, as soon the Tasks have to (a)wait, they run concurrently without an issue. Check this: https://stackoverflow.com/questions/29269370/how-to-properly-create-and-run-concurrent-tasks-using-pythons-asyncio-module
            try:
                event_or_response = self.events_and_responses_queue.get_nowait()
            except (Empty, BrokenPipeError):
                continue
            else:
                return event_or_response

    def get_event_or_response_no_wait(self):
        try:
            event_or_response = self.events_and_responses_queue.get_nowait()
        except (Empty, BrokenPipeError):
            return None
        else:
            return event_or_response

    async def handle_events_and_responses(self):
        '''
        Main loop handling all queued events or responses
        '''
        while True:
            await asyncio.sleep(0)
            message = None
            try:
                if not self.websocket or not self.websocket.open:
                    self.authenticated = False
                    if self.websocket and not self.websocket.open:
                        msg = "Websocket NOT connected. Trying to reconnect..."
                        self.logger.error(msg)
                        if not self.prevent_pushover_notifications_regarding_disconnected_websocket:
                            self.pushover_notify(msg)
                            self.prevent_pushover_notifications_regarding_disconnected_websocket = True
                    await self.websocket_connect()
                message = await self.websocket.recv()
                event_or_response = await self.parse_message(json.loads(message))
                if event_or_response:
                    self.events_and_responses_queue.put(event_or_response)
            except (websockets.ConnectionClosed, websockets.ConnectionClosedOK, websockets.ConnectionClosedError,
                    socket.gaierror, OSError) as e:
                self.logger.error(repr(e))
                await asyncio.sleep(1)
            except Exception as e:
                msg = "Exception during received message parsing: {}".format(repr(e))
                self.logger.exception(msg)
                if message:
                    self.logger.error("Message that failed being parsed: {}".format(message))
                self.pushover_notify(msg)
                await asyncio.sleep(1)

    def subscribe(self):
        self.logger.info("Subscribing channels: {}...".format(self.channels))
        self.send(self.build_message(
            method="subscribe",
            params={"channels": self.channels}
        ))

    async def parse_message(self, data: dict):
        if data["method"] == "public/heartbeat":
            heartbeat_response = {
                "id": data["id"],
                "method": "public/respond-heartbeat"
            }
            self.logger.info("Heartbeat")
            self.send(heartbeat_response)
            return None
        elif data["method"] == "subscribe":
            res = data.get("result")
            if res:
                return res
            else:
                if data["code"] == 0:
                    self.logger.info("Subscription success!")
                    return None
                else:
                    await self.websocket_disconnect()  # For simpler flow
                    self.authenticated = False
                    raise Exception(f"Error when subscribing: {json.dumps(data)}")
        elif data["method"] == "public/auth":
            if data["code"] == 0:
                self.logger.info("Authentication success!")
                self.authenticated = True
                if self.channels:
                    self.subscribe()
                return None
            else:
                await self.websocket_disconnect()  # For simpler flow
                raise Exception(f"Auth error: {json.dumps(data)}")

        return data

    async def websocket_connect(self):
        websocket_uri = self.MARKET_URI if self.client_type == self.MARKET else self.USER_URI
        # if self.debug:
        #     websocket_uri = self.SANDBOX_MARKET_URI if self.client_type == self.MARKET else self.SANDBOX_USER_URI
        self.logger.info("Connecting to websocket: {}...".format(websocket_uri))
        try:
            self.websocket = await asyncio.wait_for(websockets.connect(websocket_uri), 10)
        except Exception as e:
            self.logger.exception("Websocket connection exception: {}".format(repr(e)))
            if not self.last_websocket_connection_exception_pushover_message or (self.last_websocket_connection_exception_pushover_message and self.last_websocket_connection_exception_pushover_message != repr(e)):
                self.pushover_notify("Websocket connection exception: {}".format(repr(e)))
                self.last_websocket_connection_exception_pushover_message = repr(e)
            if self.websocket:
                await self.websocket.close()
            return
        self.pushover_notify("Connected to websocket!", 1)
        self.prevent_pushover_notifications_regarding_disconnected_websocket = False
        self.last_websocket_connection_exception_pushover_message = None
        await asyncio.sleep(1)  # As requested by crypto.com API
        if self.client_type == self.USER and self.api_key:
            self.authenticate()
        # Commented due to API defect requirering authentication (via public/auth) for public methods
        # elif self.client_type == self.USER and not self.api_key:
        #     self.logger.warning("Using USER API without providing api key (it's acceptable only when calling public methods)!")
        #     if self.channels:
        #         await self.subscribe()
        elif self.channels:
            self.subscribe()

    async def websocket_disconnect(self):
        self.logger.info("Closing websocket!")
        await self.websocket.close()

    def run(self):
        try:
            asyncio.get_running_loop()
        except Exception as e:
            self.logger.exception("No running asyncio loop detected! Terminating CryptoComApiClient!")
            raise Exception("No running asyncio loop detected! Terminating CryptoComApiClient!")
        else:
            asyncio.create_task(self.handle_requests())
            asyncio.create_task(self.handle_events_and_responses())
            asyncio.create_task(self.send_initial_requests())
            asyncio.create_task(self.dispatch())

    # async def __aenter__(self):
    #     await self.websocket_connect()
    #     return self
    #
    # async def __aexit__(self, exc_type, exc, tb):
    #     await self.websocket_disconnect()
