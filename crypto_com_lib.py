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


class CryptoComApiClient(object):

    MARKET = 0
    USER = 1

    MARKET_URI = "wss://stream.crypto.com/v2/market"
    SANDBOX_MARKET_URI = "wss://uat-stream.3ona.co/v2/market"
    USER_URI = "wss://stream.crypto.com/v2/user"
    SANDBOX_USER_URI = "wss://uat-stream.3ona.co/v2/user"

    def __init__(self, client_type: int, debug: bool = True, logger: logging.Logger = None, channels: List[str] = None, api_secret: str = None, api_key: str = None, observer_for_authenticated: Callable = None):
        self.api_secret = api_secret.encode() if api_key else None
        self.api_key = api_key
        self._next_id = 1
        self.channels = channels
        self.websocket = None
        self.client_type = client_type
        self.debug = debug
        self.requests_queue = Queue()
        self.events_and_responses_queue = Queue()
        self._authenticated = False
        self._authenticated_observers = []  # Supporting only normal (not async (coroutines)) callbacks
        if observer_for_authenticated:
            self.register_observer_for_authenticated(observer_for_authenticated)
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger("crypto_com_lib")
            self.logger.setLevel(logging.DEBUG)
            fh = logging.FileHandler("./logs/crypto_com_lib.log", mode="w")
            fh.setLevel(logging.DEBUG)
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            fh.setFormatter(formatter)
            self.logger.addHandler(ch)
            self.logger.addHandler(fh)

        # Start itself !
        self.run()

    @property
    def authenticated(self):
        return self._authenticated

    @authenticated.setter
    def authenticated(self, value):
        self._authenticated = value
        # Notify the observers of the value change
        for callback in self._authenticated_observers:
            # Supporting only normal (not async (coroutines)) callbacks
            callback(self._authenticated)

    def register_observer_for_authenticated(self, callback):
        self._authenticated_observers.append(callback)

    def get_nonce(self):
        return int(time.time() * 1000)

    def next_id(self):
        i = self._next_id
        self._next_id += 1
        return i

    def authenticate(self):
        self.logger.info("Authenticating using the API key: {}...".format(self.api_key))
        self.send(self.build_message(
            method="public/auth"
        ))

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

    async def handle_requests(self):
        while True:
            await asyncio.sleep(0)  # This line is VERY important: In the case of trying to concurrently run two looping Tasks (here handle_requests() and handle_events_and_responses()), unless the Task has an internal await expression, it will get stuck in the while loop, effectively blocking other tasks from running (much like a normal while loop). However, as soon the Tasks have to (a)wait, they run concurrently without an issue. Check this: https://stackoverflow.com/questions/29269370/how-to-properly-create-and-run-concurrent-tasks-using-pythons-asyncio-module
            if self.websocket and self.websocket.open:
                try:
                    request = self.requests_queue.get_nowait()
                except Empty:
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
                        self.logger.exception("Exception during sending request with id: {}. Putting it back to queue. Exception: {}".format(request["id"], repr(e)))
                        self.requests_queue.put(request)
                        # TODO: Send pushover notification here!
                        await asyncio.sleep(1)

    async def get_event_or_response(self):
        while True:
            await asyncio.sleep(0)  # This line is VERY important: In the case of trying to concurrently run two looping Tasks (here handle_requests() and handle_events_and_responses()), unless the Task has an internal await expression, it will get stuck in the while loop, effectively blocking other tasks from running (much like a normal while loop). However, as soon the Tasks have to (a)wait, they run concurrently without an issue. Check this: https://stackoverflow.com/questions/29269370/how-to-properly-create-and-run-concurrent-tasks-using-pythons-asyncio-module
            try:
                event_or_response = self.events_and_responses_queue.get_nowait()
            except Empty:
                continue
            else:
                return event_or_response

    def get_event_or_response_no_wait(self):
        try:
            event_or_response = self.events_and_responses_queue.get_nowait()
        except Empty:
            return None
        else:
            return event_or_response

    async def handle_events_and_responses(self):
        while True:
            await asyncio.sleep(0)
            message = None
            try:
                if not self.websocket or not self.websocket.open:
                    self.authenticated = False
                    if self.websocket and not self.websocket.open:
                        self.logger.error("Websocket NOT connected. Trying to reconnect...")
                        # TODO: Send pushover notification here!
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
                self.logger.exception("Exception during received message parsing: {}".format(repr(e)))
                if message:
                    self.logger.error("Message that failed being parsed: {}".format(message))
                # TODO: Send pushover notification here!
                await asyncio.sleep(1)

    def subscribe(self):
        self.logger.info("Subscribing channels: {}...".format(self.channels))
        self.send(self.build_message(
            method="subscribe",
            params={"channels": self.channels}
        ))

    async def parse_message(self, data: dict):
        if data["method"] == "public/heartbeat":
            data["method"] = "public/respond-heartbeat"
            self.logger.info("Heartbeat")
            self.send(data)
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
        self.websocket = await websockets.connect(websocket_uri)
        await asyncio.sleep(1)  # As requested by crypto.com API
        if self.client_type == self.USER and self.api_key:
            self.authenticate()
        # Outcommented due to API defect requirering authentication (via public/auth) for public methods
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
        asyncio.create_task(self.handle_requests())
        asyncio.create_task(self.handle_events_and_responses())

    # async def __aenter__(self):
    #     await self.websocket_connect()
    #     return self
    #
    # async def __aexit__(self, exc_type, exc, tb):
    #     await self.websocket_disconnect()
