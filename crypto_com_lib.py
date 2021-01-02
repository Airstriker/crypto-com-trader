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
from typing import List

class CryptoClient:

    MARKET = 0
    USER = 1

    MARKET_URI = "wss://stream.crypto.com/v2/market"
    SANDBOX_MARKET_URI = "wss://uat-stream.3ona.co/v2/market"
    USER_URI = "wss://stream.crypto.com/v2/user"
    SANDBOX_USER_URI = "wss://uat-stream.3ona.co/v2/user"

    def __init__(self, client_type: int, debug: bool = True, log_file: str = None, channels: List[str] = None, api_secret: str = None, api_key: str = None, websocket=None, ):
        self.api_secret = api_secret.encode() if api_key else None
        self.api_key = api_key
        self._next_id = 1
        self.channels = channels
        self.websocket = websocket
        self.client_type = client_type
        self.debug = debug
        self.authenticated = False
        self.logger = logging.getLogger(log_file) if log_file else logging.getLogger("crypto_com_lib")

    def get_nonce(self):
        return int(time.time() * 1000)

    def next_id(self):
        i = self._next_id
        self._next_id += 1
        return i

    async def authenticate(self):
        self.logger.info("Authenticating using the API key: {}...".format(self.api_key))
        await self.send(self.build_message(
            method="public/auth"
        ))

    async def send(self, message: dict):
        self.logger.info("sending request: {}".format(message))
        try:
            await self.websocket.send(json.dumps(message))
        except (websockets.ConnectionClosed, websockets.ConnectionClosedOK, websockets.ConnectionClosedError, socket.gaierror, OSError) as e:
            self.authenticated = False
            self.logger.error("Websocket NOT connected. Message with id: {} not sent!".format(message["id"]))
            e.args = ("Websocket NOT connected. Message with id: {} not sent!".format(message["id"]),)
            raise
        except Exception as e:
            self.logger.exception("Exception during sending message with id: {}. Exception: {}".format(message["id"], repr(e)))
            e.args = ("Exception during sending message with id: {}. ".format(message["id"]) + str(e),)
            raise

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

    async def next_event_or_response(self):
        event_or_response = None
        while event_or_response is None:
            message = None
            try:
                if not self.websocket.open:
                    self.authenticated = False
                    self.logger.error("Websocket NOT connected. Trying to reconnect...")
                    # TODO: Send pushover notification here!
                    await self.websocket_connect()
                message = await self.websocket.recv()
                event_or_response = await self.parse_message(json.loads(message))
            except (websockets.ConnectionClosed, websockets.ConnectionClosedOK, websockets.ConnectionClosedError, socket.gaierror, OSError) as e:
                self.logger.error(repr(e))
                await asyncio.sleep(1)
                continue
            except Exception as e:
                self.logger.exception("Exception during received message parsing: {}".format(repr(e)))
                if message:
                    self.logger.error("Message that failed being parsed: {}".format(message))
                e.args = ("Exception during received message parsing. " + str(e),)
                raise
        # self.logger.info("event or response: {}".format(event_or_response))
        return event_or_response

    async def subscribe(self):
        self.logger.info("Subscribing channels: {}...".format(self.channels))
        await self.send(self.build_message(
            method="subscribe",
            params={"channels": self.channels}
        ))

    async def parse_message(self, data: dict):
        if data["method"] == "public/heartbeat":
            data["method"] = "public/respond-heartbeat"
            self.logger.info("Heartbeat")
            await self.send(data)
        elif data["method"] == "subscribe":
            res = data.get("result")
            if res:
                return res
            else:
                if data["code"] == 0:
                    self.logger.info("Subscription success!")
                else:
                    raise Exception(f"Error when subscribing: {json.dumps(data)}")
        elif data["method"] == "public/auth":
            if data["code"] == 0:
                self.logger.info("Authentication success!")
                self.authenticated = True
                if self.channels:
                    await self.subscribe()
            else:
                raise Exception(f"Auth error: {json.dumps(data)}")
        else:
            return data

    async def websocket_connect(self):
        if self.debug:
            websocket_uri = self.SANDBOX_MARKET_URI if self.client_type == self.MARKET else self.SANDBOX_USER_URI
        else:
            websocket_uri = self.MARKET_URI if self.client_type == self.MARKET else self.USER_URI
        self.logger.info("Connecting to websocket: {}...".format(websocket_uri))
        self.websocket = await websockets.connect(websocket_uri)
        await asyncio.sleep(1)  # As requested by crypto.com API
        if self.client_type == self.USER and self.api_key:
            await self.authenticate()
        # Outcommented due to API defect requirering authentication (via public/auth) for public methods
        # elif self.client_type == self.USER and not self.api_key:
        #     self.logger.warning("Using USER API without providing api key (it's acceptable only when calling public methods)!")
        #     if self.channels:
        #         await self.subscribe()
        elif self.channels:
            await self.subscribe()

    async def __aenter__(self):
        await self.websocket_connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.logger.info("Closing websocket!")
        await self.websocket.close()