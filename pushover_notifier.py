import logging
from time import localtime, strftime
from pushover import Client
from typing import List


class PushoverNotifier(object):

    def __init__(self, application_title, pushover_application_token: str, pushover_user_keys: List[str], logger: logging.Logger = None):
        self.application_title = application_title
        self.pushover_application_token = pushover_application_token
        self.pushover_user_keys = pushover_user_keys
        self.pushover_clients = self.create_clients()
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger("pushover_notifier")
            PushoverNotifier.setup_logger(self.logger, "./logs/pushover_notifier.log")

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

    def create_clients(self):
        pushover_clients = []
        if self.pushover_application_token and self.pushover_user_keys:
            for pushover_user_key in self.pushover_user_keys:
                pushover_client = Client(pushover_user_key, api_token=self.pushover_application_token)
                pushover_clients.append(pushover_client)
        return pushover_clients

    def notify(self, message, priority=2):
        try:
            msg = strftime("%Y-%m-%d %H:%M:%S", localtime()) + " " + str(message)
            for pushover_client in self.pushover_clients:
                pushover_client.send_message(msg, title=self.application_title, priority=priority, retry=1800, expire=3600)
        except Exception as e:
            self.logger.exception("PUSHOVER NOTIFICATIONS NOT WORKING - probably connection issues: {}".format(str(e)))