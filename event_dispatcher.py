class EventDispatcher():
    def __init__(self):
        self.channel_handling_map = {}
        self.response_handling_map = {}

    def register_channel_handling_method(self, subscription_channel: str, handling_method: callable):
        self.channel_handling_map[subscription_channel] = handling_method

    def register_response_handling_method(self, response_method: str, handling_method: callable):
        self.response_handling_map[response_method] = handling_method

    def dispatch(self, event: dict):
        try:
            if "subscription" in event:
                return self.channel_handling_map[event["subscription"]](event)
            else:
                return self.response_handling_map[event["method"]](event)
        except Exception as e:
            raise e
