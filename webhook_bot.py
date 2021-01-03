'''
Using http://flask-classful.teracy.org/
'''

import ast
import hashlib
import pprint
from flask import Flask, current_app
from flask_classful import FlaskView, route
from flask import Flask, request, abort

#app = Flask(__name__)

class Webhook_bot():

    def __init__(self, webhook_pin: str, buy_sell_requests_queues_collection: dict):
        print("Initializing webhook bot...")

        self.webhook_pin = webhook_pin
        self.buy_sell_requests_queues_collection = buy_sell_requests_queues_collection
        print("***********************************************************************************************************************************************")
        print("TradingView Alert string to be used (just copy and paste it):")
        print(
            '{"type": "{{strategy.order.action}}", "price": "{{strategy.order.price}}", "token": "' + self.get_token() + '"}')
        print("***********************************************************************************************************************************************")

        # Create Flask object called app.
        self.app = self.create_app()

    # Generate unique token from webhook_pin. This adds a marginal amount of security.
    def get_token(self):
        token = hashlib.sha224(self.webhook_pin.encode('utf-8'))
        return token.hexdigest()

    def create_app(self):
        # create and configure the app
        app = Flask(__name__)

        # init variables (accessible in views)
        app.config['SECRET_KEY'] = self.get_token()
        app.config['SHARED_QUEUES'] = self.buy_sell_requests_queues_collection
        return app

    def start_bot(self):
        WebhookView.register(self.app)
        self.app.run()


class WebhookView(FlaskView):
    '''Note! Variables are passed via app.config dict'''
    route_base = '/'

    # Create root to easily let us know its on/working.
    def index(self):
        return 'Webhook bot is online'

    @route('/webhook', methods=['POST'])
    def webhook(self):
        if request.method == 'POST':
            # Parse the string data from tradingview into a python dict
            try:
                data = ast.literal_eval(request.get_data(as_text=True))
            except Exception as e:
                print("Cannot decode received data! Exception: {}".format(repr(e)))
                print("Note! The alert should be sent as the following string (replace token with the correct one!):")
                print('{"type": "{{strategy.order.action}}", "price": "{{strategy.order.price}}", "token": "99fb2f48c6af4761f904fc85f95eb56190e5d40b1f44ec3a9c1fa121"}')
                abort(403)
            # Check that the key is correct
            if current_app.config['SECRET_KEY'] == data['token']:
                print("[Alert Received]")
                print("POST Received:")
                pprint.pprint(data)
                # Add the request to each client's queue
                buy_sell_requests_queues_collection = current_app.config['SHARED_QUEUES']
                for buy_sell_requests_queue in buy_sell_requests_queues_collection.values():
                    buy_sell_requests_queue.put(data)
                return '', 200
            else:
                print("Wrong token received!")
                abort(403)
        else:
            abort(400)