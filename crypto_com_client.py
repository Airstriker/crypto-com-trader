class CryptoComClient():

    def __init__(self, crypto_com_api_key, crypto_com_secret_key, crypto_com_user, user_crypto_com_log_path, user_crypto_com_transactions_log_path, pushover_client):
        self.crypto_com_api_key = crypto_com_api_key
        self.crypto_com_secret_key = crypto_com_secret_key
        self.crypto_com_user = crypto_com_user
        self.user_crypto_com_log_path = user_crypto_com_log_path
        self.user_crypto_com_transactions_log_path = user_crypto_com_transactions_log_path
        self.pushover_client = pushover_client