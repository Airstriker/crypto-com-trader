#!/bin/bash
# Start PC version of crypto_com_trader

source ./venv/bin/activate
python crypto_com_trader.py -c ./configs/crypto_com_trader.cfg_test
