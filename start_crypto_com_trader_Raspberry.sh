#!/bin/bash
# Start Raspberry version of crypto_com_trader

source ./venv/bin/activate
python crypto_com_trader.py -r -c ./configs/crypto_com_trader.cfg_test
