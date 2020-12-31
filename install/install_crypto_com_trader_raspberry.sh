#!/bin/bash

sudo apt update || exit 1
#Needed for ts - timestamp prefix printing; and unbuffer
sudo apt install moreutils expect-dev coreutils -y || exit 1

if [[ -z $1 ]]
then
    # Send all output to one file and all output to the screen
    LOGFILE=../logs/install_crypto_com_trader_raspberry.log
    set -x
    #prefix each line with a timestamp and print it out (without buffering)
    exec > >(unbuffer -p ts "[%F %H:%M:%S]" | tee ${LOGFILE}) 2>&1
fi

sudo apt-get install libffi-dev -y || exit 1
sudo apt-get install libssl-dev -y || exit 1
sudo apt-get install openssl -y || exit 1
sudo apt-get install libbz2-dev -y || exit 1
sudo apt-get install zlib1g-dev -y || exit 1
sudo apt-get install xterm -y || exit 1
sudo python3 -m pip uninstall pip -y
sudo apt-get install python3-pip --reinstall -y || exit 1
sudo apt-get install python-dev -y || exit 1
sudo apt-get install python3-dev -y || exit 1
sudo pip3 install virtualenv || exit 1

sudo rm -rf Python-3.7.9
wget https://www.python.org/ftp/python/3.7.9/Python-3.7.9.tgz || exit 1
gunzip -c Python-3.7.9.tgz | tar xvf - || exit 1
sudo rm -rf Python-3.7.9.tgz
cd Python-3.7.9
./configure --prefix=/opt/python3.7 --enable-ipv6 --enable-ssl --with-ssl || exit 1
make -j"$(getconf _NPROCESSORS_ONLN)" || exit 1
sudo -H make install || exit 1
cd ..
sudo apt upgrade || exit 1

sudo ln -s /opt/python3.7/bin/python3.7 /usr/local/bin/python3.7 
sudo ln -s /opt/python3.7/bin/idle3.7 /usr/local/bin/idle3.7
sudo rm -rf ../venv/
sudo rm -rf Python-3.7.9

virtualenv ../venv --python=python3.7 || exit 1
source ../venv/bin/activate || exit 1

../venv/bin/pip install -r requirements.txt || exit 1

python --version
echo "Adding supervisord crypto_com_trader to autostart..."
cp crypto_com_trader_Raspberry-autostart.desktop ~/.config/autostart/ || exit 1
sed -i "s|/trader_dir_path|$(dirname $PWD)|g" ~/.config/autostart/crypto_com_trader_Raspberry-autostart.desktop || exit 1
echo "Finished installation!"
echo "NOTE! After installation crypto_com_trader will be automatically run (and protected by supervisord) on every reboot!"
echo "NOTE! Make sure the correct config file is provided in crypto_com_trader args in supervisord.conf file !"
echo "NOTE! Enter crypto.com api credentials and other data in the appropriate config file and reboot Raspberry Pi to automatically start crypto_com_trader."
