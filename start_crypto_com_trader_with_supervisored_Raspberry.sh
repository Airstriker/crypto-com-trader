#!/bin/bash

#rm -rf /var/run/supervisor.sock

pkill -9 supervisord
pkill -9 python
pkill -9 python3

sudo -s <<EOF
#list of root commands
source ./venv/bin/activate
supervisord -c ./install/supervisord.conf \
	 -l ./logs/supervisord.log \
	 -e info \
	 -j ./logs/supervisord.pid
#supervisorctl -c ./install/supervisord.conf
EOF

