#!/bin/bash

sleep 60
DEVICE="5A46-F75F"
LOGDIR=$(blkid -t UUID=${DEVICE} -l -o list | fgrep ${DEVICE} | awk '{print $3}')
if [[ $LOGDIR == /media/* ]] ; then
	while [[ ! -f /tmp/gauge-data.txt ]] ; do
		sleep 1
	done
	cd /root/ShellyRollersController
	nohup ./rollerController.py -dn  >${LOGDIR}/roller.log 2>&1 &
	logger "rollerController started"
else
	logger "Error launching rollerController - logfile location not mounted!"
	exit 1
fi
