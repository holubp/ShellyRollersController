#!/usr/bin/python

import requests
from requests.auth import HTTPBasicAuth

import daemon

import pprint
pp = pprint.PrettyPrinter(depth=3)

import time
import datetime
import re
import os.path
import json
import collections
from statistics import mean
from enum import Enum

import threading
import apscheduler

import argparse
import logging as log

import astral


class ExtendAction(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        items = getattr(namespace, self.dest) or []
        items.extend(values)
        setattr(namespace, self.dest, items)

parser = argparse.ArgumentParser()
parser.register('action', 'extend', ExtendAction)
parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='verbose information on progress of the data checks')
parser.add_argument('-d', '--debug', dest='debug', action='store_true', help='debug information on progress of the data checks')
parser.add_argument('-n', '--no-daemon', dest='nodaemon', action='store_true', help='do not daemonize the process')
parser.add_argument('-c', '--config', dest='configFile', nargs=1, help='location of config file')
parser.set_defaults(configFile = 'rollerController.conf')
args = parser.parse_args()

if args.debug:
    log.basicConfig(format="%(levelname)s: %(message)s", level=log.DEBUG)
elif args.verbose:
    log.basicConfig(format="%(levelname)s: %(message)s", level=log.INFO)
else:
    log.basicConfig(format="%(levelname)s: %(message)s")

class ShellyRollerControllerRequest(Enum):
	RESTORE = 0
	OPEN = 1

class ShellyRollerController:

	def __init__(self, name, IP, authUserName, authPassword):
		assert isinstance(authUserName, str), "Expected string, got " + str(type(authUserName))
		assert isinstance(authPassword, str), "Expected string, got " + str(type(authPassword))

		self.requestQueue = collections.deque()
		self.name = name
		self.IP = IP
		self.authUserName = authUserName
		self.authPassword = authPassword
		self.shutdownRequested = False
		self.savedState = None

		# check that the roller is properly configured - in a positioning state
		state = self.getState()
		assert(state['positioning'] == True)

		self.mainThread = threading.Thread(target=self.rollerMainThread)
		self.mainThread.start()
		log.info("Roller thread started for " + self.getNameIP())

	def getNameIP(self):
		return str(self.name + '/' + self.IP)

	def getState(self):
		resp = requests.get('http://' + self.IP + '/roller/0/state', auth=HTTPBasicAuth(self.authUserName, self.authPassword))
		if resp.status_code != 200:
			log.error('Unable to get state for roller ' + self.getNameIP + ' ... received return code ' + str(resp.status_code))
		return(resp.json())
	
	def getPos(self):
		state = self.waitUntilStopGetState()
		return int(state['current_pos'])


	def setState(self, pos):
		assert(isinstance(pos, int))
		assert(pos >= 0 and pos <= 100)
		if pos == 100:
			resp = requests.get('http://' + self.IP + '/roller/0?go=to_pos&roller_pos=' + str(pos), auth=HTTPBasicAuth(self.authUserName, self.authPassword))
			if resp.status_code != 200:
				log.error('Unable to get state for roller ' + self.getNameIP + ' ... received return code ' + str(resp.status_code))
			self.waitUntilStopGetState()

		else:
			# for correct tilt of roller one needs to shut them first and then open to the target
			resp = requests.get('http://' + self.IP + '/roller/0?go=to_pos&roller_pos=0', auth=HTTPBasicAuth(self.authUserName, self.authPassword))
			if resp.status_code != 200:
				log.error('Unable to get state for roller ' + self.getNameIP + ' ... received return code ' + str(resp.status_code))
			self.waitUntilStopGetState()
			if pos > 0:
				resp = requests.get('http://' + self.IP + '/roller/0?go=to_pos&roller_pos=' + str(pos), auth=HTTPBasicAuth(self.authUserName, self.authPassword))
				if resp.status_code != 200:
					log.error('Unable to get state for roller ' + self.getNameIP + ' ... received return code ' + str(resp.status_code))
				self.waitUntilStopGetState()


	def requestShutdown(self):
		self.shutdownRequested = True
		self.mainThread.join()
		log.info("Roller thread stopped for " + self.getNameIP())

	def rollerMainThread(self):
		while not self.shutdownRequested:
			try:
				request = self.requestQueue.popleft()
				if(request == ShellyRollerControllerRequest.OPEN):
					self.saveState()
					self.setState(100)
				elif(request == ShellyRollerControllerRequest.RESTORE):
					self.restoreState()
				else:
					log.error('Got unknown request: ' + str(request))
			except IndexError as i:
				pass
			time.sleep(1)
		
	def waitUntilStopGetState(self):
		while True:
			time.sleep(1)
			state = self.getState()
			if(state['is_valid'] == True and state['state'] == 'stop'):
				return state

	def saveState(self):
		# wait until roller gets into stabilized state
		state = self.waitUntilStopGetState()
		# save the state if not open
		if not (int(state['current_pos']) == 100):
			self.savedState = state

	def restoreState(self):
		if(self.savedState != None):
			currentState = self.getState()
			if(int(currentState['current_pos']) == 100):
				self.setState(self.savedState['current_pos'])
				self.savedState = None
			else:
			 	log.info("Not restoring the state as the roller was moved in the meantime - expected open (100) state, got " + int(currentState['current_pos']))

	def submitRequest(self, request):
		# assert isinstance(request, ShellyRollerControllerRequest), "Request shall be ShellyRollerControllerRequest type, got " + str(type(request))
		self.requestQueue.append(request)

avgWindThreshold = 40.0
avgGustThreshold =  70.0
timeOpenThresholdMinutes = 10
timeRestoreThresholdMinutes = 30

gaugeFile = '/tmp/gauge-data.txt'
sleepTime =  60
historyLength = 5

latitude = 0.0
longitude = 0.0
city = ""
country = ""
timezone = ""

zaluzie = collections.deque()

with open(args.configFile) as configFile:
	def assignValFromDict(d, k, default):
			return d[k] if k in d else default

	config = json.load(configFile)
	if "thresholds" in config:
		avgWindThreshold = assignValFromDict(config['thresholds'],'avgWindThreshold', avgWindThreshold)
		avgGustThreshold = assignValFromDict(config['thresholds'],'avgGustThreshold', avgGustThreshold)
		timeOpenThresholdMinutes = assignValFromDict(config['thresholds'],'timeOpenThresholdMinutes', timeOpenThresholdMinutes)
		timeRestoreThresholdMinutes = assignValFromDict(config['thresholds'],'timeRestoreThresholdMinutes', timeRestoreThresholdMinutes)
	if "rollers" in config:
		for r in config['rollers']:
			zaluzie.append(ShellyRollerController(r['name'], str(r['IP']), str(r['rollerUsername']), str(r['rollerPassword'])))
	if "WeeWxGaugeFile" in config:
		gaugeFile = assignValFromDict(config['WeeWxGaugeFile'],'location', gaugeFile)
		sleepTime = assignValFromDict(config['WeeWxGaugeFile'],'readPeriodSecs', sleepTime)
		historyLength = assignValFromDict(config['WeeWxGaugeFile'],'numberOfAvergagedReadings', historyLength)
	if "location" in config:
		latitude = assignValFromDict(config['location'],'latitude', latitude)
		longitude = assignValFromDict(config['location'],'longitude', longitude)
		city = assignValFromDict(config['location'],'city', "")
		country = assignValFromDict(config['location'],'country', "")
		timezone = assignValFromDict(config['location'],'timezone', "")


if not os.path.isfile(gaugeFile):
	print("Gauge file " + gaugeFile + " does not exist!")
	exit(1)


class SlidingMonitor:

	def __init__(self):
		self.container = collections.deque(maxlen=historyLength)

	def append(self, x):
		assert(isinstance(x, float))
		self.container.append(x)

	def getAvg(self):
		return mean(self.container)

	def getMax(self):
		return max(self.container)

	def isFull(self):
		if (len(self.container) == self.container.maxlen):
			log.debug("Measurement queue is already filled with data")
			return True
		else:
			log.debug("Measurement queue is not yet filled with data")
			return False
		
wlatestMonitor = SlidingMonitor();
wgustMonitor = SlidingMonitor();

#a = astral.Astral()
#a.solar_depression = 'civil'
#astralCity = a[astral.Astral.Configuration.City()]
#astralCity.latitude = latitude
#astralCity.longitude = longitude
#astralCity.city_name = city

def main_code():
	lastDate = ""
	lastDateNumeric = 0
	wasOpened = False
	datetimeLastChange = datetime.datetime.now()
	while True:
		#sunParams = astral.sun.sun(astralCity.observer, date=datetime.datetime.now(), tzinfo=pytz.timezone(astralCity.timezone))
		with open(gaugeFile) as gaugeFile_json:
			data = json.load(gaugeFile_json)
			assert(data['windunit'] == 'km/h')
			log.debug("Storing wlatest " + data['wlatest'])
			wlatestMonitor.append(float(data['wlatest']))
			log.debug("Storing wgust " + data['wgust'])
			wgustMonitor.append(float(data['wgust']))
			if(float(data['wlatest']) > float(data['wgust'])):
				log.warn("Unexpected situation: wlatest > wgust (" + data['wlatest'] + " > " + data['wgust'] + ")")
			if(lastDate != "" and lastDate == data['date']):
				log.warn("Unexpected situation: gauge hasn't been updated during the last readout period")
			else:
				lastDate = data['date']
				try:
					lastDateNumeric  = datetime.datetime.strptime(data['date'], re.sub('([A-Za-z])', '%\\1', data['dateFormat']))
					log.debug("Last date numeric is " + str(lastDateNumeric))
				except Exception as e:
					log.error("Failed to parse measurement date: '" + data['date'] + "' with format '" + data['dateFormat'] + "'" + "\n" + str(e))
		log.debug("Sliding average of wlatest is " + str(wlatestMonitor.getAvg()))

		# we only start controlling the rollers once we have complete sliding window to average, otherwise we would risk raising/restoring rollers based on initial noise
		if(wlatestMonitor.isFull()):
			datetimeNow = datetime.datetime.now()
			timeDiff = datetimeNow - datetimeLastChange
			timeDiffMinutes = int(timeDiff.total_seconds() / 60.0)
			if wlatestMonitor.getAvg() > avgWindThreshold or wgustMonitor.getAvg() > avgGustThreshold:
				# this is normal condition to raise the rollers
				if not wasOpened:
					# this is safety so that we don't open the rollers too often
					if timeDiffMinutes >= timeOpenThresholdMinutes:
						log.info("Rising rollers")
						wasOpened = True
						datetimeLastChange = datetime.datetime.now()
						for z in zaluzie:
							z.submitRequest(ShellyRollerControllerRequest.OPEN)
				# this is for cases where the rollers have been moved but should still be open because of the wind
				else:
					for z in zaluzie:
						if z.getPos() != 100:
							log.info("Re-rising roller " + z.getNameIP() + " - something has closed them in the meantime")
							z.submitRequest(ShellyRollerControllerRequest.OPEN)
			# restoring is only done when the wind/gusts drop substantially - that is 0.7 time the thresholds
			elif wlatestMonitor.getAvg() < 0.7*avgWindThreshold and wgustMonitor.getAvg() < 0.7*avgGustThreshold:
				if wasOpened and timeDiffMinutes >= timeRestoreThresholdMinutes:
					log.info("Restoring rollers")
					wasOpened = False
					datetimeLastChange = datetime.datetime.now()
					for z in zaluzie:
						z.submitRequest(ShellyRollerControllerRequest.RESTORE)
		time.sleep(sleepTime)

if args.nodaemon:
	try:
		main_code()
	except KeyboardInterrupt:
		log.info("Terminating")
		for z in zaluzie:
			z.requestShutdown()
		exit(0)
else:
	with daemon.DaemonContext():
		main_code()

