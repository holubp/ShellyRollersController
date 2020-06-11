#!/usr/bin/python
# vim:ts=8:sw=8:tw=0:noet

import requests
from requests.auth import HTTPBasicAuth
import requests.exceptions
connectionsMaxRetries = 10
connectionsRetryTimeStepSecs = 1

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
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
executors = {
    'default': ThreadPoolExecutor(1),
}

import argparse

class ExtendAction(argparse.Action):

	def __call__(self, parser, namespace, values, option_string=None):
		items = getattr(namespace, self.dest) or []
		items.extend(values)
		setattr(namespace, self.dest, items)


from astral import Astral, Location


import logging as log
import sys

class StreamToLogger(object):
	"""
	Fake file-like stream object that redirects writes to a logger instance.
	https://www.electricmonk.nl/log/2011/08/14/redirect-stdout-and-stderr-to-a-logger-in-python/
	"""
	def __init__(self, logger, log_level=log.INFO):
		self.logger = logger
		self.log_level = log_level
		self.linebuf = ''

	def write(self, buf):
		for line in buf.rstrip().splitlines():
			 self.logger.log(self.log_level, line.rstrip())

parser = argparse.ArgumentParser()
parser.register('action', 'extend', ExtendAction)
parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='verbose information on progress of the data checks')
parser.add_argument('-d', '--debug', dest='debug', action='store_true', help='debug information on progress of the data checks')
parser.add_argument('-n', '--no-daemon', dest='nodaemon', action='store_true', help='do not daemonize the process')
parser.add_argument('-t', '--test-rollers', dest='testRollers', action='store_true', help='test movement of rollers')
parser.add_argument('-c', '--config', dest='configFile', nargs=1, help='location of config file')
parser.add_argument('-l', '--log', dest='logFile', nargs=1, help='location of log file')
parser.add_argument('-p', '--pid', dest='pidFile', nargs=1, help='location of PID file')
parser.set_defaults(configFile = 'rollerController.conf', logFile = None, pidFile = None)
args = parser.parse_args()

logFormatStr="%(levelname)s|%(name)s|%(asctime)s|%(message)s"

logger = log.getLogger("ShellyRollerController")
formatter = log.Formatter(logFormatStr)

if args.logFile == None and not args.nodaemon:
	print("Log file has to be specified for the daemon mode")
	exit(1)

logLevel = log.WARN
if args.debug:
	logLevel=log.DEBUG
elif args.verbose:
	logLevel=log.INFO

logHandlers = []

if args.logFile != None:
	logFH = log.FileHandler(args.logFile[0])
	logFH.setFormatter(formatter)
	if args.debug or args.verbose:
		# file logging max to INFO, since otherwise we fill the storage too fast
		logFH.setLevel(log.INFO)
	logHandlers.append(logFH)

if args.nodaemon:
	logSH = log.StreamHandler()
	logSH.setFormatter(formatter)
	logHandlers.append(logSH)
else:
	stdout_logger = log.getLogger('STDOUT')
	sl = StreamToLogger(stdout_logger, log.INFO)
	sys.stdout = sl
	stderr_logger = log.getLogger('STDERR')
	sl = StreamToLogger(stderr_logger, log.ERROR)
	sys.stderr = sl

# XXX this is a temporary hack before we do it properly
# log.basicConfig(level=logLevel, format=logFormatStr, handlers = logHandlers)
if args.logFile != None:
	log.basicConfig(level=logLevel, format=logFormatStr, filename=args.logFile[0], filemode='a')
else:
	log.basicConfig(level=logLevel, format=logFormatStr)

if args.pidFile != None:
	pidFile = args.pidFile[0]

class ShellyRollerControllerRequest:
	def __init__(self):
		pass

class ShellyRollerControllerRequestEvent (ShellyRollerControllerRequest):
	def __init__(self, targetPos):
		assert(isinstance(targetPos, int))
		assert(targetPos >= 0 and targetPos <= 100)
		self.targetPos = targetPos

class ShellyRollerControllerRequestWindType(Enum):
	RESTORE = 0
	OPEN = 1

class ShellyRollerControllerRequestWind (ShellyRollerControllerRequest):
	def __init__(self, action):
		assert isinstance(action, ShellyRollerControllerRequestWindType)
		self.action = action

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
		logger.info("Roller thread started for " + self.getNameIP())

	def getNameIP(self):
		return str(self.name + '/' + self.IP)

	def getHTTPResp(self, urlPath):
		assert isinstance(urlPath, str)
		connectionTry = 0
		targetUrl = 'http://' + self.IP + urlPath
		resp = None
		try:
			while connectionTry < connectionsMaxRetries:
				try:
					connectionTry += 1
					time.sleep((connectionTry-1)*connectionsRetryTimeStepSecs)
					log.debug("Connecting (try " + str(connectionTry) + ") to " + self.getNameIP() + ": " + targetUrl)
					resp = requests.get(targetUrl, auth=HTTPBasicAuth(self.authUserName, self.authPassword))
					break
				except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout) as e:
					log.warn("Failed to connect to " + self.getNameIP() + " - retrying: " + str(e))
		except requests.exceptions.RequestException as e:
			log.error("Failed to connect to " + self.getNameIP() + ": " + str(e))
		return resp

	def getState(self):
		resp = self.getHTTPResp('/roller/0/state')
		if resp.status_code != 200:
			logger.error('Unable to get state for roller ' + self.getNameIP + ' ... received return code ' + str(resp.status_code))
		return(resp.json())
	
	def getStoppedState(self):
		state = self.waitUntilStopGetState()
		return state

	def getPos(self):
		state = self.getStoppedState()
		return int(state['current_pos'])

	def setPosURL(self):
		return '/roller/0?go=to_pos&roller_pos=' 

	def setPos(self, pos):
		assert(isinstance(pos, int))
		assert(pos >= 0 and pos <= 100)
		state = self.getStoppedState()
		logger.info("Processing request to set rollers to the desired state: " + str(pos))
		if int(state['current_pos']) == pos:
			logger.debug("Roller is already in the desired state")
		else:
			if pos > 0 and pos <= 10:
				# for correct tilt of roller one needs to shut them first and then open to the target
				if int(state['current_pos']) < pos and str(state['last_direction']) == "open" and str(state['stop_reason']) == "normal" and state['is_valid']:
					logger.debug("No need to prepare rollers by closing them first")
				else:
					logger.info("Preparing rollers by closing them first")
					resp = self.getHTTPResp(self.setPosURL() + '0')
					if resp.status_code != 200:
						logger.error('Unable to get state for roller ' + self.getNameIP + ' ... received return code ' + str(resp.status_code))
					self.waitUntilStopGetState()
			# now we can always set the desired state
			logger.info("Setting rollers to the desired state: " + str(pos))
			resp = self.getHTTPResp(self.setPosURL() + str(pos))
			if resp.status_code != 200:
				logger.error('Unable to get state for roller ' + self.getNameIP + ' ... received return code ' + str(resp.status_code))
			self.waitUntilStopGetState()

	# this is for the scheduled events to minimize interference
	def setPosIfNotWindy(self, pos):
		assert(isinstance(pos, int))
		assert(pos >= 0 and pos <= 100)
		if(self.savedState != None):
			self.setPos(pos)
		else:
			# if the roller is up due to the wind, we set the restore position instead
			self.savedState = pos

	def requestShutdown(self):
		self.shutdownRequested = True
		self.mainThread.join()
		logger.info("Roller thread stopped for " + self.getNameIP())

	def rollerMainThread(self):
		while not self.shutdownRequested:
			try:
				request = self.requestQueue.popleft()
				assert(isinstance(request, ShellyRollerControllerRequest))
				if isinstance(request, ShellyRollerControllerRequestWind):
					if(request.action == ShellyRollerControllerRequest.OPEN):
						self.saveState()
						self.setPos(100)
					elif(request.action == ShellyRollerControllerRequest.RESTORE):
						self.restoreState()
					else:
						logger.error('Got unknown wind request: ' + str(request))
				elif isinstance(request, ShellyRollerControllerRequestEvent):
					self.setPos(request.targetPos)
				else:
					logger.error('Got unknown request type: ' + str(request) + ' is of type ' + str(type(request)))
			except IndexError as i:
				pass
			time.sleep(1)
		
	def waitUntilStopGetState(self):
		while True:
			time.sleep(1)
			state = self.getState()
			if(state['is_valid'] == True and not state['calibrating'] and state['state'] == 'stop'):
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
				self.setPos(self.savedState['current_pos'])
				self.savedState = None
			else:
			 	logger.info("Not restoring the state as the roller was moved in the meantime - expected open (100) state, got " + int(currentState['current_pos']))

	def submitRequest(self, request):
		assert isinstance(request, ShellyRollerControllerRequest), "Request shall be ShellyRollerControllerRequest type, got " + str(type(request))
		logger.debug("Roller " + str(self) + ": submitting request " + str(request))
		self.requestQueue.append(request)

avgWindThreshold = 40.0
avgGustThreshold =  70.0
timeOpenThresholdMinutes = 10
timeRestoreThresholdMinutes = 30

gaugeFile = '/tmp/gauge-data.txt'
sleepTime =  60
historyLength = 5

a = Astral()
a.solar_depression = 'civil'

city = Location()
city.name = 'City'
city.region = 'Country'
city.latitude = 0.0
city.longitude = 0.0
city.timezone = 'UTC'
city.elevation = 0


rollers = collections.deque()

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
			rollers.append(ShellyRollerController(r['name'], str(r['IP']), str(r['rollerUsername']), str(r['rollerPassword'])))
	if "WeeWxGaugeFile" in config:
		gaugeFile = assignValFromDict(config['WeeWxGaugeFile'],'location', gaugeFile)
		sleepTime = assignValFromDict(config['WeeWxGaugeFile'],'readPeriodSecs', sleepTime)
		historyLength = assignValFromDict(config['WeeWxGaugeFile'],'numberOfAvergagedReadings', historyLength)
	if "location" in config:
		city.latitude = assignValFromDict(config['location'],'latitude', city.latitude)
		city.longitude = assignValFromDict(config['location'],'longitude', city.longitude)
		city.name = assignValFromDict(config['location'],'city', city.name)
		city.country = assignValFromDict(config['location'],'region', city.region)
		city.timezone = assignValFromDict(config['location'],'timezone', city.timezone)
		city.elevation = assignValFromDict(config['location'],'elevation', city.elevation)


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
			logger.debug("Measurement queue is already filled with data")
			return True
		else:
			logger.debug("Measurement queue is not yet filled with data")
			return False
		
wlatestMonitor = SlidingMonitor();
wgustMonitor = SlidingMonitor();

#a = astral.Astral()
#a.solar_depression = 'civil'
#astralCity = a[astral.Astral.Configuration.City()]
#astralCity.latitude = latitude
#astralCity.longitude = longitude
#astralCity.city_name = city

scheduler = BackgroundScheduler(daemon=True,timezone=config['location']['timezone'],job_defaults={'misfire_grace_time': 5*60})

def getNextSunEvent(event, offsetSeconds = None):
	assert isinstance(event, str)
	assert offsetSeconds == None or isinstance(offsetSeconds, int)
	sun = city.sun(date=datetime.date.today(), local=True)
	sunEvent = sun[event].replace(tzinfo=None)
	logger.debug(str(sunEvent))
	if offsetSeconds != None:
		sunEvent = sunEvent + datetime.timedelta(seconds=offsetSeconds)
		logger.debug(str(sunEvent))
	if datetime.datetime.now() >= sunEvent:
		sun = city.sun(date=datetime.date.today() + datetime.timedelta(days=1), local=True)
		sunEvent = sun[event].replace(tzinfo=None)
		if offsetSeconds != None:
			sunEvent = sunEvent + datetime.timedelta(seconds=offsetSeconds)
	return sunEvent

class SunJob:

	def __init__(self, job, event, offsetSeconds = None):
		self.job = job
		self.event = event
		self.offsetSeconds = offsetSeconds

sunJobs = []

def scheduleSunJobs():
	for job in sunJobs:
		logger.debug("Scheduling " + str(job))
		scheduler.add_job(job.job, trigger='date', next_run_time = str(getNextSunEvent(job.event, job.offsetSeconds)))

def registerSunJob(job, event, offsetSeconds = None):
	sunJob = SunJob(job, event, offsetSeconds)
	sunJobs.append(sunJob)

def scheduleDateJob(job, date):
	scheduler.add_job(job, trigger='date', next_run_time = date)

def main_code():
	lastDate = ""
	lastDateNumeric = 0
	wasOpened = False
	datetimeLastChange = datetime.datetime.now()
	registerSunJob(lambda : [r.submitRequest(ShellyRollerControllerRequestEvent(2)) for r in rollers], 'dawn')
	registerSunJob(lambda : [r.submitRequest(ShellyRollerControllerRequestEvent(5)) for r in rollers], 'sunrise')
	registerSunJob(lambda : [r.submitRequest(ShellyRollerControllerRequestEvent(0)) for r in rollers], 'dusk')
	scheduler.add_job(scheduleSunJobs, trigger='interval', hours=24, start_date=datetime.datetime.now()+datetime.timedelta(seconds=5))

	if args.testRollers:
		scheduler.add_job(lambda : scheduler.print_jobs(),'interval',seconds=5)
		# this is to test movement of rollers
		scheduleDateJob(lambda : [r.submitRequest(ShellyRollerControllerRequestEvent(15)) for r in rollers], datetime.datetime.now() + datetime.timedelta(seconds=15))
		scheduleDateJob(lambda : [r.submitRequest(ShellyRollerControllerRequestEvent(2)) for r in rollers], datetime.datetime.now() + datetime.timedelta(seconds=35))
		scheduleDateJob(lambda : [r.submitRequest(ShellyRollerControllerRequestEvent(5)) for r in rollers], datetime.datetime.now() + datetime.timedelta(seconds=55))
		scheduleDateJob(lambda : [r.submitRequest(ShellyRollerControllerRequestEvent(1)) for r in rollers], datetime.datetime.now() + datetime.timedelta(seconds=75))
	elif args.debug:
		scheduler.add_job(lambda : scheduler.print_jobs(),'interval',seconds=30)
	else:
		scheduler.add_job(lambda : scheduler.print_jobs(),'interval',seconds=300)

	scheduler.start()
	scheduler.print_jobs()
	while True:
		#sunParams = astral.sun.sun(astralCity.observer, date=datetime.datetime.now(), tzinfo=pytz.timezone(astralCity.timezone))
		try:
			with open(gaugeFile) as gaugeFile_json:
				data = json.load(gaugeFile_json)
				assert(data['windunit'] == 'km/h')
				logger.debug("Storing wlatest " + data['wlatest'])
				wlatestMonitor.append(float(data['wlatest']))
				logger.debug("Storing wgust " + data['wgust'])
				wgustMonitor.append(float(data['wgust']))
				if(float(data['wlatest']) > float(data['wgust'])):
					logger.warn("Unexpected situation: wlatest > wgust (" + data['wlatest'] + " > " + data['wgust'] + ")")
				if(lastDate != "" and lastDate == data['date']):
					logger.warn("Unexpected situation: gauge hasn't been updated during the last readout period")
				else:
					lastDate = data['date']
					try:
						lastDateNumeric  = datetime.datetime.strptime(data['date'], re.sub('([A-Za-z])', '%\\1', data['dateFormat']))
						logger.debug("Last date numeric is " + str(lastDateNumeric))
					except Exception as e:
						logger.error("Failed to parse measurement date: '" + data['date'] + "' with format '" + data['dateFormat'] + "'" + "\n" + str(e))
			logger.debug("Sliding average of wlatest is " + str(wlatestMonitor.getAvg()))

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
							logger.info("Rising rollers")
							wasOpened = True
							datetimeLastChange = datetime.datetime.now()
							for r in rollers:
								r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.OPEN))
					# this is for cases where the rollers have been moved but should still be open because of the wind
					else:
						for r in rollers:
							if r.getPos() != 100:
								logger.info("Re-rising roller " + r.getNameIP() + " - something has closed them in the meantime")
								r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.OPEN))
				# restoring is only done when the wind/gusts drop substantially - that is 0.7 time the thresholds
				elif wlatestMonitor.getAvg() < 0.7*avgWindThreshold and wgustMonitor.getAvg() < 0.7*avgGustThreshold:
					if wasOpened and timeDiffMinutes >= timeRestoreThresholdMinutes:
						logger.info("Restoring rollers")
						wasOpened = False
						datetimeLastChange = datetime.datetime.now()
						for r in rollers:
							r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.RESTORE))
		except e:
			log.error("Unexpected error happened when processing WeeWx data: " + str(e))

		time.sleep(sleepTime)

def stopAll(signum, frame):
	logger.info("Terminating")
	scheduler.shutdown(wait=True)
	for r in rollers:
		r.requestShutdown()


if args.nodaemon:
	try:
		main_code()
	except KeyboardInterrupt:
		stopAll(0,None)
		exit(0)
else:
	#from daemonize import Daemonize
	#daemon = Daemonize(app = "rollerController", action = main_code, pid = pidFile, keep_fds = [ logFH.stream ], logger=logger)
	#daemon.sigterm = stopAll
	#daemon.start()
	import daemon
	import signal
	import lockfile
	with daemon.DaemonContext(
			files_preserve=[ logFH.stream ],
			pidfile=lockfile.FileLock(pidFile),
			signal_map = {signal.SIGTERM : stopAll},
		):
		main_code()
