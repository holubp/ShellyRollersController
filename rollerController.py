#!/usr/bin/python
# vim:ts=8:sw=8:tw=0:noet

import requests
from requests.auth import HTTPBasicAuth
import requests.exceptions
CONNECTIONSMAXRETRIES = 10
CONNECTIONSRETRYTIMESTEPSECS = 1

import time
import datetime
import re
import os.path
import json
import collections
import Queue
from statistics import mean
from enum import Enum

import threading
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
executors = {
	'default': ThreadPoolExecutor(1)
}
import apscheduler.events

import argparse

import inspect

class ExtendAction(argparse.Action):
	"""
	This class is used to extend arrays for argparse
	"""

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
parser.add_argument('-r', '--dry-run', dest='dryrun', action='store_true', help='do everything as usual but do not touch the rollers')
parser.set_defaults(configFile='rollerController.conf', logFile=None, pidFile=None, dryrun=False)
args = parser.parse_args()

logFormatStr = "%(levelname)s|%(name)s|%(asctime)s|%(message)s"

logger = log.getLogger("ShellyRollerController")
formatter = log.Formatter(logFormatStr)

if args.logFile is None and not args.nodaemon:
	print "Log file has to be specified for the daemon mode"
	exit(1)

logLevel = log.WARN
if args.debug:
	logLevel = log.DEBUG
elif args.verbose:
	logLevel = log.INFO

log.getLogger('apscheduler').setLevel(log.WARN)
log.getLogger('urllib3.connectionpool').setLevel(log.WARN)

logHandlers = []

logFH = None
if args.logFile is not None:
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
if args.logFile is not None:
	log.basicConfig(level=logLevel, format=logFormatStr, filename=args.logFile[0], filemode='a')
else:
	log.basicConfig(level=logLevel, format=logFormatStr)

if args.pidFile is not None:
	pidFile = args.pidFile[0]

class ShellyRollerControllerRequest(object):
	pass

class ShellyRollerControllerRequestEvent(ShellyRollerControllerRequest):
	def __init__(self, targetPos):
		assert isinstance(targetPos, int)
		assert targetPos >= 0 and targetPos <= 100
		self.targetPos = targetPos

	def __str__(self):
		return "ShellyRollerControllerRequestEvent, target position " + str(self.targetPos)

class ShellyRollerControllerRequestWindType(Enum):
	RESTORE = 0
	OPEN = 1
	CLOSE = 2

class ShellyRollerControllerRequestWind(ShellyRollerControllerRequest):
	def __init__(self, action):
		# XXX this is a weird Python bug, returing int instead of ShellyRollerControllerRequestWindType
		#assert isinstance(action, ShellyRollerControllerRequestWindType), "Expected ShellyRollerControllerRequestWindType, got " + str(type(action))
		self.action = action

	def __str__(self):
		return "ShellyRollerControllerRequestWind, target action " + str(self.action)

class ShellyRollerControllerEmulator:
	def __init__(self):
		self.state = {}
		self.state['current_pos'] = 5
		self.state['is_valid'] = True
		self.state['calibrating'] = False
		self.state['positioning'] = True
		self.state['state'] = 'stop'
		self.state['last_direction'] = "open"
		self.state['stop_reason'] = "normal"

	def setPos(self, pos):
		assert isinstance(pos, int)
		if self.state['current_pos'] < pos:
			self.state['last_direction'] = "open"
		if self.state['current_pos'] > pos:
			self.state['last_direction'] = "close"
		# don't touch when already in the position
		self.state['current_pos'] = pos
		return self.state

	def getState(self):
		return self.state

shellyRollerControllerEmulator = None
if args.dryrun:
	shellyRollerControllerEmulator = ShellyRollerControllerEmulator()

class ShellyRollerController:

	def __init__(self, name, IP, authUserName, authPassword, solarAzimuthMin, solarAzimuthMax):
		assert isinstance(authUserName, str), "Expected string, got " + str(type(authUserName))
		assert isinstance(authPassword, str), "Expected string, got " + str(type(authPassword))
		assert isinstance(solarAzimuthMin, int), "Expected int, got " + str(type(solarAzimuthMin))
		assert isinstance(solarAzimuthMax, int), "Expected int, got " + str(type(solarAzimuthMax))

		self.requestQueue = Queue.Queue()
		self.name = name
		self.IP = IP
		self.authUserName = authUserName
		self.authPassword = authPassword
		self.solarAzimuthMin = solarAzimuthMin
		self.solarAzimuthMax = solarAzimuthMax
		self.shutdownRequested = False
		self.savedState = None
		self.savedStateLock = threading.RLock()
		self.restorePos = None

		# check that the roller is properly configured - in a positioning state
		state = self.getState()
		assert state['positioning'] == True

		self.mainThread = threading.Thread(target=self.rollerMainThread)
		self.mainThread.start()
		logger.info("Roller thread started for {nameIP}".format(nameIP=self.getNameIP()))

	def getNameIP(self):
		return str(self.name + '/' + self.IP)

	def getHTTPResp(self, urlPath):
		assert isinstance(urlPath, str)
		resp = None
		if args.dryrun:
			assert shellyRollerControllerEmulator is not None
			resp = shellyRollerControllerEmulator.getState()
		else:
			connectionTry = 0
			targetUrl = 'http://' + self.IP + urlPath
			try:
				while connectionTry < CONNECTIONSMAXRETRIES:
					try:
						connectionTry += 1
						time.sleep((connectionTry-1)*CONNECTIONSRETRYTIMESTEPSECS)
						logger.debug("Connecting to " + self.getNameIP() + " (try " + str(connectionTry) + "): " + targetUrl)
						resp = requests.get(targetUrl, auth=HTTPBasicAuth(self.authUserName, self.authPassword))
						break
					except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout) as e:
						logger.warn("Failed to connect to " + self.getNameIP() + " - retrying: " + str(e))
			except requests.exceptions.RequestException as e:
				logger.error("Failed to connect to " + self.getNameIP() + ": " + str(e))
		return resp

	def getState(self):
		if args.dryrun:
			assert shellyRollerControllerEmulator is not None
			resp = shellyRollerControllerEmulator.getState()
			return resp
		else:
			resp = self.getHTTPResp('/roller/0/state')
			if resp.status_code != 200:
				logger.error('Unable to get state for roller ' + self.getNameIP + ' ... received return code ' + str(resp.status_code))
				return None
			else:
				return resp.json()

	def getStoppedState(self):
		state = self.waitUntilStopGetState()
		return state

	def getPos(self):
		state = self.getStoppedState()
		return int(state['current_pos'])

	def setPosURL(self):
		return '/roller/0?go=to_pos&roller_pos='

	def setPos(self, pos):
		assert isinstance(pos, int)
		assert pos >= 0 and pos <= 100
		if args.dryrun:
			assert shellyRollerControllerEmulator is not None
			state = shellyRollerControllerEmulator.getState()
		else:
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
					if args.dryrun:
						assert shellyRollerControllerEmulator is not None
						resp = shellyRollerControllerEmulator.setPos(pos)
					else:
						resp = self.getHTTPResp(self.setPosURL() + '0')
						if resp.status_code != 200:
							logger.error('Unable to get state for roller ' + self.getNameIP + ' ... received return code ' + str(resp.status_code))
					self.waitUntilStopGetState()
			# now we can always set the desired state
			logger.info("Setting rollers to the desired state: " + str(pos))
			if args.dryrun:
				assert shellyRollerControllerEmulator is not None
				resp = shellyRollerControllerEmulator.getState()
			else:
				resp = self.getHTTPResp(self.setPosURL() + str(pos))
				if resp.status_code != 200:
					logger.error('Unable to get state for roller ' + self.getNameIP + ' ... received return code ' + str(resp.status_code))
				self.waitUntilStopGetState()

	# this is for the scheduled events to minimize interference
	def setPosIfNotWindy(self, pos):
		assert isinstance(pos, int)
		assert pos >= 0 and pos <= 100
		try:
			self.savedStateLock.acquire()
			if self.savedState is not None:
				self.setPos(pos)
			else:
				# if the roller is up due to the wind, we set the restore position instead
				self.savedState = pos
		finally:
			self.savedStateLock.release()

	def requestShutdown(self):
		self.shutdownRequested = True
		self.mainThread.join()
		logger.info("Roller thread stopped for " + self.getNameIP())

	def rollerMainThread(self):
		while not self.shutdownRequested:
			try:
				request = self.requestQueue.get(block=True, timeout=1)
				assert isinstance(request, ShellyRollerControllerRequest)
				if isinstance(request, ShellyRollerControllerRequestWind):
					if request.action == ShellyRollerControllerRequestWindType.OPEN:
						self.saveState()
						self.setPos(100)
						self.restorePos = 100
					elif request.action == ShellyRollerControllerRequestWindType.CLOSE:
						self.saveState()
						self.setPos(0)
						self.restorePos = 0
					elif request.action == ShellyRollerControllerRequestWindType.RESTORE:
						self.restoreState()
					else:
						logger.error('Got unknown wind request: ' + str(request))
				elif isinstance(request, ShellyRollerControllerRequestEvent):
					self.setPos(request.targetPos)
				else:
					logger.error('Got unknown request type: ' + str(request) + ' is of type ' + str(type(request)))
			except Queue.Empty:
				pass

	def waitUntilStopGetState(self):
		while True:
			time.sleep(1)
			state = self.getState()
			if state['is_valid'] == True and not state['calibrating'] and state['state'] == 'stop':
				return state

	def saveState(self):
		# wait until roller gets into stabilized state
		try:
			self.savedStateLock.acquire()
			state = self.waitUntilStopGetState()
			# XXX review this after we enabled closing due to sun/temp: save state if not open or closed
			if int(state['current_pos']) not in (0, 100):
				self.savedState = state
		finally:
			self.savedStateLock.release()
	
	def overrideSavedStatePos(self, pos):
		assert isinstance(pos, int)
		try:
			self.savedStateLock.acquire()
			if self.savedState is not None:
				self.savedState['current_pos'] = pos
		finally:
			self.savedStateLock.release()

	def restoreState(self):
		try:
			self.savedStateLock.acquire()
			if self.savedState is not None:
				currentState = self.getState()
				assert self.restorePos is not None
				if int(currentState['current_pos']) == self.restorePos:
					self.setPos(self.savedState['current_pos'])
					self.savedState = None
					self.restorePos = None
				else:
					logger.info("Not restoring the state as the roller was moved in the meantime - expected "
						+ str(self.restorePos) + " state, got " + str(int(currentState['current_pos'])))
		finally:
			self.savedStateLock.release()

	def submitRequest(self, request):
		assert isinstance(request, ShellyRollerControllerRequest), "Request shall be ShellyRollerControllerRequest type, got " + str(type(request))
		try:
			self.savedStateLock.acquire()
			if self.savedState is None:
				logger.debug("Roller ", self, ": submitting request ", request)
				try:
					self.requestQueue.put(request, block=True, timeout=2)
				except Queue.Full:
					logger.warn("Request queue for roller is full - not submitting request " + str(request))
			else:
				if isinstance(request, ShellyRollerControllerRequestEvent):
					logger.debug("Roller " + str(self) + ": modifying restore state to " + str(request.targetPos))
					self.overrideSavedStatePos(request.targetPos)
				else:
					logger.warn("Received wind-/sun-related request while savedState == True - not submitting request " + str(request) )
		finally:
			self.savedStateLock.release()

avgWindThreshold = 40.0
avgGustThreshold = 70.0
timeOpenThresholdMinutes = 10
timeRestoreThresholdMinutes = 30
closeAtTemperatureAtAnyAzimuth = 30
closeAtTemperatureAtDirectSunlight = 25

gaugeFile = '/tmp/gauge-data.txt'
sleepTime = 60
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
	config = json.load(configFile)
	if "thresholds" in config:
		for k in ('avgWindThreshold', 'avgGustThreshold', 'timeOpenThresholdMinutes', 'timeRestoreThresholdMinutes', 'closeAtTemperatureAtAnyAzimuth', 'closeAtTemperatureAtDirectSunlight'):
			exec(k + " = config['thresholds'].get('" + k + "', " + k + ")")
	if "rollers" in config:
		for roller in config['rollers']:
			rollers.append(ShellyRollerController(roller['name'], str(roller['IP']), str(roller['rollerUsername']), str(roller['rollerPassword']), roller['solarAzimuthMin'], roller['solarAzimuthMax']))
	if "WeeWxGaugeFile" in config:
		gaugeFile = config['WeeWxGaugeFile'].get('location', gaugeFile)
		sleepTime = config['WeeWxGaugeFile'].get('readPeriodSecs', sleepTime)
		historyLength = config['WeeWxGaugeFile'].get('numberOfAvergagedReadings', historyLength)
	if "location" in config:
		city.latitude = config['location'].get('latitude', city.latitude)
		city.longitude = config['location'].get('longitude', city.longitude)
		city.name = config['location'].get('city', city.name)
		city.country = config['location'].get('region', city.region)
		city.timezone = config['location'].get('timezone', city.timezone)
		city.elevation = config['location'].get('elevation', city.elevation)


if not os.path.isfile(gaugeFile):
	print "Gauge file " + gaugeFile + " does not exist!"
	exit(1)


class SlidingMonitor:

	def __init__(self):
		self.container = collections.deque(maxlen=historyLength)

	def append(self, x):
		assert isinstance(x, float)
		self.container.append(x)

	def getAvg(self):
		return mean(self.container)

	def getMax(self):
		return max(self.container)

	def isFull(self):
		if len(self.container) == self.container.maxlen:
			logger.debug("Measurement queue is already filled with data")
			return True
		else:
			logger.debug("Measurement queue is not yet filled with data")
			return False

wlatestMonitor = SlidingMonitor()
wgustMonitor = SlidingMonitor()
tempMonitor = SlidingMonitor()

scheduler = BackgroundScheduler(daemon=True, timezone=config['location']['timezone'], job_defaults={'misfire_grace_time': 5*60})

def getScheduledJobList():
	return "Scheduled jobs\n" + "\n".join(["id=" + str(j.id) + " name=" + str(j.name) + ' next_run_time=' + str(j.next_run_time.strftime("%Y-%m-%d %H:%M:%S %Z")) for j in scheduler.get_jobs()])

def logInfoScheduledJobList():
	logger.info(getScheduledJobList())

def logDebugScheduledJobList():
	logger.debug(getScheduledJobList())

def getNextSunEvent(event, offsetSeconds=None, minTime=None, maxTime=None, todayFired=False):
	assert isinstance(event, str)
	assert offsetSeconds is None or isinstance(offsetSeconds, int)
	assert minTime is None or isinstance(minTime, datetime.time)
	assert maxTime is None or isinstance(maxTime, datetime.time)

	def getSunEvent():
		sunEvent = sun[event].replace(tzinfo=None)
		if offsetSeconds is not None:
			sunEvent = sunEvent + datetime.timedelta(seconds=offsetSeconds)
		if minTime is not None and sunEvent.time() < minTime:
			sunEvent = sunEvent.replace(hour=minTime.hour, minute=minTime.minute, second=minTime.second)
		if maxTime is not None and sunEvent.time() > maxTime:
			sunEvent = sunEvent.replace(hour=maxTime.hour, minute=maxTime.minute, second=maxTime.second)
		return sunEvent

	sun = city.sun(date=datetime.date.today(), local=True)
	sunEvent = getSunEvent()
	if datetime.datetime.now().replace(tzinfo=None) >= sunEvent or todayFired:
		sun = city.sun(date=datetime.date.today() + datetime.timedelta(days=1), local=True)
		sunEvent = getSunEvent()
	#log.debug('sunEvent="' + str(sunEvent) + '" minTime="' + str(minTime) + '" maxTime= "' + str(maxTime) + '"')
	return sunEvent

class SunJob:

	def __init__(self, job, event, offsetSeconds=None, minTime=None, maxTime=None, args=None, kwargs=None):
		assert offsetSeconds is None or isinstance(offsetSeconds, int)
		assert minTime is None or isinstance(minTime, datetime.time)
		assert maxTime is None or isinstance(maxTime, datetime.time)
		assert args is None or isinstance(args, list)
		assert kwargs is None or isinstance(kwargs, dict)
		self.job = job
		self.args = args
		self.kwargs = kwargs
		self.event = event
		self.offsetSeconds = offsetSeconds
		self.minTime = minTime
		self.maxTime = maxTime

	def getFunctionNameWithParams(self):
		paramsList = []
		if self.args is not None:
			for i in self.args:
				paramsList.append(str(i))
		if self.kwargs is not None:
			for k,v in self.kwargs.items():
				paramsList.append(str(k) + "=" + str(v))
		return str(self.job.__name__) + "(" + ",".join(paramsList) + ")"

sunJobs = []
sunJobsIds = []

def schedulerSunJobAdd(sunJob, todayFired=False):
	log.debug("Scheduling " + sunJob.getFunctionNameWithParams() + " to "
		+ str(getNextSunEvent(sunJob.event, offsetSeconds=sunJob.offsetSeconds, minTime=sunJob.minTime, maxTime=sunJob.maxTime, todayFired=todayFired)))
	return scheduler.add_job(sunJob.job, trigger='date',
		next_run_time=str(getNextSunEvent(sunJob.event, offsetSeconds=sunJob.offsetSeconds, minTime=sunJob.minTime, maxTime=sunJob.maxTime, todayFired=todayFired)),
		args=sunJob.args, kwargs=sunJob.kwargs, name=sunJob.getFunctionNameWithParams())

def scheduleSunJob(job, event, offsetSeconds=None, minTime=None, maxTime=None, args=None, kwargs=None):
	sunJob = SunJob(job, event, offsetSeconds=offsetSeconds, minTime=minTime, maxTime=maxTime, args=args, kwargs=kwargs)
	sunJobs.append(sunJob)
	job = schedulerSunJobAdd(sunJob)
	sunJobsIds.append(job.id)

def rescheduleSunJobs(event):
	"""
	This method is a listener to each job termination -- checks if it is one of the sunJobs and if so, reschedules it.
	"""
	jobId = event.job_id
	if event.exception:
		logger.warn("Job " + str(jobId) + " failed")
	else:
		logger.debug("Checking if to reschedule job " + str(jobId))
		if jobId in sunJobsIds:
			sunJobIdx = sunJobsIds.index(jobId)
			sunJob = sunJobs[sunJobIdx]
			logger.debug("Rescheduling " + str(sunJob))
			job = schedulerSunJobAdd(sunJob, todayFired=True)
			assert job.next_run_time.replace(tzinfo=None) > datetime.datetime.now().replace(tzinfo=None) + datetime.timedelta(hours=1), "Sun jobs are not likely to repeat in less than 1 hour, please examine:\n" + str(event) + "\n" + str(job)
			sunJobsIds[sunJobIdx] = job.id
			logInfoScheduledJobList()

def scheduleDateJob(job, date):
	jobString = job.__name__
	if jobString == "<lambda>":
		try:
			jobString = jobString + ' from "' + str(inspect.getsource(job)).strip() + '"'
		except IOError:
			pass
	log.info("Scheduling " + jobString + " to " + str(date))
	return scheduler.add_job(job, trigger='date', next_run_time=date)

wasOpened = False
datetimeLastMovedWindSun = datetime.datetime.now()

wasClosedDueToTemp = False
wasClosedDueToTempAndSunAzimuth = {}
for r in rollers:
	wasClosedDueToTempAndSunAzimuth[r] = False

def requestPositionAllRollers(pos):
	global wasOpened
	global wasClosedDueToTemp
	global wasClosedDueToTempAndSunAzimuth

	for r in rollers:
		if wasOpened or wasClosedDueToTemp or wasClosedDueToTempAndSunAzimuth[r]:
			logger.debug("Overriding restore state because the roller " + str(r) + " is in wind/sun-related state. Target pos is " + str(pos))
			r.overrideSavedStatePos(pos)
		else:
			logger.debug("Setting state of roller " + str(r) + ". Target pos is " + str(pos))
			r.submitRequest(ShellyRollerControllerRequestEvent(pos))

class WeeWxReadoutError(Exception):
	pass

def main_code():
	lastDate = ""
	lastDateNumeric = 0

	global wasOpened
	global datetimeLastMovedWindSun
	global wasClosedDueToTemp
	global wasClosedDueToTempAndSunAzimuth

	scheduler.add_listener(rescheduleSunJobs, apscheduler.events.EVENT_JOB_EXECUTED | apscheduler.events.EVENT_JOB_ERROR)

	scheduleSunJob(requestPositionAllRollers, 'dawn', args=[2])
	scheduleSunJob(requestPositionAllRollers, 'sunrise', args=[5])
	#scheduleSunJob(requestPositionAllRollers(0), 'sunset', offsetSeconds=-12500, minTime=datetime.time(hour=19, minute=00), maxTime=datetime.time(hour=21, minute=00))
	scheduleSunJob(requestPositionAllRollers, 'dusk', args=[0], maxTime=datetime.time(hour=21, minute=00))

	if args.testRollers:
		scheduler.add_job(logDebugScheduledJobList, 'interval', seconds=5)
		# this is to test movement of rollers
		#logger.debug("ShellyRollerControllerRequestWindType.CLOSE is of type " + str(type(ShellyRollerControllerRequestWindType.CLOSE)) + " and repr is " + repr(ShellyRollerControllerRequestWindType.CLOSE))
		scheduleDateJob(lambda: [r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.CLOSE)) for r in rollers], datetime.datetime.now() + datetime.timedelta(seconds=1))
		scheduleDateJob(lambda: [r.submitRequest(ShellyRollerControllerRequestEvent(15)) for r in rollers], datetime.datetime.now() + datetime.timedelta(seconds=15))
		scheduleDateJob(lambda: [r.submitRequest(ShellyRollerControllerRequestEvent(2)) for r in rollers], datetime.datetime.now() + datetime.timedelta(seconds=35))
		scheduleDateJob(lambda: [r.submitRequest(ShellyRollerControllerRequestEvent(5)) for r in rollers], datetime.datetime.now() + datetime.timedelta(seconds=55))
		scheduleDateJob(lambda: [r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.OPEN)) for r in rollers], datetime.datetime.now() + datetime.timedelta(seconds=75))
		scheduleDateJob(lambda: [r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.RESTORE)) for r in rollers], datetime.datetime.now() + datetime.timedelta(seconds=120))
		scheduleDateJob(lambda: [r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.CLOSE)) for r in rollers], datetime.datetime.now() + datetime.timedelta(seconds=150))
		scheduleDateJob(lambda: [r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.RESTORE)) for r in rollers], datetime.datetime.now() + datetime.timedelta(seconds=180))
	elif args.debug:
		scheduler.add_job(logDebugScheduledJobList, 'interval', seconds=30)

	scheduler.start()
	logInfoScheduledJobList()
	while True:
		#sunParams = astral.sun.sun(astralCity.observer, date=datetime.datetime.now(), tzinfo=pytz.timezone(astralCity.timezone))
		try:
			with open(gaugeFile) as gaugeFile_json:
				data = json.load(gaugeFile_json)
				assert data['windunit'] == 'km/h'
				if lastDate != "" and lastDate == data['date']:
					raise WeeWxReadoutError("Gauge hasn't been updated during the last readout period - ignoring")
				else:
					lastDate = data['date']
					try:
						lastDateNumeric = datetime.datetime.strptime(data['date'], re.sub('([A-Za-z])', '%\\1', data['dateFormat']))
						logger.debug("Last date numeric is " + str(lastDateNumeric))
					except Exception as e:
						logger.error("Failed to parse measurement date: '" + data['date'] + "' with format '" + data['dateFormat'] + "'" + "\n" + str(e))
				logger.debug("Storing wlatest " + data['wlatest'])
				wlatestMonitor.append(float(data['wlatest']))
				logger.debug("Storing wgust " + data['wgust'])
				wgustMonitor.append(float(data['wgust']))
				logger.debug("Storing temp " + data['temp'])
				tempMonitor.append(float(data['temp']))
				if float(data['wlatest']) > float(data['wgust']):
					logger.warn("Unexpected situation: wlatest > wgust ({wlatest} > {wgust})".format(wlatest=data['wlatest'], wgust=data['wgust']))
			logger.debug("Sliding average of wlatest is {avg}".format(avg=wlatestMonitor.getAvg()))
			logger.debug("Sliding average of temp is {avg}".format(avg=tempMonitor.getAvg()))

			# wind processing gets higher priority
			# we only start controlling the rollers once we have complete sliding window to average, otherwise we would risk raising/restoring rollers based on initial noise
			if wlatestMonitor.isFull():
				datetimeNow = datetime.datetime.now()
				timeDiff = datetimeNow - datetimeLastMovedWindSun
				timeDiffMinutes = int(timeDiff.total_seconds() / 60.0)
				if wlatestMonitor.getAvg() > avgWindThreshold or wgustMonitor.getAvg() > avgGustThreshold:
					# this is normal condition to raise the rollers
					logger.debug("Wind is above the set threshold: wlatestMonitor.getAvg()={wl} avgWindThreshold={aWT} wgustMonitor.getAvg()={wg} avgGustThreshold={aGT}".format(wl=wlatestMonitor.getAvg(), aWT=avgWindThreshold, wg=wgustMonitor.getAvg(), aGT=avgGustThreshold))
					if not wasOpened:
						# this is safety so that we don't open the rollers too often
						if timeDiffMinutes >= timeOpenThresholdMinutes:
							logger.info("Rising rollers because of wind")
							wasOpened = True
							wasClosedDueToTemp = False
							for r in rollers:
								wasClosedDueToTempAndSunAzimuth[r] = False
								r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.OPEN))
							datetimeLastMovedWindSun = datetime.datetime.now()
						else:
							logger.debug("Conditions met to rise rollers after wind increased, except time threshold not ready yet: timeDiffMinutes={td} timeOpenThresholdMinutes={to}".format(td=timeDiffMinutes, to=timeOpenThresholdMinutes))
					# this is for cases where the rollers have been moved but should still be open because of the wind
					else:
						for r in rollers:
							if r.getPos() != 100:
								logger.info("Re-rising roller " + r.getNameIP() + " - something has closed them in the meantime")
								r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.OPEN))
				# restoring is only done when the wind/gusts drop substantially - that is windRestoreCoefficiet time the thresholds
				elif wasOpened and wlatestMonitor.getAvg() < windRestoreCoefficiet*avgWindThreshold and wgustMonitor.getAvg() < windRestoreCoefficiet*avgGustThreshold:
					if timeDiffMinutes >= timeRestoreThresholdMinutes:
						logger.info("Restoring rollers after wind died down")
						wasOpened = False
						datetimeLastMovedWindSun = datetime.datetime.now()
						for r in rollers:
							r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.RESTORE))
					else:
						logger.debug("Conditions met to restore rollers after wind died down, except time threshold not ready yet: timeDiffMinutes={td} timeOpenThresholdMinutes={to}".format(td=timeDiffMinutes, to=timeOpenThresholdMinutes))
			# temperature-based decisiona are only implemented if wind-based decisions are not interfering
			if tempMonitor.isFull():
				solarElevation = int(city.solar_elevation(datetime.datetime.now()))
				logger.debug("Checking temperature-based roller control:" +
					" tempMonitor.getAvg()=" + str(tempMonitor.getAvg()) +
					" closeAtTemperatureAtAnyAzimuth=" + str(closeAtTemperatureAtAnyAzimuth) +
					" closeAtTemperatureAtDirectSunlight=" + str(closeAtTemperatureAtDirectSunlight) +
					" wasClosedDueToTemp=" + str(wasClosedDueToTemp) +
					" temperatureRestoreCoefficient*closeAtTemperatureAtDirectSunlight=" + str(temperatureRestoreCoefficient*closeAtTemperatureAtDirectSunlight) +
					" solarElevation=" + str(solarElevation))
				if tempMonitor.getAvg() >= closeAtTemperatureAtAnyAzimuth and solarElevation >= 0:
					logger.debug("Conditions met to close rollers due to temperature")
					if not wasOpened and not wasClosedDueToTemp:
						# this is safety so that we don't open the rollers too often
						logger.debug("Conditions met to close rollers due to temperature, not conflicting with the wind" +
							" tempMonitor.getAvg()=" + str(tempMonitor.getAvg()) +
							" closeAtTemperatureAtAnyAzimuth=" + str(closeAtTemperatureAtAnyAzimuth) +
							" wasOpened=" + str(wasOpened) +
							" wasClosedDueToTemp=" + str(wasClosedDueToTemp))
						if timeDiffMinutes >= timeOpenThresholdMinutes:
							scheduleDateJob(lambda: [r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.CLOSE)) for r in rollers], datetime.datetime.now() + datetime.timedelta(seconds=1))
							wasClosedDueToTemp = True
							datetimeLastMovedWindSun = datetime.datetime.now()
						else:
							logger.debug("Conditions met to close rollers for temperature conditions, except time threshold not ready yet: " +
								" timeDiffMinutes=" + str(timeDiffMinutes) +
								" timeOpenThresholdMinutes=" + str(timeOpenThresholdMinutes))
					#elif wasOpened and not wasClosedDueToTemp:
						# only change the restore state but overwise don't touch it
						#for r in rollers:
							#logger.info("Overriding restore state after wind dies to 0 due to temperature conditions")
							#r.submitRequest(ShellyRollerControllerRequestEvent(0))
				elif tempMonitor.getAvg() >= closeAtTemperatureAtDirectSunlight and solarElevation >= 0 and not wasClosedDueToTemp:
					logger.debug("Conditions met to close rollers due to temperature and sunshine direction")
					solarAzimuth = int(city.solar_azimuth(datetime.datetime.now()))
					# this is safety so that we don't open the rollers too often
					if timeDiffMinutes >= timeOpenThresholdMinutes:
						for r in rollers:
							logger.debug("Conditions match to close roller " + str(r) + " due to direct sunlight:" +
							" tempMonitor.getAvg()=" + str(tempMonitor.getAvg()) +
							" closeAtTemperatureAtDirectSunlight=" + str(closeAtTemperatureAtDirectSunlight) +
							" solarAzimuth=" + str(solarAzimuth) +
							" r.solarAzimuthMin=" + str(r.solarAzimuthMin) +
							" r.solarAzimuthMax=" + str(r.solarAzimuthMax) +
							" wasClosedDueToTempAndSunAzimuth[r]=" + str(wasClosedDueToTempAndSunAzimuth[r]))
							if solarAzimuth >= r.solarAzimuthMin and solarAzimuth <= r.solarAzimuthMax and not wasClosedDueToTempAndSunAzimuth[r]:
								logger.info("Closing roller " + str(r) + " due to direct sunlight")
								scheduleDateJob(lambda: r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.CLOSE)), datetime.datetime.now() + datetime.timedelta(seconds=1))
								wasClosedDueToTempAndSunAzimuth[r] = True
						datetimeLastMovedWindSun = datetime.datetime.now()
						time.sleep(2)

				if tempMonitor.getAvg() < temperatureRestoreCoefficient*closeAtTemperatureAtDirectSunlight:
					logger.debug("Conditions met to restore rollers due to temperature")
					if wasClosedDueToTemp and timeDiffMinutes >= timeRestoreThresholdMinutes:
						logger.debug("Conditions match to restore roller " + str(r) + " due to decreased temperature:" +
							" tempMonitor.getAvg()=" + str(tempMonitor.getAvg()) +
							" temperatureRestoreCoefficient*closeAtTemperatureAtDirectSunlight=" + str(temperatureRestoreCoefficient*closeAtTemperatureAtDirectSunlight) +
							" wasClosedDueToTemp=" + str(wasClosedDueToTemp) +
							" timeDiffMinutes=" + str(timeDiffMinutes) +
							" timeRestoreThresholdMinutes=" + str(timeRestoreThresholdMinutes))
						logger.info("Restoring rollers  due to decreased temperature")
						scheduleDateJob(lambda: [r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.RESTORE)) for r in rollers], datetime.datetime.now() + datetime.timedelta(seconds=1))
						datetimeLastMovedWindSun = datetime.datetime.now()
						wasClosedDueToTemp = False
						for r in rollers:
							wasClosedDueToTempAndSunAzimuth[r] = False
						time.sleep(2)

		except WeeWxReadoutError as e:
			logger.debug(e.message)

		except Exception as e:
			logger.error("Unexpected error happened when processing WeeWx data: " + str(e))

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
		stopAll(0, None)
		exit(0)
else:
#	from daemonize import Daemonize
#	assert logFH is not None
#	assert isinstance(pidFile, str) and pidFile != ""
#	assert logger is not None
#	try:
#		daemon = Daemonize(app = "rollerController", action = main_code, pid = pidFile, keep_fds = [ logFH.stream ], logger=logger)
#		daemon.sigterm = stopAll
#		logger.info("Daemonizing " + __name__)
#		daemon.start()
#	except:
#		raise

	import daemon
	from daemon.pidlockfile import PIDLockFile
	import signal
	assert logFH is not None
	assert isinstance(pidFile, str) and pidFile != ""
	log.info("Deamonizing " + __name__)
	with daemon.DaemonContext(files_preserve=[logFH.stream], pidfile=PIDLockFile(pidFile), signal_map={signal.SIGTERM : stopAll}):
		log.info("Deamonizing " + __name__)
		main_code()
