#!/usr/bin/python
# vim:ts=8:sw=8:tw=0:noet

import socket

import requests
from requests.auth import HTTPBasicAuth
import requests.exceptions
CONNECTIONSMAXRETRIES = 10
CONNECTIONSRETRYTIMESTEPSECS = 1
WEEWX_RESTART_CMD='/etc/init.d/weewx restart'
WEEWX_STALE_READ_TIMEOUT_MINS=30

import time
import datetime
import re
import os.path
import json
import collections
import Queue
from statistics import mean
from enum import Enum
import copy

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
import ephem
from math import degrees

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
		logger.debug("ShellyRollerControllerEmulator %s: setting position to %s", self, pos)
		if self.state['current_pos'] < pos:
			self.state['last_direction'] = "open"
		elif self.state['current_pos'] > pos:
			self.state['last_direction'] = "close"
		# don't touch when already in the position
		self.state['current_pos'] = pos
		logger.debug("ShellyRollerControllerEmulator %s: current_pos=%s last_direction=%s", self, self.state['current_pos'], self.state['last_direction'])
		return self.state

	def getState(self):
		return self.state

shellyRollerControllerEmulator = None
if args.dryrun:
	shellyRollerControllerEmulator = ShellyRollerControllerEmulator()

class ShellyRollerControllerException(Exception):
	pass

class ShellyRollerController:

	def __init__(self, name, hostname, authUserName, authPassword, solarAzimuthMin, solarAzimuthMax):
		assert isinstance(authUserName, str), "Expected string, got " + str(type(authUserName))
		assert isinstance(authPassword, str), "Expected string, got " + str(type(authPassword))
		assert isinstance(solarAzimuthMin, int), "Expected int, got " + str(type(solarAzimuthMin))
		assert isinstance(solarAzimuthMax, int), "Expected int, got " + str(type(solarAzimuthMax))

		self.requestQueue = Queue.Queue()
		self.name = name
		self.hostname = hostname
                self.IP = socket.gethostbyname(hostname)
		self.authUserName = authUserName
		self.authPassword = authPassword
		self.solarAzimuthMin = solarAzimuthMin
		self.solarAzimuthMax = solarAzimuthMax
		self.shutdownRequested = False
		self.__savedState = None
		self.__savedStateLock = threading.RLock()
		self.lastWindTempPos = None

		# check that the roller is properly configured - in a positioning state
		state = self.getState()
		assert state['positioning'] == True

		self.mainThread = threading.Thread(target=self.rollerMainThread)
		self.mainThread.start()
		logger.info("Roller %s: Roller thread started", self)

	def getNameHostname(self):
		return str(self.name + '/' + self.hostname)

	def __str__(self):
		return self.getNameHostname()

	def getHTTPResp(self, urlPath):
		assert isinstance(urlPath, str)
		assert not args.dryrun, "getHTTPResp should not be called at all when in the dry run mode"
		resp = None
		connectionTry = 0
		targetUrl = 'http://' + self.hostname + urlPath
		try:
			while connectionTry < CONNECTIONSMAXRETRIES:
				try:
					connectionTry += 1
					time.sleep((connectionTry-1)*CONNECTIONSRETRYTIMESTEPSECS)
					logger.debug("Roller %s: Connecting (try %s): %s", self, connectionTry, targetUrl)
					resp = requests.get(targetUrl, auth=HTTPBasicAuth(self.authUserName, self.authPassword))
					break
				except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout) as e:
					logger.warn("Roller %s: Failed to connect - retrying: %s", self, e)
		except requests.exceptions.RequestException as e:
			logger.error("Roller %s: Failed to connect to %s", self, e)
		return resp

	def getState(self):
		state = None
		if args.dryrun:
			assert shellyRollerControllerEmulator is not None
			state = shellyRollerControllerEmulator.getState()
		else:
			resp = self.getHTTPResp('/roller/0/state')
                        if resp is None:
				raise ShellyRollerControllerException('Roller %s: Failed to connect' % (self))
                        elif resp.status_code != 200:
				raise ShellyRollerControllerException('Roller %s: Unable to get state ... received HTTP return code %s' % (self, resp.status_code))
			else:
				state = resp.json()
		return state

	def getPos(self):
		state = self.waitUntilStopGetState()
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
			state = self.waitUntilStopGetState()
		logger.info("Roller %s: Processing request to set rollers to the desired state %s", self, pos)
		try:
			if int(state['current_pos']) == pos:
				logger.debug("Roller %s: Already in the desired state", self)
				return state
			else:
				if pos > 0 and pos <= 10:
					# for correct tilt of roller one needs to shut them first and then open to the target
					if int(state['current_pos']) < pos and str(state['last_direction']) == "open" and str(state['stop_reason']) == "normal" and state['is_valid']:
						logger.debug("Roller %s: No need to prepare rollers by closing them first", self)
					else:
						logger.info("Roller %s: Preparing rollers by closing them first", self)
						if args.dryrun:
							assert shellyRollerControllerEmulator is not None
							resp = shellyRollerControllerEmulator.setPos(0)
						else:
							resp = self.getHTTPResp(self.setPosURL() + '0')
							if resp.status_code != 200:
								raise ShellyRollerControllerException('Roller %s: Unable to set position 0 while preparing rollers by closing them first ... received HTTP return code %s' % (self, resp.status_code))
						state = self.waitUntilStopGetState()
						if int(state['current_pos']) != 0:
							raise ShellyRollerControllerException('Roller %s: Unable to set position 0 while preparing rollers by closing them first ... final position is %s' % (self, state['current_pos']))
				# now we can set the desired state
				logger.info("Roller %s: Setting rollers to the desired state: %s", self, pos)
				if args.dryrun:
					assert shellyRollerControllerEmulator is not None
					resp = shellyRollerControllerEmulator.setPos(pos)
				else:
					resp = self.getHTTPResp(self.setPosURL() + str(pos))
					state = None
					if resp.status_code != 200:
						raise ShellyRollerControllerException('Roller %s: Unable to set position %s ... received HTTP return code %s' % (self, pos, resp.status_code))
				state = self.waitUntilStopGetState()
				if int(state['current_pos']) != pos:
					raise ShellyRollerControllerException('Roller %s: Unable to set position %s ... final position is %s' % (self, pos, state['current_pos']))
				return state
		except ShellyRollerControllerException as e:
			logger.error(e.message)
			return None

	def requestShutdown(self):
		self.shutdownRequested = True
		self.mainThread.join()
		logger.info("Roller %s: Roller thread stopped", self)

	def rollerMainThread(self):
		while not self.shutdownRequested:
			try:
				request = self.requestQueue.get(block=True, timeout=1)
				assert isinstance(request, ShellyRollerControllerRequest)
				if isinstance(request, ShellyRollerControllerRequestWind):
					if request.action == ShellyRollerControllerRequestWindType.OPEN:
						self.__saveState()
						self.setPos(100)
						self.lastWindTempPos = 100
					elif request.action == ShellyRollerControllerRequestWindType.CLOSE:
						self.__saveState()
						self.setPos(0)
						self.lastWindTempPos = 0
					elif request.action == ShellyRollerControllerRequestWindType.RESTORE:
						self.__restoreState()
					else:
						logger.error('Roller %s: Got unknown wind request: %s', self, request)
				elif isinstance(request, ShellyRollerControllerRequestEvent):
					self.setPos(request.targetPos)
				else:
					logger.error('Roller %s: Got unknown request type: %s is of type %s', self, request, type(request))
			except ShellyRollerControllerException as e:
				logger.error('Roller %s: Failed to process %s: %s', self, request, e.message)
			except Queue.Empty:
				pass

	def waitUntilStopGetState(self):
		while True:
			time.sleep(1)
			state = self.getState()
			if state['is_valid'] == True and not state['calibrating'] and state['state'] == 'stop':
				return state

	def __saveState(self):
		# wait until roller gets into stabilized state
		state = self.waitUntilStopGetState()
		try:
			logger.debug("Roller %s: Acquiring lock to save state", self)
			self.__savedStateLock.acquire()
			logger.debug("Roller %s: Acquired lock to save state", self)
			# do not override already saved state
			if self.__savedState == None:
				logger.debug("Roller %s: Saving state %s", self, state)
				self.__savedState = copy.deepcopy(state)
		finally:
			self.__savedStateLock.release()
			logger.debug("Roller %s: Released lock after saving state", self)

	def overrideSavedStatePos(self, pos):
		assert isinstance(pos, int)
		try:
			logger.debug("Roller %s: Acquiring lock to override state", self)
			self.__savedStateLock.acquire()
			logger.debug("Roller %s: Acquired lock to override state", self)
			if self.__savedState is not None:
				logger.debug("Roller %s: Overriding saved state position from %s to %s", self, self.__savedState['current_pos'], pos)
				self.__savedState['current_pos'] = pos
			else:
				log.warn("Failed to override saved state position - no saved state available.")
		finally:
			self.__savedStateLock.release()
			logger.debug("Roller %s: Released lock after overriding state", self)

	def __restoreState(self):
		try:
			logger.debug("Roller %s: Acquiring lock to restore state", self)
			self.__savedStateLock.acquire()
			logger.debug("Roller %s: Acquired lock to restore state", self)
			if self.__savedState is not None:
				currentState = self.getState()
				assert self.lastWindTempPos is not None
				if int(currentState['current_pos']) == self.lastWindTempPos:
					logger.info("Roller %s: Restoring position to %s", self, int(self.__savedState['current_pos']))
					self.setPos(self.__savedState['current_pos'])
				else:
					logger.info("Roller %s: Not restoring the state as the roller was moved in the meantime - expected %s state, got %s, state to be restored %s", self, self.lastWindTempPos, int(currentState['current_pos']), int(self.__savedState['current_pos']))
				self.__savedState = None
				self.lastWindTempPos = None
			else:
				logger.warn("Roller %s: no saved state to restore", self)
		except ShellyRollerControllerException as e:
			logger.error("Failed to restore position: " + e.message)
		finally:
			self.__savedStateLock.release()
			logger.debug("Roller %s: Released lock after restoring state", self)

	def submitRequest(self, request):
		assert isinstance(request, ShellyRollerControllerRequest), "Roller %s: Request shall be ShellyRollerControllerRequest type, got %s" % (self, type(request))
		try:
			logger.debug("Roller %s: Acquiring lock to submit request", self)
			self.__savedStateLock.acquire()
			logger.debug("Roller %s: Acquired lock to submit request", self)
			if self.__savedState is None:
				logger.debug("Roller %s: submitting request %s", self, request)
				try:
					self.requestQueue.put(request, block=True, timeout=2)
				except Queue.Full:
					logger.warn("Roller %s: request queue for roller is full - not submitting request %s", self, request)
			else:
				if isinstance(request, ShellyRollerControllerRequestEvent):
					logger.debug("Roller %s: modifying restore state to %s", self, request.targetPos)
					self.overrideSavedStatePos(request.targetPos)
                                elif isinstance(request, ShellyRollerControllerRequestWind):
                                        if request.action == ShellyRollerControllerRequestWindType.RESTORE:
                                            logger.debug("Roller %s: submitting state restore request %s", self, request)
                                        else:
                                            logger.debug("Roller %s: submitting state new wind-/sun-related request %s, while __savedState == True", self, request)
                                        try:
                                                self.requestQueue.put(request, block=True, timeout=2)
                                        except Queue.Full:
                                                logger.warn("Roller %s: request queue for roller is full - not submitting request %s", self, request)
		finally:
			self.__savedStateLock.release()
			logger.debug("Roller %s: Released lock to submit request", self)

avgWindThreshold = 40.0
avgGustThreshold = 70.0
windRestoreCoefficiet = 0.7
timeOpenThresholdMinutes = 10
timeRestoreThresholdMinutes = 30
closeAtTemperatureAtAnyAzimuth = 30
closeAtTemperatureAtDirectSunlight = 25
temperatureRestoreCoefficient = 0.9

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
		for k in ('avgWindThreshold', 'avgGustThreshold', 'windRestoreCoefficiet', 'timeOpenThresholdMinutes', 'timeRestoreThresholdMinutes', 'closeAtTemperatureAtAnyAzimuth', 'closeAtTemperatureAtDirectSunlight', 'temperatureRestoreCoefficient'):
			exec(k + " = config['thresholds'].get('" + k + "', " + k + ")")
	if "rollers" in config:
		for roller in config['rollers']:
			rollers.append(ShellyRollerController(roller['name'], str(roller['hostname']), str(roller['rollerUsername']), str(roller['rollerPassword']), roller['solarAzimuthMin'], roller['solarAzimuthMax']))
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


ephem_home = ephem.Observer()
ephem_home.lat, ephem_home.lon, ephem_home.elevation = str(city.latitude), str(city.longitude), int(city.elevation)
ephem_moon = ephem.Moon()

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
			logger.debug("Measurement queue %s: already filled with data", self)
			return True
		else:
			logger.debug("Measurement queue %s: not yet filled with data", self)
			return False

wlatestMonitor = SlidingMonitor()
logger.debug("Measurement queue %s: instantiated as wlatestMonitor", wlatestMonitor)
wgustMonitor = SlidingMonitor()
logger.debug("Measurement queue %s: instantiated as wgustMonitor", wgustMonitor)
tempMonitor = SlidingMonitor()
logger.debug("Measurement queue %s: instantiated as tempMonitor", tempMonitor)

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
datetimeLastMovedWindSun = None

wasClosedDueToTemp = False
wasClosedDueToTempAndSunAzimuth = {}
for roller in rollers:
	wasClosedDueToTempAndSunAzimuth[roller] = False

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

class WeeWxReadoutException(Exception):
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
	scheduleSunJob(requestPositionAllRollers, 'dusk', args=[1], maxTime=datetime.time(hour=21, minute=00))

	if args.testRollers:
		scheduler.add_job(logDebugScheduledJobList, 'interval', seconds=5)
		# this is to test movement of rollers
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
		# cycle invariants - intentionaly outside of try/catch block, so that the program terminates if invariants are violated
		assert sum([wasOpened, wasClosedDueToTemp]) <= 1, "wasOpened and wasClosedDueToTemp are mutually exclusive - wasOpen has higher priority. wasOpened={}, wasClosedDueToTemp={}".format(wasOpened, wasClosedDueToTemp)
		if sum(wasClosedDueToTempAndSunAzimuth.values()) > 0:
			assert sum([wasOpened, wasClosedDueToTemp]) == 0, "When one of the wasClosedDueToTempAndSunAzimuth is set, wasOpened and wasClosedDueToTempAndSunAzimuth must not be set as these are preceding in priority. wasOpened={}, wasClosedDueToTemp={}, sum(wasClosedDueToTempAndSunAzimuth.values())={}".format(wasOpened, wasClosedDueToTemp, sum(wasClosedDueToTempAndSunAzimuth.values()))
		if sum([wasOpened, wasClosedDueToTemp]) > 0:
			assert sum(wasClosedDueToTempAndSunAzimuth.values()) == 0, "When one of the wasOpened and wasClosedDueToTempAndSunAzimuth is set, wasClosedDueToTempAndSunAzimuth must not be set as these are preceding in priority. wasOpened={}, wasClosedDueToTemp={}, sum(wasClosedDueToTempAndSunAzimuth.values())={}".format(wasOpened, wasClosedDueToTemp, sum(wasClosedDueToTempAndSunAzimuth.values()))
		if not (sum([wasOpened, wasClosedDueToTemp]) == 0 and sum(wasClosedDueToTempAndSunAzimuth.values()) == 0):
			assert datetimeLastMovedWindSun is not None, "datetimeLastMovedWindSun must not be None when any of wasOpened, wasClosedDueToTemp] and sum(wasClosedDueToTempAndSunAzimuth.values() are set"
			# datetimeLastMovedWindSun can be not None after restoring the state of rollers (e.g., sun direction condition no longer met) - so inverse assertion for "else" branch does not work

		ephem_home.date = datetime.datetime.utcnow()
		ephem_moon.compute(ephem_home)
		moon_azimuth  = round(degrees(float(ephem_moon.az)),1)
		moon_altitude = round(degrees(float(ephem_moon.alt)),1)
		moon_illum = round(ephem_moon.phase,1)
		logger.debug("Moon info: azimuth={}deg, altitude={}deg, phase={}".format(moon_azimuth, moon_altitude, moon_illum))
		logger.debug("Moon details: az={m.az} alt={m.alt} ra={m.ra}, dec={m.dec} dist={dist:.1f} for location {l.lat:f},{l.lon:f} at {l.date!s}".format(m=ephem_moon,l=ephem_home,dist=ephem_moon.earth_distance*149598000))

		try:
			with open(gaugeFile) as gaugeFile_json:
				def parseWeeWxDate(data):
					try:
						return datetime.datetime.strptime(data['date'], re.sub('([A-Za-z])', '%\\1', data['dateFormat']))
					except Exception as e:
						error_msg = "Failed to parse measurement date: '" + data['date'] + "' with format '" + data['dateFormat'] + "'" + "\n" + str(e)
						logger.error(error_msg)
						raise WeeWxReadoutException(error_msg)

				data = json.load(gaugeFile_json)
				assert data['windunit'] == 'km/h'
				try:
					if lastDate != "" and lastDate == data['date']:
						if parseWeeWxDate(data) <= datetime.datetime.now() - datetime.timedelta(minutes=WEEWX_STALE_READ_TIMEOUT_MINS):
							logger.debug("Gauge is stale for too long, going to restart WeeWx: last update time {}, current time {}, maximum allowed time delta {}".format(parseWeeWxDate(data), datetime.datetime.now(), datetime.timedelta(minutes=WEEWX_STALE_READ_TIMEOUT_MINS)))
							returned_value = os.system(WEEWX_RESTART_CMD)
							if returned_value != 0:
								logger.warn("Failed to restart WeeWx: restart command '{}' returned value {}".format(WEEWX_RESTART_CMD, returned_value))
							else:
								logger.warn("WeeWx restarted due to stale gauge file - last updated on {}".format(data['date']))

						raise WeeWxReadoutException("Gauge hasn't been updated during the last readout period - ignoring")
					else:
						lastDate = data['date']
						lastDateNumeric = parseWeeWxDate(data)
				except WeeWxReadoutException as e:
					raise WeeWxReadoutException("Failed to parse date from WeeWx gauge file: " + str(e))

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
			timeDiffMinutes = None
			if datetimeLastMovedWindSun is not None:
				datetimeNow = datetime.datetime.now()
				timeDiff = datetimeNow - datetimeLastMovedWindSun
				timeDiffMinutes = int(timeDiff.total_seconds() / 60.0)

			if wlatestMonitor.isFull():
                                # XXX: I have added a temporary testing hack to get rollers up in the night
				if wlatestMonitor.getAvg() > avgWindThreshold or wgustMonitor.getAvg() > avgGustThreshold or ( not ( datetime.time(6,00) < datetime.datetime.now().time() < datetime.time(22,00) ) and wlatestMonitor.getAvg() > 5.0 ):
					# this is normal condition to raise the rollers
					logger.debug("Wind is above the set threshold: wlatestMonitor.getAvg()={wl} avgWindThreshold={aWT} wgustMonitor.getAvg()={wg} avgGustThreshold={aGT}".format(wl=wlatestMonitor.getAvg(), aWT=avgWindThreshold, wg=wgustMonitor.getAvg(), aGT=avgGustThreshold))
					if not wasOpened:
						# this is safety so that we don't open the rollers too often
						if timeDiffMinutes is None or timeDiffMinutes >= timeOpenThresholdMinutes:
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
						if timeDiffMinutes >= timeOpenThresholdMinutes:
							for r in rollers:
								if r.getPos() != 100:
									logger.info("Re-rising roller " + r.getNameHostname() + " - something has closed them in the meantime")
									r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.OPEN))
									datetimeLastMovedWindSun = datetime.datetime.now()

				# restoring is only done when the wind/gusts drop substantially - that is windRestoreCoefficiet time the thresholds
                                # XXX: I have added a temporary testing hack to get rollers up in the night
				elif wasOpened and wlatestMonitor.getAvg() < windRestoreCoefficiet*avgWindThreshold and wgustMonitor.getAvg() < windRestoreCoefficiet*avgGustThreshold and ( datetime.time(6,00) < datetime.datetime.now().time() < datetime.time(22,00) ) :
					if timeDiffMinutes is None or timeDiffMinutes >= timeRestoreThresholdMinutes:
						logger.info("Restoring rollers after wind died down")
						wasOpened = False
						for r in rollers:
							r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.RESTORE))
						datetimeLastMovedWindSun = datetime.datetime.now()
					else:
						logger.debug("Conditions met to restore rollers after wind died down, except time threshold not ready yet: timeDiffMinutes={td} timeOpenThresholdMinutes={to}".format(td=timeDiffMinutes, to=timeOpenThresholdMinutes))

			# temperature-based decisions are only implemented if wind-based decisions are not interfering
			if tempMonitor.isFull() and not wasOpened:
				solarElevation = int(city.solar_elevation(datetime.datetime.now()))
				if tempMonitor.getAvg() >= closeAtTemperatureAtAnyAzimuth:
					logger.debug("Conditions met to close rollers due to temperature" +
						" tempMonitor.getAvg()=" + str(tempMonitor.getAvg()) +
						" closeAtTemperatureAtAnyAzimuth=" + str(closeAtTemperatureAtAnyAzimuth) +
						" closeAtTemperatureAtDirectSunlight=" + str(closeAtTemperatureAtDirectSunlight) +
						" wasClosedDueToTemp=" + str(wasClosedDueToTemp) +
						" temperatureRestoreCoefficient*closeAtTemperatureAtDirectSunlight=" + str(temperatureRestoreCoefficient*closeAtTemperatureAtDirectSunlight) +
						" solarElevation=" + str(solarElevation))
					if not wasClosedDueToTemp:
						# this is safety so that we don't open the rollers too often
						logger.debug("Conditions met to close rollers due to temperature, not conflicting with the wind" +
							" tempMonitor.getAvg()=" + str(tempMonitor.getAvg()) +
							" closeAtTemperatureAtAnyAzimuth=" + str(closeAtTemperatureAtAnyAzimuth) +
							" wasOpened=" + str(wasOpened) +
							" wasClosedDueToTemp=" + str(wasClosedDueToTemp))
						if timeDiffMinutes is None or timeDiffMinutes >= timeOpenThresholdMinutes:
							for r in rollers:
								r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.CLOSE))
								wasClosedDueToTempAndSunAzimuth[r] = False
							wasClosedDueToTemp = True
							datetimeLastMovedWindSun = datetime.datetime.now()
						else:
							logger.debug("Conditions met to close rollers for temperature conditions, except time threshold not ready yet: " +
								" timeDiffMinutes=" + str(timeDiffMinutes) +
								" timeOpenThresholdMinutes=" + str(timeOpenThresholdMinutes))

			# directional temperature-based decisions for individual rollers are only implemented if wind-based decisions are not interfering and if the global temperature-based control is not in place
			if tempMonitor.isFull() and not wasOpened and not wasClosedDueToTemp:
				if tempMonitor.getAvg() >= closeAtTemperatureAtDirectSunlight and solarElevation >= 0:
					logger.debug("Conditions met to close rollers due to temperature and sunshine direction")
					solarAzimuth = int(city.solar_azimuth(datetime.datetime.now()))
					# this is safety so that we don't open the rollers too often
					if timeDiffMinutes is None or timeDiffMinutes >= timeOpenThresholdMinutes:
						for r in rollers:
							logger.debug("Conditions match to close roller " + str(r) + " due to direct sunlight:" +
							" tempMonitor.getAvg()=" + str(tempMonitor.getAvg()) +
							" closeAtTemperatureAtDirectSunlight=" + str(closeAtTemperatureAtDirectSunlight) +
							" solarAzimuth=" + str(solarAzimuth) +
							" r.solarAzimuthMin=" + str(r.solarAzimuthMin) +
							" r.solarAzimuthMax=" + str(r.solarAzimuthMax) +
							" wasClosedDueToTempAndSunAzimuth[r]=" + str(wasClosedDueToTempAndSunAzimuth[r]))
							if r.solarAzimuthMin <= solarAzimuth <= r.solarAzimuthMax and not wasClosedDueToTempAndSunAzimuth[r]:
								logger.info("Closing roller " + str(r) + " due to direct sunlight")
								r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.CLOSE))
								wasClosedDueToTempAndSunAzimuth[r] = True
								datetimeLastMovedWindSun = datetime.datetime.now()
							elif not (r.solarAzimuthMin <= solarAzimuth <= r.solarAzimuthMax) and wasClosedDueToTempAndSunAzimuth[r]:
								logger.info("Restoring roller " + str(r) + " no direct sunlight anymore - sun out of the affected angle")
								r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.RESTORE))
								wasClosedDueToTempAndSunAzimuth[r] = False
								datetimeLastMovedWindSun = datetime.datetime.now()

				elif solarElevation < 0 and sum(wasClosedDueToTempAndSunAzimuth.values()) > 0 and (timeDiffMinutes is None or timeDiffMinutes >= timeRestoreThresholdMinutes):
					for r in rollers:
						if wasClosedDueToTempAndSunAzimuth[r]:
							logger.info("Restoring roller " + str(r) + " no direct sunlight anymore - sun below horizon")
							r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.RESTORE))
							wasClosedDueToTempAndSunAzimuth[r] = False
							datetimeLastMovedWindSun = datetime.datetime.now()

			# restore of rollers due to temperature is implemented if wind-based decisions are not interfering and done regardless if done on individual or collective basis
			if tempMonitor.isFull() and not wasOpened and (timeDiffMinutes is None or timeDiffMinutes >= timeRestoreThresholdMinutes):
				if tempMonitor.getAvg() < temperatureRestoreCoefficient*closeAtTemperatureAtDirectSunlight and (wasClosedDueToTemp or sum(wasClosedDueToTempAndSunAzimuth.values()) > 0):
					logger.debug("Conditions match to restore roller " + str(r) + " due to decreased temperature:" +
						" tempMonitor.getAvg()=" + str(tempMonitor.getAvg()) +
						" temperatureRestoreCoefficient*closeAtTemperatureAtDirectSunlight=" + str(temperatureRestoreCoefficient*closeAtTemperatureAtDirectSunlight) +
						" wasClosedDueToTemp=" + str(wasClosedDueToTemp) +
						" sum(wasClosedDueToTempAndSunAzimuth.values())=" + str(sum(wasClosedDueToTempAndSunAzimuth.values())) +
						" timeDiffMinutes=" + str(timeDiffMinutes) +
						" timeRestoreThresholdMinutes=" + str(timeRestoreThresholdMinutes))
					logger.info("Restoring rollers  due to decreased temperature")
					if (wasClosedDueToTemp):
						for r in rollers:
							r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.RESTORE))
					else:
						for r in rollers:
							if wasClosedDueToTempAndSunAzimuth[r]:
								r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.RESTORE))
					datetimeLastMovedWindSun = datetime.datetime.now()
					wasClosedDueToTemp = False
					for r in rollers:
						wasClosedDueToTempAndSunAzimuth[r] = False

				# Reopen non-affected windows if temperature drops, but keep the one directly sunlit shut
				elif  temperatureRestoreCoefficient*closeAtTemperatureAtDirectSunlight <= tempMonitor.getAvg() < tempMonitor.getAvg() < closeAtTemperatureAtAnyAzimuth and wasClosedDueToTemp and solarElevation >= 0:
					logger.debug("Conditions match to restore roller " + str(r) + " due to decreased temperature while sun still shines on some windows:" +
						" tempMonitor.getAvg()=" + str(tempMonitor.getAvg()) +
						" temperatureRestoreCoefficient*closeAtTemperatureAtDirectSunlight=" + str(temperatureRestoreCoefficient*closeAtTemperatureAtDirectSunlight) +
						" wasClosedDueToTemp=" + str(wasClosedDueToTemp) +
						" sum(wasClosedDueToTempAndSunAzimuth.values())=" + str(sum(wasClosedDueToTempAndSunAzimuth.values())) +
						" timeDiffMinutes=" + str(timeDiffMinutes) +
						" timeRestoreThresholdMinutes=" + str(timeRestoreThresholdMinutes) +
						" solarElevation=" + str(solarElevation))
					wasClosedDueToTemp = False
					for r in rollers:
						if r.solarAzimuthMin <= solarAzimuth <= r.solarAzimuthMax:
							log.debug("Roller {} has r.solarAzimuthMin={} r.solarAzimuthMax={}. Solar azimuth is {}".format(r, r.solarAzimuthMin, r.solarAzimuthMax, solarAzimuth))
							log.info("Not restoring roller {} as it is directly sunlit.".format(r))
							wasClosedDueToTempAndSunAzimuth[r] = True
						else:
							r.submitRequest(ShellyRollerControllerRequestWind(ShellyRollerControllerRequestWindType.RESTORE))
							wasClosedDueToTempAndSunAzimuth[r] = False


		except WeeWxReadoutException as e:
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
