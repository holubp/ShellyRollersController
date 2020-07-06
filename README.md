# ShellyRollersController

This is a simple tool to control the rollers based on current wind conditions, with the intention to allow very cheap automated control system. It uses Shelly 2.5 roller controller and WeeWx-based weather station (currently I am using WH1080). It is not meant at the moment as a generic solution but as a quick tool I needed for my own home and that somebody may use as an inspiration to write something more elaborate.

While more complex systematic solutions exists (such as home-assistant.io), this aims to be as lightweight as possible, running even on limited Volumio Raspberry Pi with old Python 2.7.

## Prerequisites

* WeeWx
* Rtgd extension for WeeWx:
  * wget https://github.com/gjr80/weewx-realtime_gauge-data/releases/download/v0.4.2/rtgd-0.4.2.tar.gz
  * wee_extension --install=rtgd-0.4.2.tar.gz
* Python 2.x with the following modules
  * python-daemon
    * pip install python-daemon
  * DateTime
    * pip install DateTime
  * Enum
    * pip install enum34
  * requests
    * pip install requests
  * statistics
    * pip install statistics
  * astral
    * pip install astral
    * pip install astral==1.10.1 (for Python 2.7)
  * APscheduler
    * pip install APscheduler
    * pip install APScheduler==3.2.0 (for older Python 2.7 versions)
  * ephem
    * pip install ephem

## My current configuration

* Rollers controlled by Shelly 2.5 (https://shelly.cloud/shelly-25-wifi-smart-relay-roller-shutter-home-automation/, configured in roller mode)
* WH1080 weather station (https://www.hadex.cz/t104-meteostanice-wh1080/)
* Small Raspberry Pi 3B+ with Volumio installed (used primarily as audio source ;) )
