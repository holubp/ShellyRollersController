# ShellyRollersController

This is a simple tool to control the rollers based on current wind conditions, with the intention to allow very cheap automated control system. It uses Shelly 2.5 roller controller and WeeWx-based weather station (currently I am using WH1080). It is not meant at the moment as a generic solution but as a quick tool I needed for my own home and that somebody may use as an inspiration to write something more elaborate.

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
    * pip install Enum
  * requests
    * pip install requests
  * statistics
    * pip install statistics

## My current configuration

* Rollers controlled by Shelly 2.5 (https://shelly.cloud/shelly-25-wifi-smart-relay-roller-shutter-home-automation/, configured in roller mode)
* WH1080 weather station (https://www.hadex.cz/t104-meteostanice-wh1080/)
* Small Raspberry Pi 3B+ with Volumio installed (used primarily as audio source ;) )
