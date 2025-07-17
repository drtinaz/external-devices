This service adds virtual switches to the gx device GUI v2 switch panel (this service was tested on a cerbo GX, but quite possibly will work on the ekrano and venus running on rpi as it does not interact with the gpio's. This service creates dbus services which natively interact with venus os)
that can be used to control external relay modules via mqtt. 
This can be manually installed as a service, or installed via Kevin Windrems
setup helper. The simplest method is setup helper. 

*************************************************
*********** MANUAL INSTALLATION *************
1. Download this package to the /data directory
3. in SSH terminal enter the following:

```
cp /data/MQTT-switches/config.ini.example /data/switches.config.ini
```

5. Complete initial configuration by editing the config file:
```
nano /data/switches.config.ini
```
6. Install and start the service mqtt_switches.py

UPDATES: Package updates and venus os firmware updates will require you to repeat the install process, less the configuration.

*******************************************************************
************ INSTALL USING KEVINS SETUP HELPER **************
1. Install Kevins setup helper ( https://github.com/kwindrem/SetupHelper )
3. From the package manager menu click on inactive packages and click 'new' at the top
4. enter the following:
   package name : MQTT-switches
   github user : drtinaz
   github branch or tag : main
5. click proceed
6. now go to 'active packages' and click on MQTT-switches and download
7. open a SSH terminal and enter the following:
```
cp /data/MQTT-switches/config.ini.example /data/switches.config.ini
```
8. Complete initial configuration by editing the config file:
```
nano /data/switches.config.ini
```
9. Finish installing the service by entering the following:
```
/data/MQTT-switches/setup install
```

UPDATES: Installing this package via setup helper ensures that package updates will be automatic,
and venus os firmware updates will result in automatic re-installation of the package.

*********************************************************************************************************
******** ABOUT THE CONFIG FILE 

switches.config.ini will survive package updates, this file only needs to be modified at initial install, or if you
need to change the device configuration.
Any changes made from the gui (such as switch names, device names or group names) will automatically be saved to the configuration
file and these changes will survive a reboot or venus os firmware upgrade.
