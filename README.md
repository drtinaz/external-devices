For victron venus os devices running firmware version 3.60 or later.

*******************************************************************
This installs a service that will create virtual devices for venus os that can be displayed on gui v2. (Relay modules, virtual batteries, temperature sensors, and tank sensors) This has been tested on Cerbo GX but most likely can also be used on Ekrano or raspi. Node red is not required, allowing this service to be installed on regular os versions. MQTT is the protocol used for status and control.

*******************************************************************
Installation is simple with Kevin Windrems' setup helper
1. Install Kevins setup helper ( https://github.com/kwindrem/SetupHelper )
3. From the package manager menu click on inactive packages and click 'new' at the top
4. enter the following:
   package name : venus-os_virtual-devices
   github user : drtinaz
   github branch or tag : main
5. click proceed
6. now go to 'active packages' and click on venus-os_virtual-devices and verify download has occurred
7. open a SSH terminal and run the configuration script:
```
/data/venus-os_virtual-devices/config.py
```
8. When the configuration is complete, select option 1 to install and activate the service.

********************************************************************************
All custom names and group names can be changed from the
gui and will automatically update the config file to persist reboots and system upgrades. If other changes are needed, re-run the config script, or edit the config file with a text editor. The config file is located at /data/setupOptions/venus-os_virtual-devices/optionsSet

**************************************************************************
***********              CONFIGURATION SETTINGS               ************

LOG LEVEL: this is usually set to INFO. For troubleshooting, set to DEBUG.

NUMBER OF RELAY MODULES: This is the number of modules that you wish to configure, not the total number of switches. The number of switches comes later.

NUMBER OF TEMPERATURE SENSORS: number of temp sensors to be configure.

NUMBER OF TANK SENSORS: number of tank sensors to be configured.

NUMBER OF VIRTUAL BATTERIES: number of batteries to be configured.

DEVICE INSTANCE: leave all device instances the default value unless you are certain there is a conflict. Press enter to continue.

CUSTOM NAME FOR EACH RELAY MODULE: makes it easier to keep track of each module and the switches assigned to each module. Default values will increment for each device.

NUMBER OF SWITCHES (FOR THE CURRENT MODULE): the number of switches being configured for the current module.

MQTT ON state payload for Relay: expected payload value recieved by the susbscribed topic for "on". (topics are defined further down)

MQTT OFF state payload for Relay: expected payload value recieved by the susbscribed topic for "off".

MQTT ON command payload for Relay: payload value to be sent on the command topic to turn the relay "on"

MQTT OFF command payload for Relay: payload value to be sent on the command topic to turn the relay "off"

serial for Relay Module: XXXXXXXXXXXXXXXX : a serial number is automatically assigned for each device. Enter to continue.

custom name for Relay Module x, switch x : enter a custom name for each switch

group for Relay Module x, switch x : enter the group name for each switch. this is the switch pane name that will appear on the gui.

MQTT state topic for Relay Module x, switch x : this is the path to the mqtt topic with the state of the relay

MQTT command topic for Relay Module x, switch x : this is the path to the mqtt topic that will issue the command to set the state of the relay (switch the relay on or off)

By this point in the configuration you should be able to determine what the rest of the settings do by the prompt that is given.
