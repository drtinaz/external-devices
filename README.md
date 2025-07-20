*****************************************************************************************
******** CURRENTLY UNDER DEVELOPEMENT - NOT FUNCTIONAL ********************
****************************************************************************************

This installs a service that will provide virtual switches for venus os that can be displayed on gui v2. This has been tested on Cerbo GX but most likely can also be used on Ekrano or raspi. Node red is not required, allowing this service to be installed on regular os versions.

************ INSTALL USING KEVINS SETUP HELPER **************
1. Install Kevins setup helper ( https://github.com/kwindrem/SetupHelper )
3. From the package manager menu click on inactive packages and click 'new' at the top
4. enter the following:
   package name : MQTT-switches
   github user : drtinaz
   github branch or tag : main
5. click proceed
6. now go to 'active packages' and click on MQTT-switches and download
7. open a SSH terminal and run the configuration script:
```
/data/MQTT-switches/config.py
```
8. When the configuration is complete, select option 1 to install and activate the service.

Device custom names, switch custom names, and group assignments can all be changed from the
gui and will populate to the config file to persist reboots and system upgrades. If you need to
make other changes such as number of devices or device specific settings, re-run the config script
in terminal and select option 2 from the menu.


********************************************************************************************
*************                     BREAKING CHANGES                       *******************

UPDATING TO V1.0 FROM PREVIOUS VERSIONS WILL BREAK THE SERVICE. THE CONFIG FILE LOCATION HAS
BEEN MOVED. YOU CAN COPY THE CONTENTS OF THE CONFIG TO THE NEW LOCATION BY DOING THE FOLLOWING:

1. open SSH terminal and enter the following:
```
cp /data/switches.config.ini /data/setupOptions/MQTT-switches/optionsSet
```
2. reboot for the changes to take effect:
```
reboot
```

