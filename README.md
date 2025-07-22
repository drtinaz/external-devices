For victron venus os devices running firmware version 3.60 or later.
This installs a service that will create virtual devices for venus os that can be displayed on gui v2. (Relay modules, virtual batteries, temperature sensors, and tank sensors) This has been tested on Cerbo GX but most likely can also be used on Ekrano or raspi. Node red is not required, allowing this service to be installed on regular os versions. MQTT is the protocol used for status and control.

************ INSTALL USING KEVINS SETUP HELPER **************
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

All custom names and group names can be changed from the
gui and will automatically update the config file to persist reboots and system upgrades. If other changes are needed, re-run the config script, or edit the config file with a text editor. The config file is located at /data/setupOptions/venus-os_virtual-devices/optionsSet
