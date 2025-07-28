For victron venus os devices running firmware version 3.60 or later.

*******************************************************************
This installs a service that will create virtual (external) devices for venus os that can be displayed on gui v2. (Relay modules, digital inputs, virtual batteries, temperature sensors, and tank sensors) This has been tested on Cerbo GX but most likely can also be used on Ekrano or raspi. Node red is not required, allowing this service to be installed on regular os versions. MQTT is the protocol used for status and control.

*******************************************************************
Installation is simple with Kevin Windrems' setup helper
1. Install Kevins setup helper ( https://github.com/kwindrem/SetupHelper )
3. From the package manager menu click on inactive packages and click 'new' at the top
4. enter the following:
   package name : external-devices
   github user : drtinaz
   github branch or tag : main
5. click proceed
6. now go to 'active packages' and click on external-devices and verify download has occurred
7. open a SSH terminal and run the configuration script:
```
/data/external-devices/config.py
```
9. Select option 1 to configure global settings.
10. Select option 2 to add devices.
11. Once all desired devices have been added, select option 6 to return to the main configuration
    menu, then select option 5 to exit the configuration menu, then select option 1 to install
    and activate the service.
