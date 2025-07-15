This service adds virtual switches to the gx device GUI v2 switch panel (cerbo and possibly ekrano)
that can be used to control external relay modules via mqtt. 
This can be manually installed as a service, or installed via Kevin Windrems
setup helper. The simplest method is setup helper. 

*********** MANUAL INSTALLATION *************
1. Download this package to the /data directory
2. open a ssh terminal session and navigate to /data/MQTT-switches
3. in the terminal enter the following: cp config.ini.example /data/switches.config.ini
4. navigate to /data and open switches.config.ini using vi or nano and enter the correct details for your configuration.
5. Save and close switches.config.ini
6. Install and start the service mqtt_switches.py


************ INSTALL USING kEVINS SETUP HELPER **************
1. Install Kevins setup helper ( https://github.com/kwindrem/SetupHelper )
2. Goto settings/package manager and turn off 'auto install'
3. From the package manager menu click on inactive packages and click 'new' at the top
4. enter the following:
5. package name : MQTT-switches
6. github user : drtinaz
7. github branch or tag : main
8. click proceed
9. now go to 'active packages' and click on MQTT-switches and download
10. complete steps 2 thru 5 of the manual installation
11. you can now re-enable 'auto install' in setup helper menu or click 'install' in the active packages

************ PACKAGE UPDATES **********************
If you chose the manual installation method, you will need to manually download and install any future updates.
If you chose to use setup helper, any future updates will be automatic as long as 'auto install packages' is selected.
switches.config.ini will survive package updates, this file only needs to be modified at initial install.
any changes made from the gui (such as switch names, device names or group names) will automatically be saved to the configuration
file and these changes will survive a reboot or venus os firmware upgrade.
