#!/usr/bin/env python3

from gi.repository import GLib
import logging
import sys
import os
import random
import configparser
import subprocess
import time
import paho.mqtt.client as mqtt
import threading

logger = logging.getLogger()

for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Now configure the logging for the script
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create a StreamHandler to write logs to the console (stderr by default)
console_handler = logging.StreamHandler(sys.stdout) # You can use sys.stderr as well
console_handler.setFormatter(formatter)

# Add the StreamHandler to the root logger
logger.addHandler(console_handler)

# Set the root logger's level low enough to catch everything
# The log level from the config file will filter what is actually written
logger.setLevel(logging.DEBUG)

# --- BEGIN: CENTRALIZED CONFIG FILE PATH ---
# Set the global path for the config file as requested by the user.
# Changed from '/data/switches.config.ini' to the new path
CONFIG_FILE_PATH = '/data/setupOptions/venus-os_virtual-devices/optionsSet'

try:
    sys.path.insert(1, "/opt/victronenergy/dbus-systemcalc-py/ext/velib_python")
    from vedbus import VeDbusService
except ImportError:
    logger.critical("Cannot find vedbus library. Please ensure it's in the correct path.")
    sys.exit(1)

class DbusMyTestSwitch(VeDbusService):

    def __init__(self, service_name, device_config, output_configs, serial_number, mqtt_config, mqtt_on_payload, mqtt_off_payload):
        super().__init__(service_name, register=False)

        # Store device and output config data for saving changes
        self.device_config = device_config
        self.output_configs = output_configs
        self.device_index = device_config.getint('DeviceIndex')

        # --- BEGIN: NEW MQTT PAYLOADS ---
        self.mqtt_on_payload = mqtt_on_payload
        self.mqtt_off_payload = mqtt_off_payload
        # --- END: NEW MQTT PAYLOADS ---

        # General device settings
        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.16')
        self.add_path('/Mgmt/Connection', 'Virtual')
        
        # Get values from the device-specific config section
        self.add_path('/DeviceInstance', self.device_config.getint('DeviceInstance'))
        self.add_path('/ProductId', 49257)
        # ProductName is a fixed value and is not writable
        self.add_path('/ProductName', 'Virtual switch')
        # Make CustomName writeable and link to the config value
        self.add_path('/CustomName', self.device_config.get('CustomName'), writeable=True, onchangecallback=self.handle_dbus_change)
        
        # Serial number is now read from the config file
        self.add_path('/Serial', serial_number)
        
        self.add_path('/State', 256)
        
        self.add_path('/FirmwareVersion', 0)
        self.add_path('/HardwareVersion', 0)
        self.add_path('/Connected', 1)

        # MQTT specific members
        self.mqtt_client = None
        self.mqtt_config = mqtt_config
        self.dbus_path_to_state_topic_map = {}
        self.dbus_path_to_command_topic_map = {}
        
        # Loop through the outputs and add their D-Bus paths
        for output_data in output_configs:
            self.add_output(output_data)

        # Initialize and connect the MQTT client
        self.setup_mqtt_client()
        
        # Register the service on the D-Bus AFTER all paths have been added
        self.register()
        # This is the ONLY message that remains at INFO level
        logger.info(f"Service '{service_name}' for device '{self.device_config.get('CustomName')}' registered on D-Bus.")

    def add_output(self, output_data):
        """
        Adds a single switchable output and its settings to the D-Bus service,
        and stores MQTT topic mappings.
        """
        output_prefix = f'/SwitchableOutput/output_{output_data["index"]}'
        
        # Store topic mappings for later use
        state_topic = output_data.get('MqttStateTopic')
        command_topic = output_data.get('MqttCommandTopic')
        dbus_state_path = f'{output_prefix}/State'

        if state_topic and command_topic:
            self.dbus_path_to_state_topic_map[dbus_state_path] = state_topic
            self.dbus_path_to_command_topic_map[dbus_state_path] = command_topic

        self.add_path(f'{output_prefix}/Name', output_data['name'])
        self.add_path(f'{output_prefix}/Status', 0)

        # Add the State path, which will be writable.
        self.add_path(dbus_state_path, 0, writeable=True, onchangecallback=self.handle_dbus_change)

        settings_prefix = f'{output_prefix}/Settings'
        self.add_path(f'{settings_prefix}/CustomName', output_data['custom_name'], writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path(f'{settings_prefix}/Group', output_data['group'], writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path(f'{settings_prefix}/Type', 1, writeable=True)
        self.add_path(f'{settings_prefix}/ValidTypes', 7)

    def setup_mqtt_client(self, retry_interval=5, max_retries=12):
        """
        Initializes and starts the MQTT client with retry logic.
        """
        # FIX: Change to use the new Callback API version 2
        self.mqtt_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self['/Serial']
        )
        
        if self.mqtt_config.get('Username'):
            self.mqtt_client.username_pw_set(
                self.mqtt_config.get('Username'),
                self.mqtt_config.get('Password')
            )
            
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message
        self.mqtt_client.on_publish = self.on_mqtt_publish
        
        retries = 0
        while retries < max_retries:
            try:
                logger.debug(f"Attempting to connect to MQTT broker ({retries + 1}/{max_retries})...")
                self.mqtt_client.connect(
                    self.mqtt_config.get('BrokerAddress'),
                    self.mqtt_config.getint('Port', 1883),
                    60
                )
                # Start the MQTT network loop in a separate thread
                self.mqtt_client.loop_start()
                logger.debug("MQTT client started.")
                return # Exit on successful connection
            except Exception as e:
                logger.error(f"Failed to connect to MQTT broker: {e}. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
                retries += 1
        
        logger.critical(f"Failed to connect to MQTT broker after {max_retries} attempts. Exiting.")
        sys.exit(1)


    def on_mqtt_connect(self, client, userdata, flags, rc, properties):
        """
        MQTT callback for when the client connects to the broker.
        Subscribes only to state topics.
        """
        if rc == 0:
            logger.debug("Connected to MQTT Broker!")
            # Subscribe ONLY to state topics
            state_topics = list(self.dbus_path_to_state_topic_map.values())
            for topic in state_topics:
                client.subscribe(topic)
                logger.debug(f"Subscribed to MQTT state topic: {topic}")
        else:
            logger.error(f"Failed to connect to MQTT broker, return code {rc}")
            # If connection fails, attempt to reconnect or exit
            # For now, just log, loop_start will handle automatic reconnection if possible
            # based on mosquitto's internal retry mechanism, but we added a more robust retry
            # at initial connection in setup_mqtt_client.

    def on_mqtt_message(self, client, userdata, msg):
        """
        MQTT callback for when a message is received on a subscribed topic.
        Only accepts payloads as defined in the config file.
        """
        try:
            payload = msg.payload.decode().strip().lower()
            topic = msg.topic
            logger.debug(f"Received MQTT message on topic '{topic}': {payload}")

            new_state = None
            if payload == self.mqtt_on_payload.lower():
                new_state = 1
            elif payload == self.mqtt_off_payload.lower():
                new_state = 0
            else:
                logger.warning(f"Invalid MQTT payload received for topic '{topic}': '{payload}'. Expected '{self.mqtt_on_payload}' or '{self.mqtt_off_payload}'.")
                return
            
            # Find the corresponding D-Bus path for this topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            
            if dbus_path:
                # Check if the state is already the same to prevent redundant D-Bus signals.
                if self[dbus_path] == new_state:
                    logger.debug(f"D-Bus state is already {new_state}, ignoring redundant MQTT message.")
                    return
                
                # Use GLib.idle_add to schedule the D-Bus update in the main thread
                # This will trigger a PropertiesChanged signal on D-Bus.
                GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, new_state)
            
        except (ValueError, KeyError) as e:
            logger.error(f"Error processing MQTT message: {e}")
    
    def on_mqtt_publish(self, client, userdata, mid, reason_code, properties):
        """
        MQTT callback for when a publish request has been sent.
        """
        logger.debug(f"Publish message with mid: {mid} acknowledged by client.")

    def handle_dbus_change(self, path, value):
        """
        Callback function to handle changes to D-Bus paths.
        This is triggered when a D-Bus client requests a change.
        """
        # If the change is to a state path, publish to MQTT
        if "/State" in path:
            logger.debug(f"D-Bus change handler triggered for {path} with value {value}")
            if value not in [0, 1]:
                logger.warning(f"Invalid D-Bus state value received: {value}. Expected 0 or 1.")
                return False
            self.publish_mqtt_command(path, value)
            return True
        
        # If the change is to the top-level device's CustomName, save it to the config file
        elif path == '/CustomName':
            key_name = 'CustomName'
            section_name = f'Device_{self.device_index}'
            logger.debug(f"D-Bus settings change triggered for {path} with value '{value}'. Saving to config file.")
            self.save_config_change(section_name, key_name, value)
            return True

        # If the change is to a nested settings path, save it to the config file
        elif "/Settings" in path:
            try:
                parts = path.split('/')
                # Corrected indices below
                output_index = parts[2].replace('output_', '')
                setting_key = parts[4]
                section_name = f'Output_{self.device_index}_{output_index}'
                logger.debug(f"D-Bus settings change triggered for {path} with value '{value}'. Saving to config file.")
                self.save_config_change(section_name, setting_key, value)
                return True
            except IndexError:
                logger.error(f"Could not parse D-Bus path for config save: {path}")
                return False

        logger.warning(f"Unhandled D-Bus change request for path: {path}")
        return False

    def save_config_change(self, section, key, value):
        """
        Saves a changed D-Bus setting to the corresponding config file.
        """
        config = configparser.ConfigParser()
        
        try:
            config.read(CONFIG_FILE_PATH)
            
            if not config.has_section(section):
                logger.warning(f"Creating new section '{section}' in config file.")
                config.add_section(section)

            # Update the value and write to file
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as configfile:
                config.write(configfile)
                
            logger.debug(f"Successfully saved setting '{key}' to section '{section}' in config file.")

        except Exception as e:
            logger.error(f"Failed to save config file changes for key '{key}' in section '{section}': {e}")
            
    def publish_mqtt_command(self, path, value):
        """
        Centralized and robust method to publish a command to MQTT.
        """
        if not self.mqtt_client or not self.mqtt_client.is_connected():
            logger.warning("MQTT client is not connected. Cannot publish.")
            return

        if path not in self.dbus_path_to_command_topic_map:
            logger.warning(f"No command topic mapped for D-Bus path: {path}")
            return

        try:
            command_topic = self.dbus_path_to_command_topic_map[path]

            # --- BEGIN: UPDATED PAYLOAD LOGIC ---
            mqtt_payload = self.mqtt_on_payload if value == 1 else self.mqtt_off_payload
            # --- END: UPDATED PAYLOAD LOGIC ---

            # Note: Commands are typically not retained
            (rc, mid) = self.mqtt_client.publish(command_topic, mqtt_payload, retain=False)
            
            if rc == mqtt.MQTT_ERR_SUCCESS:
                logger.debug(f"Publish request for '{path}' sent to command topic '{command_topic}'. mid: {mid}")
            else:
                logger.error(f"Failed to publish to '{command_topic}', return code: {rc}")
        except Exception as e:
            logger.error(f"Error during MQTT publish: {e}")
            
    def update_dbus_from_mqtt(self, path, value):
        """
        A centralized method to handle MQTT-initiated state changes to D-Bus.
        """
        self[path] = value
        logger.debug(f"Successfully changed '{path}' to {value} from source: mqtt")
        
        return False # Return False for GLib.idle_add to run only once

def run_device_service(device_index):
    """
    Main function for a single D-Bus service process.
    """
    from dbus.mainloop.glib import DBusGMainLoop
    DBusGMainLoop(set_as_default=True)
    
    # Log the start of this specific device process
    logger.info(f"Starting D-Bus service process for device {device_index}.")

    config = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE_PATH):
        logger.critical(f"Configuration file not found: {CONFIG_FILE_PATH}")
        sys.exit(1)
    
    try:
        config.read(CONFIG_FILE_PATH)
    except configparser.Error as e:
        logger.critical(f"Error parsing configuration file: {e}")
        sys.exit(1)
        
    # Mapping for log levels
    LOG_LEVELS = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    # Get log level from config, default to INFO if not found or invalid
    if config.has_section('Global'):
        log_level_str = config['Global'].get('LogLevel', 'INFO').upper()
        log_level = LOG_LEVELS.get(log_level_str, logging.INFO)
    else:
        log_level = logging.INFO
        
    logger.setLevel(log_level)
    logger.debug(f"Log level set to: {logging.getLevelName(logger.level)}")

    device_section = f'Device_{device_index}'
    if not config.has_section(device_section):
        logger.critical(f"Configuration section '{device_section}' not found. Cannot start.")
        sys.exit(1)
        
    device_config = config[device_section]
    
    # --- BEGIN: NEW CONFIG READING ---
    mqtt_on_payload = device_config.get('MqttOnPayload', 'ON')
    mqtt_off_payload = device_config.get('MqttOffPayload', 'OFF')
    # --- END: NEW CONFIG READING ---

    # Store the device index in the device config for later use
    device_config['DeviceIndex'] = str(device_index)
    
    # --- BEGIN: MODIFIED SERIAL NUMBER HANDLING ---
    # Now, the serial number MUST be present in the config file.
    # If not found or empty, log a critical error and exit.
    serial_number = device_config.get('Serial')
    if not serial_number or serial_number.strip() == '':
        logger.critical(f"Serial number not found or is empty for device {device_index} in config. Exiting. Please run the setup script to generate a serial.")
        sys.exit(1)
    else:
        logger.debug(f"Using existing serial number '{serial_number}' for device {device_index}.")
    # --- END: MODIFIED SERIAL NUMBER HANDLING ---

    try:
        num_switches = device_config.getint('NumberOfSwitches')
    except (configparser.NoOptionError, ValueError):
        logger.warning("No 'NumberOfSwitches' found in [Global] section. Defaulting to 1 switch.")
        num_switches = 1

    output_configs = []
    for j in range(1, num_switches + 1):
        output_section = f'Output_{device_index}_{j}'
        
        output_data = {
            'index': j,
            'name': f'Switch {j}',
            'custom_name': '',
            'group': '',
            'MqttStateTopic': None,
            'MqttCommandTopic': None,
        }

        if config.has_section(output_section):
            output_settings = config[output_section]
            output_data['custom_name'] = output_settings.get('CustomName', '')
            output_data['group'] = output_settings.get('Group', '')
            output_data['MqttStateTopic'] = output_settings.get('MqttStateTopic', None)
            output_data['MqttCommandTopic'] = output_settings.get('MqttCommandTopic', None)
        
        output_configs.append(output_data)

    service_name = f'com.victronenergy.switch.virtual_{serial_number}'

    # Get MQTT configuration
    mqtt_config = config['MQTT'] if config.has_section('MQTT') else {}

    # Pass the MQTT config to the DbusMyTestSwitch class
    # --- BEGIN: UPDATED CONSTRUCTOR CALL ---
    DbusMyTestSwitch(service_name, device_config, output_configs, serial_number, mqtt_config, mqtt_on_payload, mqtt_off_payload)
    # --- END: UPDATED CONSTRUCTOR CALL ---
    
    logger.debug('Connected to D-Bus, and switching over to GLib.MainLoop() (= event based)')
    
    mainloop = GLib.MainLoop()
    mainloop.run()

def main():
    """
    The main launcher function that runs as the parent process.
    """
    # Log the start of the overall launcher script
    logger.info("Starting D-Bus Virtual Switch service launcher.")

    config = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE_PATH):
        logger.critical(f"Configuration file not found: {CONFIG_FILE_PATH}")
        sys.exit(1)
    
    try:
        config.read(CONFIG_FILE_PATH)
    except configparser.Error as e:
        logger.critical(f"Error parsing configuration file: {e}")
        sys.exit(1)
    
    # Mapping for log levels
    LOG_LEVELS = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    # Get log level from config, default to INFO if not found or invalid
    if config.has_section('Global'):
        log_level_str = config['Global'].get('LogLevel', 'INFO').upper()
        log_level = LOG_LEVELS.get(log_level_str, logging.INFO)
    else:
        log_level = logging.INFO
    
    logger.setLevel(log_level)
    logger.debug(f"Log level set to: {logging.getLevelName(logger.level)}")

    try:
        num_devices = config.getint('Global', 'numberofmodules')
    except (configparser.NoSectionError, configparser.NoOptionError):
        logger.warning("No 'numberofmodules' found in [Global] section. Defaulting to 1 device.")
        num_devices = 1

    script_path = os.path.abspath(sys.argv[0])
    processes = []
    
    logger.debug(f"Starting {num_devices} virtual switch device processes...")

    for i in range(1, num_devices + 1):
        device_section = f'Device_{i}'
        
        if not config.has_section(device_section):
            logger.warning(f"Configuration section '{device_section}' not found. Skipping device {i}.")
            continue
            
        cmd = [sys.executable, script_path, str(i)]
        
        try:
            process = subprocess.Popen(cmd, env=os.environ, close_fds=True)
            processes.append(process)
            logger.debug(f"Started process for virtual device {i} (PID: {process.pid})")
        except Exception as e:
            logger.error(f"Failed to start process for device {i}: {e}")
            
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.debug("Terminating all child processes.")
        for p in processes:
            p.terminate()
        for p in processes:
            p.wait()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_device_service(sys.argv[1])
    else:
        main()
