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
import json # Import the json module

logger = logging.getLogger()

for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Now configure the logging for the script
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Reverting console handler changes
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

def get_json_attribute(data, path):
    """
    Recursively gets an attribute from a nested dictionary using a dot-separated path.
    Returns None if the path does not exist.
    """
    parts = path.split('.')
    current = data
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current

class DbusSwitch(VeDbusService):

    # MODIFIED: Changed __init__ signature to accept four distinct payload types
    def __init__(self, service_name, device_config, output_configs, serial_number, mqtt_config,
                 mqtt_on_state_payload, mqtt_off_state_payload, mqtt_on_command_payload, mqtt_off_command_payload):
        super().__init__(service_name, register=False)

        # Store device and output config data for saving changes
        self.device_config = device_config
        self.device_index = device_config.getint('DeviceIndex')

        # --- BEGIN: NEW MQTT PAYLOADS (MODIFIED) ---
        self.mqtt_on_state_payload_raw = mqtt_on_state_payload # Store raw for comparison
        self.mqtt_off_state_payload_raw = mqtt_off_state_payload # Store raw for comparison
        self.mqtt_on_command_payload = mqtt_on_command_payload
        self.mqtt_off_command_payload = mqtt_off_command_payload
        
        self.mqtt_on_state_payload_json = None
        self.mqtt_off_state_payload_json = None

        logger.debug(f"DEBUG: Raw mqtt_on_state_payload from config: '{mqtt_on_state_payload}' (type: {type(mqtt_on_state_payload)})")
        logger.debug(f"DEBUG: Parsed mqtt_on_state_payload_json before if: {self.mqtt_on_state_payload_json}")

        # Attempt to parse ON and OFF state payloads as JSON
        try:
            parsed_on = json.loads(mqtt_on_state_payload)
            if isinstance(parsed_on, dict) and len(parsed_on) == 1:
                self.mqtt_on_state_payload_json = parsed_on
                logger.debug(f"Parsed MqttOnStatePayload as JSON: {self.mqtt_on_state_payload_json}")
        except json.JSONDecodeError:
            pass # Not a JSON, keep as string comparison

        try:
            parsed_off = json.loads(mqtt_off_state_payload)
            if isinstance(parsed_off, dict) and len(parsed_off) == 1:
                self.mqtt_off_state_payload_json = parsed_off
                logger.debug(f"Parsed MqttOffStatePayload as JSON: {self.mqtt_off_state_payload_json}")
        except json.JSONDecodeError:
            pass # Not a JSON, keep as string comparison
        # --- END: NEW MQTT PAYLOADS (MODIFIED) ---

        # General device settings
        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.18') # Updated version
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

        # Validate topics: ensure they are not None, empty, or placeholder strings
        if state_topic and state_topic != '' and 'path/to/mqtt' not in state_topic \
           and command_topic and command_topic != '' and 'path/to/mqtt' not in command_topic:
            self.dbus_path_to_state_topic_map[dbus_state_path] = state_topic
            self.dbus_path_to_command_topic_map[dbus_state_path] = command_topic
        else:
            logger.warning(f"MQTT topics for {dbus_state_path} are invalid or not set. State Topic: '{state_topic}', Command Topic: '{command_topic}'. Ignoring.")


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
        Handles JSON payloads with a 'value' key, or raw string payloads.
        Specifically for switch type.
        MODIFIED: Handles complex JSON payloads based on mqtt_on/off_state_payloads.
        """
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            logger.debug(f"Received MQTT message on topic '{topic}': {payload_str}")

            new_state = None
            processed_payload_value = None # This will hold the extracted value from JSON or the raw string

            try:
                # Attempt to parse incoming payload as JSON first
                incoming_json = json.loads(payload_str)
                logger.debug(f"Incoming MQTT payload is JSON: {incoming_json}")

                if self.mqtt_on_state_payload_json:
                    # If ON payload is JSON, extract attribute and value
                    on_attr, on_val = list(self.mqtt_on_state_payload_json.items())[0]
                    extracted_on_value = get_json_attribute(incoming_json, on_attr)
                    logger.debug(f"Comparing JSON: looking for '{on_attr}' with value '{on_val}'. Found '{extracted_on_value}'.")
                    if extracted_on_value is not None and str(extracted_on_value).lower() == str(on_val).lower():
                        new_state = 1
                        processed_payload_value = extracted_on_value
                
                if new_state is None and self.mqtt_off_state_payload_json:
                    # If OFF payload is JSON, extract attribute and value
                    off_attr, off_val = list(self.mqtt_off_state_payload_json.items())[0]
                    extracted_off_value = get_json_attribute(incoming_json, off_attr)
                    logger.debug(f"Comparing JSON: looking for '{off_attr}' with value '{off_val}'. Found '{extracted_off_value}'.")
                    if extracted_off_value is not None and str(extracted_off_value).lower() == str(off_val).lower():
                        new_state = 0
                        processed_payload_value = extracted_off_value

                if new_state is None: # If JSON matching didn't yield a state, fall back to "value" key or raw
                    if isinstance(incoming_json, dict) and "value" in incoming_json:
                        processed_payload_value = str(incoming_json["value"]).lower()
                    else:
                        logger.warning(f"JSON payload for topic '{topic}' did not match configured JSON payloads or 'value' key: {payload_str}. Falling back to raw payload string comparison.")
                        processed_payload_value = payload_str.lower()

            except json.JSONDecodeError:
                # If not JSON, treat the original payload as a raw string
                logger.debug(f"Incoming MQTT payload is not JSON. Falling back to raw string comparison: {payload_str}")
                processed_payload_value = payload_str.lower()
            
            # Now, determine new_state based on processed_payload_value (either from JSON or raw string)
            if new_state is None: # Only if JSON parsing/matching didn't set it
                if processed_payload_value == self.mqtt_on_state_payload_raw.lower():
                    new_state = 1
                elif processed_payload_value == self.mqtt_off_state_payload_raw.lower():
                    new_state = 0
                else:
                    # MODIFIED: Updated log message to reflect the new payload variables
                    logger.warning(f"Invalid MQTT payload received for topic '{topic}': '{payload_str}'. Expected '{self.mqtt_on_state_payload_raw}' or '{self.mqtt_off_state_payload_raw}' or a matching JSON structure.")
                    return
            
            # Find the corresponding D-Bus path for this topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            logger.debug(f"Mapping topic '{topic}' to D-Bus path: {dbus_path}")
            
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
            # Updated section name for device configuration
            section_name = f'Relay_Module_{self.device_index}' 
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
                # Updated section name for output configuration
                section_name = f'switch_{self.device_index}_{output_index}'
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

            # --- BEGIN: UPDATED PAYLOAD LOGIC (MODIFIED) ---
            # Use command payloads for outgoing messages
            mqtt_payload = self.mqtt_on_command_payload if value == 1 else self.mqtt_off_command_payload
            # --- END: UPDATED PAYLOAD LOGIC (MODIFIED) ---

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

class DbusTempSensor(VeDbusService):
    TEMPERATURE_TYPES = {
        'battery': 0,
        'fridge': 1,
        'generic': 2,
        'room': 3,
        'outdoor': 4,
        'water heater': 5,
        'freezer': 6
    }

    def __init__(self, service_name, device_config, serial_number, mqtt_config):
        super().__init__(service_name, register=False)

        self.device_config = device_config
        self.device_index = device_config.getint('DeviceIndex')

        # General device settings
        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.18') # Updated version
        self.add_path('/Mgmt/Connection', 'Virtual')
        
        self.add_path('/DeviceInstance', self.device_config.getint('DeviceInstanceInstance'))
        self.add_path('/ProductId', 49248) # Product ID for virtual temperature sensor
        self.add_path('/ProductName', 'Virtual temperature') # Fixed product name
        self.add_path('/CustomName', self.device_config.get('CustomName'), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Serial', serial_number)
        
        self.add_path('/Status', 0) # 0 for OK
        self.add_path('/Connected', 1) # 1 for connected

        # Temperature specific paths
        self.add_path('/Temperature', 0.0) # Initial temperature
        self.add_path('/BatteryVoltage', 0.0) # Initial BatteryVoltage
        self.add_path('/Humidity', 0.0) # Initial Humidity

        # TemperatureType mapping and D-Bus path
        initial_type_str = self.device_config.get('Type', 'generic').lower()
        initial_type_int = self.TEMPERATURE_TYPES.get(initial_type_str, self.TEMPERATURE_TYPES['generic'])
        self.add_path('/TemperatureType', initial_type_int, writeable=True, onchangecallback=self.handle_dbus_change)

        # MQTT specific members
        self.mqtt_client = None
        self.mqtt_config = mqtt_config
        self.dbus_path_to_state_topic_map = {
            '/Temperature': self.device_config.get('TemperatureStateTopic'),
            '/Humidity': self.device_config.get('HumidityStateTopic'),
            '/BatteryVoltage': self.device_config.get('BatteryStateTopic')
        }

        # Remove None, empty, or 'path/to/mqtt' values from the map
        self.dbus_path_to_state_topic_map = {
            k: v for k, v in self.dbus_path_to_state_topic_map.items()
            if v is not None and v != '' and 'path/to/mqtt' not in v
        }

        # Initialize and connect the MQTT client
        self.setup_mqtt_client()
        
        self.register()
        logger.info(f"Service '{service_name}' for device '{self.device_config.get('CustomName')}' registered on D-Bus.")

    def setup_mqtt_client(self, retry_interval=5, max_retries=12):
        """
        Initializes and starts the MQTT client with retry logic.
        """
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
        
        retries = 0
        while retries < max_retries:
            try:
                logger.debug(f"Attempting to connect to MQTT broker ({retries + 1}/{max_retries})...")
                self.mqtt_client.connect(
                    self.mqtt_config.get('BrokerAddress'),
                    self.mqtt_config.getint('Port', 1883),
                    60
                )
                self.mqtt_client.loop_start()
                logger.debug("MQTT client started.")
                return
            except Exception as e:
                logger.error(f"Failed to connect to MQTT broker: {e}. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
                retries += 1
        
        logger.critical(f"Failed to connect to MQTT broker after {max_retries} attempts. Exiting.")
        sys.exit(1)

    def on_mqtt_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            logger.debug("Connected to MQTT Broker!")
            for dbus_path, topic in self.dbus_path_to_state_topic_map.items():
                if topic:
                    client.subscribe(topic)
                    logger.debug(f"Subscribed to MQTT state topic: {topic} for D-Bus path {dbus_path}")
        else:
            logger.error(f"Failed to connect to MQTT broker, return code {rc}")

    def on_mqtt_message(self, client, userdata, msg):
        """
        MQTT callback for when a message is received on a subscribed topic.
        Handles JSON payloads with a 'value' key, or raw string payloads.
        """
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            logger.debug(f"Received MQTT message on topic '{topic}': {payload_str}")

            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            
            if not dbus_path:
                logger.warning(f"Received MQTT message on unknown topic for TempSensor: {topic}")
                return

            value = None
            try:
                # Attempt to parse as JSON first
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json:
                    value = float(incoming_json["value"])
                else:
                    logger.warning(f"JSON payload for topic '{topic}' does not contain a 'value' key: {payload_str}. Skipping update.")
                    return
            except json.JSONDecodeError:
                # If not JSON, try to parse as a direct float
                try:
                    value = float(payload_str)
                except ValueError:
                    logger.warning(f"Invalid numeric payload received for topic '{topic}': '{payload_str}'. Expected a number or a JSON object with a 'value' key.")
                    return
            
            logger.debug(f"Parsed value from MQTT for {topic}: {value} (type: {type(value)})")

            # Use GLib.idle_add to schedule the D-Bus update in the main thread
            GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)
            
        except Exception as e:
            logger.error(f"Error processing MQTT message for TempSensor: {e}")
            
    def handle_dbus_change(self, path, value):
        if path == '/CustomName':
            key_name = 'CustomName'
            section_name = f'Temp_Sensor_{self.device_index}'
            logger.debug(f"D-Bus settings change triggered for {path} with value '{value}'. Saving to config file.")
            self.save_config_change(section_name, key_name, value)
            return True
        elif path == '/TemperatureType':
            # Convert integer back to string for saving
            type_str = next((k for k, v in self.TEMPERATURE_TYPES.items() if v == value), 'generic')
            key_name = 'Type'
            section_name = f'Temp_Sensor_{self.device_index}'
            logger.debug(f"D-Bus settings change triggered for {path} with value '{value}' ({type_str}). Saving to config file.")
            self.save_config_change(section_name, key_name, type_str)
            return True
        logger.warning(f"Unhandled D-Bus change request for TempSensor path: {path}")
        return False

    def save_config_change(self, section, key, value):
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section):
                logger.warning(f"Creating new section '{section}' in config file.")
                config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as configfile:
                config.write(configfile)
            logger.debug(f"Successfully saved setting '{key}' to section '{section}' in config file.")
        except Exception as e:
            logger.error(f"Failed to save config file changes for TempSensor key '{key}' in section '{section}': {e}")

    def update_dbus_from_mqtt(self, path, value):
        self[path] = value
        logger.debug(f"Successfully changed '{path}' to {value} from source: mqtt (TempSensor)")
        return False # Run once

class DbusTankSensor(VeDbusService):
    FLUID_TYPES = {
        'fuel': 0,
        'fresh water': 1,
        'waste water': 2,
        'live well': 3,
        'oil': 4,
        'black water': 5,
        'gasoline': 6,
        'diesel': 7,
        'lpg': 8,
        'lng': 9,
        'hydraulic oil': 10,
        'raw water': 11
    }

    def __init__(self, service_name, device_config, serial_number, mqtt_config):
        super().__init__(service_name, register=False)

        self.device_config = device_config
        self.device_index = device_config.getint('DeviceIndex')

        # General device settings
        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.18') # Updated version
        self.add_path('/Mgmt/Connection', 'Virtual')
        
        self.add_path('/DeviceInstance', self.device_config.getint('DeviceInstance'))
        self.add_path('/ProductId', 49251) # Product ID for virtual tank sensor
        self.add_path('/ProductName', 'Virtual tank') # Fixed product name
        self.add_path('/CustomName', self.device_config.get('CustomName'), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Serial', serial_number)
        
        self.add_path('/Status', 0) # 0 for OK
        self.add_path('/Connected', 1) # 1 for connected

        # Tank specific paths
        self.add_path('/BatteryVoltage', 0.0)
        self.add_path('/Capacity', self.device_config.getfloat('Capacity', 0.2), writeable=True, onchangecallback=self.handle_dbus_change)
        
        initial_fluid_type_str = self.device_config.get('FluidType', 'fresh water').lower()
        initial_fluid_type_int = self.FLUID_TYPES.get(initial_fluid_type_str, self.FLUID_TYPES['fresh water'])
        self.add_path('/FluidType', initial_fluid_type_int, writeable=True, onchangecallback=self.handle_dbus_change)
        
        self.add_path('/Level', 0.0) # Calculated or received via MQTT
        self.add_path('/RawUnit', self.device_config.get('RawUnit', ''))
        self.add_path('/RawValue', 0.0) # Received via MQTT or manually set
        self.add_path('/RawValueEmpty', self.device_config.getfloat('RawValueEmpty', 0.0), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/RawValueFull', self.device_config.getfloat('RawValueFull', 0.0), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Remaining', 0.0) # Calculated
        self.add_path('/Shape', 0) # Not specified if writable, default to 0
        self.add_path('/Temperature', 0.0) # Not specified if writable, initial 0.0

        # Alarms
        self.add_path('/Alarms/High/Active', 0)
        self.add_path('/Alarms/High/Delay', 0)
        self.add_path('/Alarms/High/Enable', 0)
        self.add_path('/Alarms/High/Restore', 0)
        self.add_path('/Alarms/High/State', 0)
        self.add_path('/Alarms/Low/Active', 0)
        self.add_path('/Alarms/Low/Delay', 0)
        self.add_path('/Alarms/Low/Enable', 0)
        self.add_path('/Alarms/Low/Restore', 0)
        self.add_path('/Alarms/Low/State', 0)


        # MQTT specific members
        self.mqtt_client = None
        self.mqtt_config = mqtt_config
        self.dbus_path_to_state_topic_map = {}
        self.is_level_direct = False # Flag to indicate if Level is directly provided or calculated from RawValue

        # Helper function to validate a topic string
        def is_valid_topic(topic):
            return topic is not None and topic != '' and 'path/to/mqtt' not in topic

        # Determine if Level is directly provided or calculated from RawValue
        level_state_topic = self.device_config.get('LevelStateTopic')
        raw_value_state_topic = self.device_config.get('RawValueStateTopic')

        self.is_level_direct = False # Default to RawValue calculation or no level updates

        if is_valid_topic(raw_value_state_topic):
            self.dbus_path_to_state_topic_map['/RawValue'] = raw_value_state_topic
            logger.debug(f"Tank Sensor {self.device_index} configured for RawValue updates via MQTT to calculate Level.")
        elif is_valid_topic(level_state_topic):
            self.is_level_direct = True
            self.dbus_path_to_state_topic_map['/Level'] = level_state_topic
            logger.debug(f"Tank Sensor {self.device_index} configured for direct Level updates via MQTT.")
        else:
            logger.warning(f"Tank Sensor {self.device_index} has neither LevelStateTopic nor RawValueStateTopic configured. Level and Remaining will not update via MQTT.")

        # Always add temperature and battery voltage if their topics exist, regardless of level source
        if is_valid_topic(self.device_config.get('TemperatureStateTopic')):
            self.dbus_path_to_state_topic_map['/Temperature'] = self.device_config.get('TemperatureStateTopic')
        if is_valid_topic(self.device_config.get('BatteryStateTopic')):
            self.dbus_path_to_state_topic_map['/BatteryVoltage'] = self.device_config.get('BatteryStateTopic')

        logger.debug(f"Tank Sensor {self.device_index} - is_level_direct: {self.is_level_direct}")
        logger.debug(f"Tank Sensor {self.device_index} - dbus_path_to_state_topic_map: {self.dbus_path_to_state_topic_map}")

        # Initialize and connect the MQTT client
        self.setup_mqtt_client()
        
        self.register()
        logger.info(f"Service '{service_name}' for device '{self.device_config.get('CustomName')}' registered on D-Bus.")

        # Perform initial calculation after paths are registered
        if not self.is_level_direct: # Only calculate level from raw value if raw value is the primary input
            self._calculate_level_from_raw_value()
        # Always calculate remaining based on the current level, whether it was set directly or calculated
        self._calculate_remaining_from_level()


    def setup_mqtt_client(self, retry_interval=5, max_retries=12):
        """
        Initializes and starts the MQTT client with retry logic.
        """
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
        
        retries = 0
        while retries < max_retries:
            try:
                logger.debug(f"Attempting to connect to MQTT broker ({retries + 1}/{max_retries})...")
                self.mqtt_client.connect(
                    self.mqtt_config.get('BrokerAddress'),
                    self.mqtt_config.getint('Port', 1883),
                    60
                )
                self.mqtt_client.loop_start()
                logger.debug("MQTT client started.")
                return
            except Exception as e:
                logger.error(f"Failed to connect to MQTT broker: {e}. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
                retries += 1
        
        logger.critical(f"Failed to connect to MQTT broker after {max_retries} attempts. Exiting.")
        sys.exit(1)

    def on_mqtt_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            logger.debug("Connected to MQTT Broker!")
            for dbus_path, topic in self.dbus_path_to_state_topic_map.items():
                if topic:
                    client.subscribe(topic)
                    logger.debug(f"Subscribed to MQTT state topic: {topic} for D-Bus path {dbus_path}")
        else:
            logger.error(f"Failed to connect to MQTT broker, return code {rc}")

    def on_mqtt_message(self, client, userdata, msg):
        """
        MQTT callback for when a message is received on a subscribed topic.
        Handles JSON payloads with a 'value' key, or raw string payloads.
        """
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            logger.debug(f"Received MQTT message on topic '{topic}': {payload_str}")

            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            
            if not dbus_path:
                logger.warning(f"Received MQTT message on unknown topic for TankSensor: {topic}")
                return

            value = None
            try:
                # Attempt to parse as JSON first
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json:
                    value = float(incoming_json["value"])
                else:
                    logger.warning(f"JSON payload for topic '{topic}' does not contain a 'value' key: {payload_str}. Skipping update.")
                    return
            except json.JSONDecodeError:
                # If not JSON, try to parse as a direct float
                try:
                    value = float(payload_str)
                except ValueError:
                    logger.warning(f"Invalid numeric payload received for topic '{topic}': '{payload_str}'. Expected a number or a JSON object with a 'value' key.")
                    return
            
            logger.debug(f"Parsed value from MQTT for {topic}: {value} (type: {type(value)})")

            # Use GLib.idle_add to schedule the D-Bus update in the main thread
            if dbus_path == '/RawValue' and not self.is_level_direct:
                logger.debug(f"Calling _update_raw_value_and_trigger_calculation for value: {value}")
                GLib.idle_add(self._update_raw_value_and_trigger_calculation, value)
            elif dbus_path == '/Level' and self.is_level_direct:
                logger.debug(f"Calling _update_level_and_calculate_remaining for value: {value}")
                GLib.idle_add(self._update_level_and_calculate_remaining, value)
            else:
                # This covers /Temperature and /BatteryVoltage, or if a topic is mismatched with is_level_direct
                logger.debug(f"Calling update_dbus_from_mqtt (generic) for {dbus_path} with value: {value}")
                GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)
            
        except Exception as e:
            logger.error(f"Error processing MQTT message for TankSensor: {e}")

    def _update_level_and_calculate_remaining(self, level_value):
        """
        Updates Level directly and then calculates Remaining.
        This is called from GLib.idle_add to ensure thread safety.
        """
        if 0.0 <= level_value <= 100.0:
            self['/Level'] = round(level_value, 2)
            logger.debug(f"Successfully changed '/Level' to {self['/Level']} from source: mqtt (direct level update)")
            self._calculate_remaining_from_level()
        else:
            logger.warning(f"Received invalid level value: {level_value}. Level must be between 0 and 100.")
        return False # Run once

    def _update_raw_value_and_trigger_calculation(self, raw_value):
        """
        Updates RawValue and then triggers recalculation of Level and Remaining.
        This is called from GLib.idle_add to ensure thread safety.
        """
        self['/RawValue'] = raw_value
        logger.debug(f"Successfully changed '/RawValue' to {raw_value} from source: mqtt")
        self._calculate_level_from_raw_value_and_then_remaining()
        return False # Run once

    def _calculate_level_from_raw_value_and_then_remaining(self):
        """
        Helper to calculate Level from RawValue and then Remaining from the new Level.
        """
        self._calculate_level_from_raw_value()
        self._calculate_remaining_from_level()
        return False # Run once

    def _calculate_level_from_raw_value(self):
        """
        Calculates Level based on RawValue, RawValueEmpty, and RawValueFull.
        """
        raw_value = self['/RawValue']
        raw_empty = self['/RawValueEmpty']
        raw_full = self['/RawValueFull']

        level = 0.0
        if raw_full != raw_empty:
            level = ((raw_value - raw_empty) / (raw_full - raw_empty)) * 100.0
            # Clamp level between 0 and 100
            level = max(0.0, min(100.0, level))
        elif raw_value == raw_full and raw_value != 0: # Check if it's the specific non-zero 'full' value
            level = 100.0
        elif raw_value == raw_empty and raw_value == 0: # Check if it's the specific zero 'empty' value
            level = 0.0
        else:
             logger.warning(f"RawValueFull ({raw_full}) is equal to RawValueEmpty ({raw_empty}), "
                            f"but RawValue ({raw_value}) is different or ambiguous. Cannot calculate level reliably, defaulting to 0.")
             level = 0.0 # Default to empty in ambiguous cases

        if self['/Level'] != round(level, 2):
            self['/Level'] = round(level, 2)
            logger.debug(f"Calculated and updated '/Level' to {self['/Level']}% from RawValue: {raw_value}")
        return False # For GLib.idle_add

    def _calculate_remaining_from_level(self):
        """
        Calculates Remaining from Level and Capacity.
        """
        level = self['/Level']
        capacity = self['/Capacity']
        
        remaining = (level / 100.0) * capacity
        
        if self['/Remaining'] != round(remaining, 2): # Round to 2 decimal places
            self['/Remaining'] = round(remaining, 2)
            logger.debug(f"Calculated and updated '/Remaining' to {self['/Remaining']} from Level: {level}% and Capacity: {capacity}")
        return False # For GLib.idle_add

    def handle_dbus_change(self, path, value):
        """
        Callback function to handle changes to D-Bus paths for tank sensors.
        This is triggered when a D-Bus client requests a change or an internal calculation occurs.
        """
        section_name = f'Tank_Sensor_{self.device_index}'
        
        if path == '/CustomName':
            key_name = 'CustomName'
            logger.debug(f"D-Bus settings change triggered for {path} with value '{value}'. Saving to config file.")
            self.save_config_change(section_name, key_name, value)
            return True
        elif path == '/FluidType':
            key_name = 'FluidType'
            # Convert integer value back to string for saving
            fluid_type_str = next((k for k, v in self.FLUID_TYPES.items() if v == value), None)
            if fluid_type_str:
                logger.debug(f"D-Bus settings change triggered for {path} with value '{fluid_type_str}'. Saving to config file and recalculating remaining.")
                self.save_config_change(section_name, key_name, fluid_type_str)
                GLib.idle_add(self._calculate_remaining_from_level)
                return True
            else:
                logger.warning(f"Invalid FluidType value received: {value}. Not saving to config.")
                return False
        elif path == '/Capacity':
            key_name = 'Capacity'
            logger.debug(f"D-Bus settings change triggered for {path} with value '{value}'. Saving to config file and recalculating remaining.")
            self.save_config_change(section_name, key_name, value)
            GLib.idle_add(self._calculate_remaining_from_level)
            return True
        elif path in ['/RawValueEmpty', '/RawValueFull']:
            key_name = path.replace('/', '') # e.g., 'RawValueEmpty', 'RawValueFull'
            logger.debug(f"D-Bus settings change triggered for {path} with value '{value}'. Saving to config file.")
            self.save_config_change(section_name, key_name, value)
            # Only trigger recalculation if RawValue is the primary input
            if not self.is_level_direct:
                logger.debug(f"Triggering _calculate_level_from_raw_value_and_then_remaining due to {path} change.")
                GLib.idle_add(self._calculate_level_from_raw_value_and_then_remaining)
            return True

        logger.warning(f"Unhandled D-Bus change request for path: {path}")
        return False

    def save_config_change(self, section, key, value):
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section):
                logger.warning(f"Creating new section '{section}' in config file.")
                config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as configfile:
                config.write(configfile)
            logger.debug(f"Successfully saved setting '{key}' to section '{section}' in config file.")
        except Exception as e:
            logger.error(f"Failed to save config file changes for key '{key}' in section '{section}': {e}")
            
    def update_dbus_from_mqtt(self, path, value):
        """
        A centralized method to handle MQTT-initiated state changes to D-Bus.
        """
        self[path] = value
        logger.debug(f"Successfully changed '{path}' to {value} from source: mqtt (generic update)")
        
        return False # Return False for GLib.idle_add to run only once

class DbusBattery(VeDbusService):

    def __init__(self, service_name, device_config, serial_number, mqtt_config):
        super().__init__(service_name, register=False)

        self.device_config = device_config
        self.device_index = device_config.getint('DeviceIndex')

        # General device settings
        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.18') # Updated version
        self.add_path('/Mgmt/Connection', 'Virtual')
        
        self.add_path('/DeviceInstance', self.device_config.getint('DeviceInstance'))
        self.add_path('/ProductId', 49253) # Product ID for virtual battery
        self.add_path('/ProductName', 'Virtual battery') # Fixed product name
        self.add_path('/CustomName', self.device_config.get('CustomName'), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Serial', serial_number)
        
        self.add_path('/Connected', 1)
        self.add_path('/ErrorCode', 0)
        self.add_path('/NrOfDistributors', 0) # Not clear from example, assuming 0
        self.add_path('/Soc', 0.0) # Initial State of Charge
        self.add_path('/Soh', 100.0) # Initial State of Health
        self.add_path('/System/MinCellVoltage', None) # As per example
        
        # Capacity is writable and linked to config
        self.add_path('/Capacity', self.device_config.getfloat('CapacityAh'), writeable=True, onchangecallback=self.handle_dbus_change)

        # DC Paths
        self.add_path('/Dc/0/Current', 0.0)
        self.add_path('/Dc/0/Power', 0.0)
        self.add_path('/Dc/0/Temperature', 25.0) # Default to 25C
        self.add_path('/Dc/0/Voltage', 0.0)

        # Info Paths (not writable via D-Bus directly, derived or received from MQTT)
        self.add_path('/Info/ChargeRequest', 0)
        self.add_path('/Info/MaxChargeCurrent', None)
        self.add_path('/Info/MaxChargeVoltage', None)
        self.add_path('/Info/MaxDischargeCurrent', None)
        self.add_path('/Info/BatteryLowVoltage', None) # As per example, seems to be a static info path

        # Alarm Paths (not writable via D-Bus, received via MQTT or derived)
        # All alarms default to 0 (No alarm) as per example
        self.add_path('/Alarms/CellImbalance', 0)
        self.add_path('/Alarms/HighCellVoltage', 0)
        self.add_path('/Alarms/HighChargeCurrent', 0)
        self.add_path('/Alarms/HighCurrent', 0)
        self.add_path('/Alarms/HighDischargeCurrent', 0)
        self.add_path('/Alarms/HighTemperature', 0)
        self.add_path('/Alarms/HighVoltage', 0)
        self.add_path('/Alarms/InternalFailure', 0)
        self.add_path('/Alarms/LowCellVoltage', 0)
        self.add_path('/Alarms/LowSoc', 0)
        self.add_path('/Alarms/LowTemperature', 0)
        self.add_path('/Alarms/LowVoltage', 0)
        self.add_path('/Alarms/StateOfHealth', 0)

        # MQTT specific members
        self.mqtt_client = None
        self.mqtt_config = mqtt_config
        
        # Map D-Bus paths to their respective MQTT state topics from config
        self.dbus_path_to_state_topic_map = {
            '/Dc/0/Current': self.device_config.get('CurrentStateTopic'),
            '/Dc/0/Power': self.device_config.get('PowerStateTopic'),
            '/Dc/0/Temperature': self.device_config.get('TemperatureStateTopic'),
            '/Dc/0/Voltage': self.device_config.get('VoltageStateTopic'),
            '/Info/MaxChargeCurrent': self.device_config.get('MaxChargeCurrentStateTopic'),
            '/Info/MaxChargeVoltage': self.device_config.get('MaxChargeVoltageStateTopic'),
            '/Info/MaxDischargeCurrent': self.device_config.get('MaxDischargeCurrentStateTopic'),
            '/Soc': self.device_config.get('SocStateTopic'),
            '/Soh': self.device_config.get('SohStateTopic'),
            # Add alarm topics here if they are provided via MQTT
            '/Alarms/CellImbalance': self.device_config.get('CellImbalanceAlarmTopic'),
            '/Alarms/HighCellVoltage': self.device_config.get('HighCellVoltageAlarmTopic'),
            '/Alarms/HighChargeCurrent': self.device_config.get('HighChargeCurrentAlarmTopic'),
            '/Alarms/HighCurrent': self.device_config.get('HighCurrentAlarmTopic'),
            '/Alarms/HighDischargeCurrent': self.device_config.get('HighDischargeCurrentAlarmTopic'),
            '/Alarms/HighTemperature': self.device_config.get('HighTemperatureAlarmTopic'),
            '/Alarms/HighVoltage': self.device_config.get('HighVoltageAlarmTopic'),
            '/Alarms/InternalFailure': self.device_config.get('InternalFailureAlarmTopic'),
            '/Alarms/LowCellVoltage': self.device_config.get('LowCellVoltageAlarmTopic'),
            '/Alarms/LowSoc': self.device_config.get('LowSocAlarmTopic'),
            '/Alarms/LowTemperature': self.device_config.get('LowTemperatureAlarmTopic'),
            '/Alarms/LowVoltage': self.device_config.get('LowVoltageAlarmTopic'),
            '/Alarms/StateOfHealth': self.device_config.get('StateOfHealthAlarmTopic'),
        }

        # Filter out invalid topics (None, empty string, or placeholder)
        self.dbus_path_to_state_topic_map = {
            k: v for k, v in self.dbus_path_to_state_topic_map.items()
            if v is not None and v != '' and 'path/to/mqtt' not in v
        }
        
        logger.debug(f"Battery Sensor {self.device_index} - dbus_path_to_state_topic_map: {self.dbus_path_to_state_topic_map}")

        # Initialize and connect the MQTT client
        self.setup_mqtt_client()
        
        self.register()
        logger.info(f"Service '{service_name}' for device '{self.device_config.get('CustomName')}' registered on D-Bus.")

    def setup_mqtt_client(self, retry_interval=5, max_retries=12):
        """
        Initializes and starts the MQTT client with retry logic.
        """
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
        
        retries = 0
        while retries < max_retries:
            try:
                logger.debug(f"Attempting to connect to MQTT broker ({retries + 1}/{max_retries})...")
                self.mqtt_client.connect(
                    self.mqtt_config.get('BrokerAddress'),
                    self.mqtt_config.getint('Port', 1883),
                    60
                )
                self.mqtt_client.loop_start()
                logger.debug("MQTT client started.")
                return
            except Exception as e:
                logger.error(f"Failed to connect to MQTT broker: {e}. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
                retries += 1
        
        logger.critical(f"Failed to connect to MQTT broker after {max_retries} attempts. Exiting.")
        sys.exit(1)

    def on_mqtt_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            logger.debug("Connected to MQTT Broker!")
            for dbus_path, topic in self.dbus_path_to_state_topic_map.items():
                if topic:
                    client.subscribe(topic)
                    logger.debug(f"Subscribed to MQTT state topic: {topic} for D-Bus path {dbus_path}")
        else:
            logger.error(f"Failed to connect to MQTT broker, return code {rc}")

    def on_mqtt_message(self, client, userdata, msg):
        """
        MQTT callback for when a message is received on a subscribed topic.
        Handles JSON payloads with a 'value' key, or raw string payloads.
        """
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            logger.debug(f"Received MQTT message on topic '{topic}': {payload_str}")

            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            
            if not dbus_path:
                logger.warning(f"Received MQTT message on unknown topic for Battery: {topic}")
                return

            value = None
            try:
                # Attempt to parse as JSON first
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json:
                    value = incoming_json["value"] # Can be float or int for alarms
                else:
                    logger.warning(f"JSON payload for topic '{topic}' does not contain a 'value' key: {payload_str}. Skipping update.")
                    return
            except json.JSONDecodeError:
                # If not JSON, try to parse as a direct float/int
                try:
                    value = float(payload_str)
                    if dbus_path.startswith('/Alarms'): # If it's an alarm path, convert to int
                        value = int(value)
                except ValueError:
                    logger.warning(f"Invalid numeric payload received for topic '{topic}': '{payload_str}'. Expected a number or a JSON object with a 'value' key.")
                    return
            
            logger.debug(f"Parsed value from MQTT for {topic}: {value} (type: {type(value)})")

            # Use GLib.idle_add to schedule the D-Bus update in the main thread
            GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)
            
        except Exception as e:
            logger.error(f"Error processing MQTT message for Battery: {e}")

    def handle_dbus_change(self, path, value):
        """
        Callback function to handle changes to D-Bus paths for battery sensors.
        """
        section_name = f'Virtual_Battery_{self.device_index}'
        
        if path == '/CustomName':
            key_name = 'CustomName'
            logger.debug(f"D-Bus settings change triggered for {path} with value '{value}'. Saving to config file.")
            self.save_config_change(section_name, key_name, value)
            return True
        elif path == '/Capacity':
            key_name = 'CapacityAh' # Note: config key is 'CapacityAh'
            logger.debug(f"D-Bus settings change triggered for {path} with value '{value}'. Saving to config file.")
            self.save_config_change(section_name, key_name, value)
            return True
        
        logger.warning(f"Unhandled D-Bus change request for Battery path: {path}")
        return False

    def save_config_change(self, section, key, value):
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section):
                logger.warning(f"Creating new section '{section}' in config file.")
                config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as configfile:
                config.write(configfile)
            logger.debug(f"Successfully saved setting '{key}' to section '{section}' in config file.")
        except Exception as e:
            logger.error(f"Failed to save config file changes for Battery key '{key}' in section '{section}': {e}")
            
    def update_dbus_from_mqtt(self, path, value):
        """
        A centralized method to handle MQTT-initiated state changes to D-Bus.
        """
        self[path] = value
        logger.debug(f"Successfully changed '{path}' to {value} from source: mqtt (battery update)")
        
        return False # Return False for GLib.idle_add to run only once

def run_device_service(device_type, device_index):
    """
    Main function for a single D-Bus service process, now distinguishing by device type.
    """
    from dbus.mainloop.glib import DBusGMainLoop
    DBusGMainLoop(set_as_default=True)
    
    logger.info(f"Starting D-Bus service process for {device_type} {device_index}.")

    config = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE_PATH):
        logger.critical(f"Configuration file not found: {CONFIG_FILE_PATH}")
        sys.exit(1)
    
    try:
        config.read(CONFIG_FILE_PATH)
    except configparser.Error as e:
        logger.critical(f"Error parsing configuration file: {e}")
        sys.exit(1)
        
    LOG_LEVELS = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    if config.has_section('Global'):
        log_level_str = config['Global'].get('LogLevel', 'INFO').upper()
        log_level = LOG_LEVELS.get(log_level_str, logging.INFO)
    else:
        log_level = logging.INFO
        
    logger.setLevel(log_level)
    logger.debug(f"Log level set to: {logging.getLevelName(logger.level)}")

    if device_type == 'switch':
        device_section = f'Relay_Module_{device_index}'
        if not config.has_section(device_section):
            logger.critical(f"Configuration section '{device_section}' not found. Cannot start.")
            sys.exit(1)
            
        device_config = config[device_section]
        
        # MODIFIED: Retrieve all four distinct payload values from config

        mqtt_on_state_payload = device_config.get('mqtt_on_state_payload', '1')
        mqtt_off_state_payload = device_config.get('mqtt_off_state_payload', '0')
        mqtt_on_command_payload = device_config.get('mqtt_on_command_payload', '1') # Default to '1' for command
        mqtt_off_command_payload = device_config.get('mqtt_off_command_payload', '0') # Default to '0' for command

        device_config['DeviceIndex'] = str(device_index)
        
        serial_number = device_config.get('Serial')
        if not serial_number or serial_number.strip() == '':
            logger.critical(f"Serial number not found or is empty for device {device_index} in config. Exiting. Please run the setup script to generate a serial.")
            sys.exit(1)
        else:
            logger.debug(f"Using existing serial number '{serial_number}' for device {device_index}.")

        try:
            num_switches = device_config.getint('NumberOfSwitches')
        except (configparser.NoOptionError, ValueError):
            logger.warning(f"No 'NumberOfSwitches' found in [{device_section}] section. Defaulting to 1 switch.")
            num_switches = 1

        output_configs = []
        for j in range(1, num_switches + 1):
            output_section = f'switch_{device_index}_{j}'
            
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

        mqtt_config = config['MQTT'] if config.has_section('MQTT') else {}

        # MODIFIED: Pass all four payload values to the DbusSwitch constructor
        DbusSwitch(service_name, device_config, output_configs, serial_number, mqtt_config,
                   mqtt_on_state_payload, mqtt_off_state_payload, mqtt_on_command_payload, mqtt_off_command_payload)
    
    elif device_type == 'temp_sensor':
        device_section = f'Temp_Sensor_{device_index}'
        if not config.has_section(device_section):
            logger.critical(f"Configuration section '{device_section}' not found. Cannot start.")
            sys.exit(1)
            
        device_config = config[device_section]
        device_config['DeviceIndex'] = str(device_index)

        serial_number = device_config.get('Serial')
        if not serial_number or serial_number.strip() == '':
            logger.critical(f"Serial number not found or is empty for temperature sensor {device_index} in config. Exiting. Please run the setup script to generate a serial.")
            sys.exit(1)
        else:
            logger.debug(f"Using existing serial number '{serial_number}' for temperature sensor {device_index}.")
        
        service_name = f'com.victronenergy.temperature.virtual_{serial_number}'
        mqtt_config = config['MQTT'] if config.has_section('MQTT') else {}

        DbusTempSensor(service_name, device_config, serial_number, mqtt_config)

    elif device_type == 'tank_sensor':
        device_section = f'Tank_Sensor_{device_index}'
        if not config.has_section(device_section):
            logger.critical(f"Configuration section '{device_section}' not found. Cannot start.")
            sys.exit(1)
            
        device_config = config[device_section]
        device_config['DeviceIndex'] = str(device_index)

        serial_number = device_config.get('Serial')
        if not serial_number or serial_number.strip() == '':
            logger.critical(f"Serial number not found or is empty for tank sensor {device_index} in config. Exiting. Please run the setup script to generate a serial.")
            sys.exit(1)
        else:
            logger.debug(f"Using existing serial number '{serial_number}' for tank sensor {device_index}.")
        
        service_name = f'com.victronenergy.tank.virtual_{serial_number}'
        mqtt_config = config['MQTT'] if config.has_section('MQTT') else {}

        DbusTankSensor(service_name, device_config, serial_number, mqtt_config)
    
    # --- BEGIN: VIRTUAL BATTERY INTEGRATION ---
    elif device_type == 'battery':
        device_section = f'Virtual_Battery_{device_index}'
        if not config.has_section(device_section):
            logger.critical(f"Configuration section '{device_section}' not found. Cannot start.")
            sys.exit(1)
            
        device_config = config[device_section]
        device_config['DeviceIndex'] = str(device_index)

        serial_number = device_config.get('Serial')
        if not serial_number or serial_number.strip() == '':
            logger.critical(f"Serial number not found or is empty for virtual battery {device_index} in config. Exiting. Please run the setup script to generate a serial.")
            sys.exit(1)
        else:
            logger.debug(f"Using existing serial number '{serial_number}' for virtual battery {device_index}.")
        
        service_name = f'com.victronenergy.battery.virtual_{serial_number}'
        mqtt_config = config['MQTT'] if config.has_section('MQTT') else {}

        DbusBattery(service_name, device_config, serial_number, mqtt_config)
    # --- END: VIRTUAL BATTERY INTEGRATION ---
        
    else:
        logger.critical(f"Unknown device type: {device_type}")
        sys.exit(1)

    logger.debug('Connected to D-Bus, and switching over to GLib.MainLoop() (= event based)')
    
    mainloop = GLib.MainLoop()
    mainloop.run()

def main():
    """
    The main launcher function that runs as the parent process.
    """
    logger.info("Starting D-Bus Virtual Devices service launcher.")

    config = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE_PATH):
        logger.critical(f"Configuration file not found: {CONFIG_FILE_PATH}")
        sys.exit(1)
    
    try:
        config.read(CONFIG_FILE_PATH)
    except configparser.Error as e:
        logger.critical(f"Error parsing configuration file: {e}")
        sys.exit(1)
    
    LOG_LEVELS = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    if config.has_section('Global'):
        log_level_str = config['Global'].get('LogLevel', 'INFO').upper()
        log_level = LOG_LEVELS.get(log_level_str, logging.INFO)
    else:
        log_level = logging.INFO
    
    logger.setLevel(log_level)
    logger.debug(f"Log level set to: {logging.getLevelName(logger.level)}")

    processes = []
    script_path = os.path.abspath(sys.argv[0])

    # Handle virtual switch devices
    try:
        num_switch_modules = config.getint('Global', 'numberofmodules')
    except (configparser.NoSectionError, configparser.NoOptionError):
        logger.warning("No 'numberofmodules' found in [Global] section. Defaulting to 0 switch modules.")
        num_switch_modules = 0

    logger.debug(f"Starting {num_switch_modules} virtual switch device processes...")
    for i in range(1, num_switch_modules + 1):
        device_section_found = False
        for section in config.sections():
            if section.lower() == f'relay_module_{i}'.lower():
                device_section_found = True
                break
        
        if not device_section_found:
            logger.warning(f"Configuration section for Relay_Module_{i} not found. Skipping switch device {i}.")
            continue
            
        cmd = [sys.executable, script_path, 'switch', str(i)]
        try:
            process = subprocess.Popen(cmd, env=os.environ, close_fds=True)
            processes.append(process)
            logger.debug(f"Started process for virtual switch device {i} (PID: {process.pid})")
        except Exception as e:
            logger.error(f"Failed to start process for switch device {i}: {e}")

    # Handle virtual temperature sensor devices
    try:
        num_temp_sensors = config.getint('Global', 'numberoftempsensors')
    except (configparser.NoSectionError, configparser.NoOptionError):
        logger.warning("No 'numberoftempsensors' found in [Global] section. Defaulting to 0 temperature sensors.")
        num_temp_sensors = 0

    logger.debug(f"Starting {num_temp_sensors} virtual temperature sensor processes...")
    for i in range(1, num_temp_sensors + 1):
        device_section_found = False
        for section in config.sections():
            if section.lower() == f'temp_sensor_{i}'.lower():
                device_section_found = True
                break
        
        if not device_section_found:
            logger.warning(f"Configuration section for Temp_Sensor_{i} not found. Skipping temperature sensor {i}.")
            continue
            
        cmd = [sys.executable, script_path, 'temp_sensor', str(i)]
        try:
            process = subprocess.Popen(cmd, env=os.environ, close_fds=True)
            processes.append(process)
            logger.debug(f"Started process for virtual temperature sensor {i} (PID: {process.pid})")
        except Exception as e:
            logger.error(f"Failed to start process for temperature sensor {i}: {e}")

    # Handle virtual tank sensor devices
    try:
        num_tank_sensors = config.getint('Global', 'numberoftanksensors')
    except (configparser.NoSectionError, configparser.NoOptionError):
        logger.warning("No 'numberoftanksensors' found in [Global] section. Defaulting to 0 tank sensors.")
        num_tank_sensors = 0

    logger.debug(f"Starting {num_tank_sensors} virtual tank sensor processes...")
    for i in range(1, num_tank_sensors + 1):
        device_section_found = False
        for section in config.sections():
            if section.lower() == f'tank_sensor_{i}'.lower():
                device_section_found = True
                break
        
        if not device_section_found:
            logger.warning(f"Configuration section for Tank_Sensor_{i} not found. Skipping tank sensor {i}.")
            continue
            
        cmd = [sys.executable, script_path, 'tank_sensor', str(i)]
        try:
            process = subprocess.Popen(cmd, env=os.environ, close_fds=True)
            processes.append(process)
            logger.debug(f"Started process for virtual tank sensor {i} (PID: {process.pid})")
        except Exception as e:
            logger.error(f"Failed to start process for tank sensor {i}: {e}")

    # --- BEGIN: VIRTUAL BATTERY INTEGRATION ---
    try:
        num_virtual_batteries = config.getint('Global', 'numberofvirtualbatteries')
    except (configparser.NoSectionError, configparser.NoOptionError):
        logger.warning("No 'numberofvirtualbatteries' found in [Global] section. Defaulting to 0 virtual batteries.")
        num_virtual_batteries = 0

    logger.debug(f"Starting {num_virtual_batteries} virtual battery processes...")
    for i in range(1, num_virtual_batteries + 1):
        device_section_found = False
        for section in config.sections():
            if section.lower() == f'virtual_battery_{i}'.lower():
                device_section_found = True
                break
        
        if not device_section_found:
            logger.warning(f"Configuration section for Virtual_Battery_{i} not found. Skipping virtual battery {i}.")
            continue
            
        cmd = [sys.executable, script_path, 'battery', str(i)]
        try:
            process = subprocess.Popen(cmd, env=os.environ, close_fds=True)
            processes.append(process)
            logger.debug(f"Started process for virtual battery {i} (PID: {process.pid})")
        except Exception as e:
            logger.error(f"Failed to start process for virtual battery {i}: {e}")
    # --- END: VIRTUAL BATTERY INTEGRATION ---

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
    if len(sys.argv) > 2:
        run_device_service(sys.argv[1], sys.argv[2])
    else:
        main()
