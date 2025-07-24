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
import json
import re # Import regex module

logger = logging.getLogger()

for handler in logger.handlers[:]:
    logger.removeHandler(handler)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

CONFIG_FILE_PATH = '/data/setupOptions/external-devices/optionsSet'

try:
    sys.path.insert(1, "/opt/victronenergy/dbus-systemcalc-py/ext/velib_python")
    from vedbus import VeDbusService
except ImportError:
    logger.critical("Cannot find vedbus library. Please ensure it's in the correct path.")
    sys.exit(1)

def get_json_attribute(data, path):
    parts = path.split('.')
    current = data
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current

# ====================================================================
# DbusSwitch Class
# ====================================================================
class DbusSwitch(VeDbusService):
    def __init__(self, service_name, device_config, output_configs, serial_number, mqtt_config,
                 mqtt_on_state_payload, mqtt_off_state_payload, mqtt_on_command_payload, mqtt_off_command_payload):
        super().__init__(service_name, register=False)
        self.device_config = device_config
        self.device_index = device_config.getint('DeviceIndex')
        self.mqtt_on_state_payload_raw = mqtt_on_state_payload
        self.mqtt_off_state_payload_raw = mqtt_off_state_payload
        self.mqtt_on_command_payload = mqtt_on_command_payload
        self.mqtt_off_command_payload = mqtt_off_command_payload
        self.mqtt_on_state_payload_json = None
        self.mqtt_off_state_payload_json = None

        try:
            parsed_on = json.loads(mqtt_on_state_payload)
            if isinstance(parsed_on, dict) and len(parsed_on) == 1:
                self.mqtt_on_state_payload_json = parsed_on
        except json.JSONDecodeError:
            pass

        try:
            parsed_off = json.loads(mqtt_off_state_payload)
            if isinstance(parsed_off, dict) and len(parsed_off) == 1:
                self.mqtt_off_state_payload_json = parsed_off
        except json.JSONDecodeError:
            pass

        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.19') # Updated version
        self.add_path('/Mgmt/Connection', 'Virtual')
        self.add_path('/DeviceInstance', self.device_config.getint('DeviceInstance'))
        self.add_path('/ProductId', 49257)
        self.add_path('/ProductName', 'Virtual switch')
        self.add_path('/CustomName', self.device_config.get('CustomName'), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Serial', serial_number)
        self.add_path('/State', 256)
        self.add_path('/FirmwareVersion', 0)
        self.add_path('/HardwareVersion', 0)
        self.add_path('/Connected', 1)

        self.mqtt_client = None
        self.mqtt_config = mqtt_config
        self.dbus_path_to_state_topic_map = {}
        self.dbus_path_to_command_topic_map = {}

        for output_data in output_configs:
            self.add_output(output_data)

        self.setup_mqtt_client()
        self.register()
        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' registered on D-Bus.")

    def add_output(self, output_data):
        output_prefix = f'/SwitchableOutput/output_{output_data["index"]}'
        state_topic = output_data.get('MqttStateTopic')
        command_topic = output_data.get('MqttCommandTopic')
        dbus_state_path = f'{output_prefix}/State'

        if state_topic and 'path/to/mqtt' not in state_topic and command_topic and 'path/to/mqtt' not in command_topic:
            self.dbus_path_to_state_topic_map[dbus_state_path] = state_topic
            self.dbus_path_to_command_topic_map[dbus_state_path] = command_topic
        else:
            logger.warning(f"MQTT topics for {dbus_state_path} are invalid. Ignoring.")

        self.add_path(f'{output_prefix}/Name', output_data['name'])
        self.add_path(f'{output_prefix}/Status', 0)
        self.add_path(dbus_state_path, 0, writeable=True, onchangecallback=self.handle_dbus_change)
        settings_prefix = f'{output_prefix}/Settings'
        self.add_path(f'{settings_prefix}/CustomName', output_data['custom_name'], writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path(f'{settings_prefix}/Group', output_data['group'], writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path(f'{settings_prefix}/Type', 1, writeable=True)
        self.add_path(f'{settings_prefix}/ValidTypes', 7)

    def setup_mqtt_client(self, retry_interval=5, max_retries=12):
        self.mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=self['/Serial'])
        if self.mqtt_config.get('Username'):
            self.mqtt_client.username_pw_set(self.mqtt_config.get('Username'), self.mqtt_config.get('Password'))
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message
        self.mqtt_client.on_publish = self.on_mqtt_publish
        retries = 0
        while retries < max_retries:
            try:
                self.mqtt_client.connect(self.mqtt_config.get('BrokerAddress'), self.mqtt_config.getint('Port', 1883), 60)
                self.mqtt_client.loop_start()
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
            state_topics = list(self.dbus_path_to_state_topic_map.values())
            for topic in state_topics:
                client.subscribe(topic)
        else:
            logger.error(f"Failed to connect to MQTT broker, return code {rc}")

    def on_mqtt_message(self, client, userdata, msg):
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            new_state = None
            try:
                incoming_json = json.loads(payload_str)
                if self.mqtt_on_state_payload_json:
                    on_attr, on_val = list(self.mqtt_on_state_payload_json.items())[0]
                    extracted_on_value = get_json_attribute(incoming_json, on_attr)
                    if extracted_on_value is not None and str(extracted_on_value).lower() == str(on_val).lower():
                        new_state = 1
                if new_state is None and self.mqtt_off_state_payload_json:
                    off_attr, off_val = list(self.mqtt_off_state_payload_json.items())[0]
                    extracted_off_value = get_json_attribute(incoming_json, off_attr)
                    if extracted_off_value is not None and str(extracted_off_value).lower() == str(off_val).lower():
                        new_state = 0
                if new_state is None:
                    processed_payload_value = str(incoming_json.get("value", payload_str)).lower()
            except json.JSONDecodeError:
                processed_payload_value = payload_str.lower()
            if new_state is None:
                if processed_payload_value == self.mqtt_on_state_payload_raw.lower():
                    new_state = 1
                elif processed_payload_value == self.mqtt_off_state_payload_raw.lower():
                    new_state = 0
                else:
                    return
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            if dbus_path and self[dbus_path] != new_state:
                GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, new_state)
        except Exception as e:
            logger.error(f"Error processing MQTT message: {e}")

    def on_mqtt_publish(self, client, userdata, mid, reason_code, properties):
        logger.debug(f"Publish message with mid: {mid} acknowledged.")

    def handle_dbus_change(self, path, value):
        if "/State" in path:
            if value in [0, 1]:
                self.publish_mqtt_command(path, value)
                return True
            return False
        elif path == '/CustomName':
            section_name = f'Relay_Module_{self.device_index}'
            self.save_config_change(section_name, 'CustomName', value)
            return True
        elif "/Settings" in path:
            try:
                parts = path.split('/')
                output_index = parts[2].replace('output_', '')
                setting_key = parts[4]
                section_name = f'switch_{self.device_index}_{output_index}'
                self.save_config_change(section_name, setting_key, value)
                return True
            except IndexError:
                return False
        return False

    def save_config_change(self, section, key, value):
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section):
                config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as configfile:
                config.write(configfile)
        except Exception as e:
            logger.error(f"Failed to save config file changes for key '{key}': {e}")

    def publish_mqtt_command(self, path, value):
        if not self.mqtt_client or not self.mqtt_client.is_connected():
            return
        if path not in self.dbus_path_to_command_topic_map:
            return
        try:
            command_topic = self.dbus_path_to_command_topic_map[path]
            mqtt_payload = self.mqtt_on_command_payload if value == 1 else self.mqtt_off_command_payload
            self.mqtt_client.publish(command_topic, mqtt_payload, retain=False)
        except Exception as e:
            logger.error(f"Error during MQTT publish: {e}")

    def update_dbus_from_mqtt(self, path, value):
        self[path] = value
        return False

# ====================================================================
# DbusDigitalInput Class
# ====================================================================
class DbusDigitalInput(VeDbusService):
    # Added mapping for text to integer conversion
    DIGITAL_INPUT_TYPES = {
        'disabled': 0,
        'pulse meter': 1,
        'door alarm': 2,
        'bilge pump': 3,
        'bilge alarm': 4,
        'burglar alarm': 5,
        'smoke alarm': 6,
        'fire alarm': 7,
        'co2 alarm': 8,
        'generator': 9,
        'touch input control': 10,
        'generic': 3 # Default if not specified or unrecognized
    }

    def __init__(self, service_name, device_config, serial_number, mqtt_config):
        super().__init__(service_name, register=False)

        self.device_config = device_config
        # The section name itself (e.g., 'input_1_1') is used for saving
        self.config_section_name = device_config.name 

        # General device settings
        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.19')
        self.add_path('/Mgmt/Connection', 'Virtual')
        
        # Paths from config
        self.add_path('/DeviceInstance', self.device_config.getint('DeviceInstance'))
        self.add_path('/ProductId', 41318) # From user example
        self.add_path('/ProductName', 'Virtual digital input')
        self.add_path('/Serial', serial_number)

        # Writable paths with callbacks
        self.add_path('/CustomName', self.device_config.get('CustomName', 'Digital Input'), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Count', self.device_config.getint('Count', 0), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/State', self.device_config.getint('State', 0), writeable=True, onchangecallback=self.handle_dbus_change)
        
        # Modified: Convert text 'Type' from config to integer for D-Bus
        initial_type_str = self.device_config.get('Type', 'generic').lower() # Get as string, make lowercase
        initial_type_int = self.DIGITAL_INPUT_TYPES.get(initial_type_str, self.DIGITAL_INPUT_TYPES['generic']) # Convert to int, default to generic
        self.add_path('/Type', initial_type_int, writeable=True, onchangecallback=self.handle_dbus_change)
        
        # Settings paths
        self.add_path('/Settings/InvertTranslation', self.device_config.getint('InvertTranslation', 0), writeable=True, onchangecallback=self.handle_dbus_change)
        # Added new D-Bus paths for InvertAlarm and AlarmSetting
        self.add_path('/Settings/InvertAlarm', self.device_config.getint('InvertAlarm', 0), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Settings/AlarmSetting', self.device_config.getint('AlarmSetting', 0), writeable=True, onchangecallback=self.handle_dbus_change)


        # Read-only paths updated by the service
        self.add_path('/Connected', 1)
        self.add_path('/InputState', 0)
        self.add_path('/Alarm', 0)

        # MQTT specific members
        self.mqtt_client = None
        self.mqtt_config = mqtt_config
        self.mqtt_state_topic = self.device_config.get('MqttStateTopic')
        self.mqtt_on_payload = self.device_config.get('MqttOnStatePayload', 'ON')
        self.mqtt_off_payload = self.device_config.get('MqttOffStatePayload', 'OFF')

        # Initialize and connect the MQTT client if a topic is set
        if self.mqtt_state_topic and 'path/to/mqtt' not in self.mqtt_state_topic:
            self.setup_mqtt_client()
        else:
            logger.warning(f"No valid MqttStateTopic for '{self['/CustomName']}'. State will not update from MQTT.")

        self.register()
        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' registered on D-Bus.")

    def setup_mqtt_client(self, retry_interval=5, max_retries=12):
        self.mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=self['/Serial'])
        if self.mqtt_config.get('Username'):
            self.mqtt_client.username_pw_set(self.mqtt_config.get('Username'), self.mqtt_config.get('Password'))
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message
        
        retries = 0
        while retries < max_retries:
            try:
                self.mqtt_client.connect(self.mqtt_config.get('BrokerAddress'), self.mqtt_config.getint('Port', 1883), 60)
                self.mqtt_client.loop_start()
                return
            except Exception as e:
                logger.error(f"Failed to connect to MQTT broker: {e}. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
                retries += 1
        logger.critical(f"Failed to connect to MQTT broker after {max_retries} attempts. Exiting.")
        sys.exit(1)

    def on_mqtt_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            logger.debug(f"Connected to MQTT Broker! Subscribing to: {self.mqtt_state_topic}")
            client.subscribe(self.mqtt_state_topic)
        else:
            logger.error(f"Failed to connect to MQTT broker, return code {rc}")

    def on_mqtt_message(self, client, userdata, msg):
        if msg.topic != self.mqtt_state_topic:
            return
        
        try:
            payload_str = msg.payload.decode().strip()
            logger.debug(f"Received MQTT message on topic '{msg.topic}': {payload_str}")

            raw_state = None
            if payload_str.lower() == self.mqtt_on_payload.lower():
                raw_state = 1
            elif payload_str.lower() == self.mqtt_off_payload.lower():
                raw_state = 0
            
            if raw_state is None:
                logger.warning(f"Invalid MQTT payload '{payload_str}' received. Expected '{self.mqtt_on_payload}' or '{self.mqtt_off_payload}'.")
                return

            # Apply inversion if set
            invert = self['/Settings/InvertTranslation']
            final_state = (1 - raw_state) if invert == 1 else raw_state

            # Schedule D-Bus update in main thread
            GLib.idle_add(self.update_dbus_from_mqtt, final_state)

        except Exception as e:
            logger.error(f"Error processing MQTT message for Digital Input: {e}")

    def update_dbus_from_mqtt(self, new_state):
        if self['/InputState'] != new_state:
            self['/InputState'] = new_state
            # Mirror InputState to State path as well
            self['/State'] = new_state 
            logger.debug(f"Updated /InputState and /State for '{self['/CustomName']}' to {new_state}")
        return False # Run only once

    def handle_dbus_change(self, path, value):
        try:
            key_name = path.split('/')[-1] # e.g., 'CustomName', 'Count', 'InvertAlarm', 'AlarmSetting'
            logger.debug(f"D-Bus settings change triggered for {path} with value '{value}'. Saving to config file.")
            
            # Modified: Convert integer 'Type' back to text for config file saving
            value_to_save = value
            if path == '/Type':
                # Find the string name for the given integer value, default to 'generic' if not found
                value_to_save = next((name for name, num in self.DIGITAL_INPUT_TYPES.items() if num == value), 'generic')

            self.save_config_change(self.config_section_name, key_name, value_to_save) # Use value_to_save
            return True
        except Exception as e:
            logger.error(f"Failed to handle D-Bus change for {path}: {e}")
            return False

    def save_config_change(self, section, key, value):
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section):
                config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as configfile:
                config.write(configfile)
            logger.debug(f"Successfully saved setting '{key}' to section '{section}' in config file.")
        except Exception as e:
            logger.error(f"Failed to save config file changes for key '{key}' in section '{section}': {e}")

# ====================================================================
# DbusTempSensor Class
# ====================================================================
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
        self.add_path('/Mgmt/ProcessVersion', '0.1.19')
        self.add_path('/Mgmt/Connection', 'Virtual')
        
        self.add_path('/DeviceInstance', self.device_config.getint('DeviceInstance'))
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
        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' registered on D-Bus.")

    def setup_mqtt_client(self, retry_interval=5, max_retries=12):
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
                self.mqtt_client.connect(
                    self.mqtt_config.get('BrokerAddress'),
                    self.mqtt_config.getint('Port', 1883),
                    60
                )
                self.mqtt_client.loop_start()
                return
            except Exception as e:
                logger.error(f"Failed to connect to MQTT broker: {e}. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
                retries += 1
        
        logger.critical(f"Failed to connect to MQTT broker after {max_retries} attempts. Exiting.")
        sys.exit(1)

    def on_mqtt_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            for dbus_path, topic in self.dbus_path_to_state_topic_map.items():
                if topic:
                    client.subscribe(topic)
        else:
            logger.error(f"Failed to connect to MQTT broker, return code {rc}")

    def on_mqtt_message(self, client, userdata, msg):
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            
            if not dbus_path:
                return

            value = None
            try:
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json:
                    value = float(incoming_json["value"])
                else:
                    return
            except json.JSONDecodeError:
                try:
                    value = float(payload_str)
                except ValueError:
                    return
            
            GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)
            
        except Exception as e:
            logger.error(f"Error processing MQTT message for TempSensor: {e}")
            
    def handle_dbus_change(self, path, value):
        section_name = f'Temp_Sensor_{self.device_index}'
        if path == '/CustomName':
            self.save_config_change(section_name, 'CustomName', value)
            return True
        elif path == '/TemperatureType':
            type_str = next((k for k, v in self.TEMPERATURE_TYPES.items() if v == value), 'generic')
            self.save_config_change(section_name, 'Type', type_str)
            return True
        return False

    def save_config_change(self, section, key, value):
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section):
                config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as configfile:
                config.write(configfile)
        except Exception as e:
            logger.error(f"Failed to save config file changes for TempSensor key '{key}': {e}")

    def update_dbus_from_mqtt(self, path, value):
        self[path] = value
        return False # Run once

# ====================================================================
# DbusTankSensor Class
# ====================================================================
class DbusTankSensor(VeDbusService):
    FLUID_TYPES = {
        'fuel': 0, 'fresh water': 1, 'waste water': 2, 'live well': 3, 'oil': 4,
        'black water': 5, 'gasoline': 6, 'diesel': 7, 'lpg': 8, 'lng': 9,
        'hydraulic oil': 10, 'raw water': 11
    }

    def __init__(self, service_name, device_config, serial_number, mqtt_config):
        super().__init__(service_name, register=False)
        self.device_config = device_config
        self.device_index = device_config.getint('DeviceIndex')

        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.19')
        self.add_path('/Mgmt/Connection', 'Virtual')
        
        self.add_path('/DeviceInstance', self.device_config.getint('DeviceInstance'))
        self.add_path('/ProductId', 49251)
        self.add_path('/ProductName', 'Virtual tank')
        self.add_path('/CustomName', self.device_config.get('CustomName'), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Serial', serial_number)
        
        self.add_path('/Status', 0)
        self.add_path('/Connected', 1)

        self.add_path('/Capacity', self.device_config.getfloat('Capacity', 0.2), writeable=True, onchangecallback=self.handle_dbus_change)
        
        initial_fluid_type_str = self.device_config.get('FluidType', 'fresh water').lower()
        initial_fluid_type_int = self.FLUID_TYPES.get(initial_fluid_type_str, self.FLUID_TYPES['fresh water'])
        self.add_path('/FluidType', initial_fluid_type_int, writeable=True, onchangecallback=self.handle_dbus_change)
        
        self.add_path('/Level', 0.0)
        self.add_path('/Remaining', 0.0)
        self.add_path('/RawValue', 0.0)
        self.add_path('/RawValueEmpty', self.device_config.getfloat('RawValueEmpty', 0.0), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/RawValueFull', self.device_config.getfloat('RawValueFull', 0.0), writeable=True, onchangecallback=self.handle_dbus_change)
        
        # Other paths not yet implemented via MQTT
        self.add_path('/RawUnit', self.device_config.get('RawUnit', ''))
        self.add_path('/Shape', 0)
        self.add_path('/Temperature', 0.0)
        self.add_path('/BatteryVoltage', 0.0)

        # MQTT setup
        self.mqtt_client = None
        self.mqtt_config = mqtt_config
        self.dbus_path_to_state_topic_map = {}
        self.is_level_direct = False

        def is_valid_topic(topic):
            return topic and 'path/to/mqtt' not in topic

        level_topic = self.device_config.get('LevelStateTopic')
        raw_topic = self.device_config.get('RawValueStateTopic')

        if is_valid_topic(raw_topic):
            self.dbus_path_to_state_topic_map['/RawValue'] = raw_topic
        elif is_valid_topic(level_topic):
            self.is_level_direct = True
            self.dbus_path_to_state_topic_map['/Level'] = level_topic
        
        # Add other topics if they exist
        if is_valid_topic(self.device_config.get('TemperatureStateTopic')):
            self.dbus_path_to_state_topic_map['/Temperature'] = self.device_config.get('TemperatureStateTopic')
        if is_valid_topic(self.device_config.get('BatteryStateTopic')):
            self.dbus_path_to_state_topic_map['/BatteryVoltage'] = self.device_config.get('BatteryStateTopic')

        self.setup_mqtt_client()
        self.register()
        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' registered on D-Bus.") 

        if not self.is_level_direct:
            self._calculate_level_from_raw_value()
        self._calculate_remaining_from_level()

    def setup_mqtt_client(self, retry_interval=5, max_retries=12):
        self.mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=self['/Serial'])
        if self.mqtt_config.get('Username'):
            self.mqtt_client.username_pw_set(self.mqtt_config.get('Username'), self.mqtt_config.get('Password'))
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message
        retries = 0
        while retries < max_retries:
            try:
                self.mqtt_client.connect(self.mqtt_config.get('BrokerAddress'), self.mqtt_config.getint('Port', 1883), 60)
                self.mqtt_client.loop_start()
                return
            except Exception as e:
                time.sleep(retry_interval)
                retries += 1
        sys.exit(1)

    def on_mqtt_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            for dbus_path, topic in self.dbus_path_to_state_topic_map.items():
                if topic: client.subscribe(topic)
        else:
            logger.error(f"Failed to connect to MQTT broker, return code {rc}")

    def on_mqtt_message(self, client, userdata, msg):
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            if not dbus_path: return

            value = None
            try:
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json:
                    value = float(incoming_json["value"])
            except json.JSONDecodeError:
                try: value = float(payload_str)
                except ValueError: return
            
            if value is None: return

            if dbus_path == '/RawValue' and not self.is_level_direct:
                GLib.idle_add(self._update_raw_value_and_recalculate, value)
            elif dbus_path == '/Level' and self.is_level_direct:
                GLib.idle_add(self._update_level_and_recalculate, value)
            else:
                GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)

        except Exception as e:
            logger.error(f"Error processing MQTT message for TankSensor: {e}")

    def _update_raw_value_and_recalculate(self, raw_value):
        self['/RawValue'] = raw_value
        self._calculate_level_from_raw_value()
        self._calculate_remaining_from_level()
        return False

    def _update_level_and_recalculate(self, level_value):
        if 0.0 <= level_value <= 100.0:
            self['/Level'] = round(level_value, 2)
            self._calculate_remaining_from_level()
        return False

    def _calculate_level_from_raw_value(self):
        raw_value = self['/RawValue']
        raw_empty = self['/RawValueEmpty']
        raw_full = self['/RawValueFull']
        level = 0.0
        if raw_full != raw_empty:
            level = ((raw_value - raw_empty) / (raw_full - raw_empty)) * 100.0
            level = max(0.0, min(100.0, level))
        self['/Level'] = round(level, 2)

    def _calculate_remaining_from_level(self):
        remaining = (self['/Level'] / 100.0) * self['/Capacity']
        self['/Remaining'] = round(remaining, 2)

    def handle_dbus_change(self, path, value):
        section_name = f'Tank_Sensor_{self.device_index}'
        key_name = path.split('/')[-1]
        self.save_config_change(section_name, key_name, value)

        if path in ['/RawValueEmpty', '/RawValueFull'] and not self.is_level_direct:
            GLib.idle_add(self._calculate_level_from_raw_value)
            GLib.idle_add(self._calculate_remaining_from_level)
        elif path in ['/Capacity', '/FluidType']:
            GLib.idle_add(self._calculate_remaining_from_level)
        
        return True

    def save_config_change(self, section, key, value):
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section): config.add_section(section)
            
            if key == 'FluidType':
                 value = next((k for k, v in self.FLUID_TYPES.items() if v == value), 'fresh water')

            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as f:
                config.write(f)
        except Exception as e:
            logger.error(f"Failed to save config change for TankSensor: {e}")

    def update_dbus_from_mqtt(self, path, value):
        self[path] = value
        return False

# ====================================================================
# DbusBattery Class
# ====================================================================
class DbusBattery(VeDbusService):
    def __init__(self, service_name, device_config, serial_number, mqtt_config):
        super().__init__(service_name, register=False)
        self.device_config = device_config
        self.device_index = device_config.getint('DeviceIndex')

        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.19')
        self.add_path('/Mgmt/Connection', 'Virtual')
        
        self.add_path('/DeviceInstance', self.device_config.getint('DeviceInstance'))
        self.add_path('/ProductId', 49253)
        self.add_path('/ProductName', 'Virtual battery')
        self.add_path('/CustomName', self.device_config.get('CustomName'), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Serial', serial_number)
        
        self.add_path('/Connected', 1)
        self.add_path('/Soc', 0.0)
        self.add_path('/Soh', 100.0)
        self.add_path('/Capacity', self.device_config.getfloat('CapacityAh'), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Dc/0/Current', 0.0)
        self.add_path('/Dc/0/Power', 0.0)
        self.add_path('/Dc/0/Temperature', 25.0)
        self.add_path('/Dc/0/Voltage', 0.0)
        
        # Other paths
        self.add_path('/ErrorCode', 0)
        self.add_path('/Info/MaxChargeCurrent', None)
        self.add_path('/Info/MaxDischargeCurrent', None)
        self.add_path('/Info/MaxChargeVoltage', None)

        self.mqtt_client = None
        self.mqtt_config = mqtt_config
        
        self.dbus_path_to_state_topic_map = {
            '/Dc/0/Current': self.device_config.get('CurrentStateTopic'),
            '/Dc/0/Power': self.device_config.get('PowerStateTopic'),
            '/Dc/0/Temperature': self.device_config.get('TemperatureStateTopic'),
            '/Dc/0/Voltage': self.device_config.get('VoltageStateTopic'),
            '/Soc': self.device_config.get('SocStateTopic'),
            '/Soh': self.device_config.get('SohStateTopic'),
        }
        self.dbus_path_to_state_topic_map = {k: v for k, v in self.dbus_path_to_state_topic_map.items() if v and 'path/to/mqtt' not in v}
        
        self.setup_mqtt_client()
        self.register()
        # FIX: Changed self.get_value('/CustomName') to self['/CustomName']
        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' registered on D-Bus.")

    def setup_mqtt_client(self, retry_interval=5, max_retries=12):
        self.mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=self['/Serial'])
        if self.mqtt_config.get('Username'):
            self.mqtt_client.username_pw_set(self.mqtt_config.get('Username'), self.mqtt_config.get('Password'))
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message
        retries = 0
        while retries < max_retries:
            try:
                self.mqtt_client.connect(self.mqtt_config.get('BrokerAddress'), self.mqtt_config.getint('Port', 1883), 60)
                self.mqtt_client.loop_start()
                return
            except Exception as e:
                time.sleep(retry_interval)
                retries += 1
        sys.exit(1)

    def on_mqtt_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            for dbus_path, topic in self.dbus_path_to_state_topic_map.items():
                if topic: client.subscribe(topic)
        else:
            logger.error(f"Failed to connect to MQTT broker, return code {rc}")

    def on_mqtt_message(self, client, userdata, msg):
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            if not dbus_path: return

            value = None
            try:
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json:
                    value = incoming_json["value"]
            except json.JSONDecodeError:
                try: value = float(payload_str)
                except ValueError: return
            
            if value is None: return
            
            GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)
            
        except Exception as e:
            logger.error(f"Error processing MQTT message for Battery: {e}")

    def handle_dbus_change(self, path, value):
        section_name = f'Virtual_Battery_{self.device_index}'
        if path == '/CustomName':
            self.save_config_change(section_name, 'CustomName', value)
            return True
        elif path == '/Capacity':
            self.save_config_change(section_name, 'CapacityAh', value)
            return True
        return False

    def save_config_change(self, section, key, value):
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section): config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as f:
                config.write(f)
        except Exception as e:
            logger.error(f"Failed to save config change for Battery: {e}")
            
    def update_dbus_from_mqtt(self, path, value):
        self[path] = value
        return False


# ====================================================================
# Main Service Runner
# ====================================================================
def run_device_service(device_type, config_section_name):
    from dbus.mainloop.glib import DBusGMainLoop
    DBusGMainLoop(set_as_default=True)
    
    logger.info(f"Starting D-Bus service process for {device_type} defined in [{config_section_name}].")

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE_PATH)
        
    log_level = logging.INFO
    if config.has_section('Global'):
        log_level_str = config['Global'].get('LogLevel', 'INFO').upper()
        log_level = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR}.get(log_level_str, logging.INFO)
    logger.setLevel(log_level)

    if not config.has_section(config_section_name):
        logger.critical(f"Configuration section '{config_section_name}' not found. Cannot start.")
        sys.exit(1)
        
    device_config = config[config_section_name]
    mqtt_config = config['MQTT'] if config.has_section('MQTT') else {}

    device_index_match = re.search(r'_(\d+)', config_section_name)
    device_index = device_index_match.group(1) if device_index_match else '0'
    device_config['DeviceIndex'] = device_index
    
    serial_number = device_config.get('Serial')
    if not serial_number:
        logger.critical(f"Serial number not found or is empty for [{config_section_name}]. Exiting. Please run setup.")
        sys.exit(1)

    if device_type == 'switch':
        mqtt_on_state = device_config.get('mqtt_on_state_payload', '1')
        mqtt_off_state = device_config.get('mqtt_off_state_payload', '0')
        mqtt_on_cmd = device_config.get('mqtt_on_command_payload', '1')
        mqtt_off_cmd = device_config.get('mqtt_off_command_payload', '0')
        
        num_switches = device_config.getint('NumberOfSwitches', 1)
        output_configs = []
        for j in range(1, num_switches + 1):
            output_section = f'switch_{device_index}_{j}'
            output_data = {'index': j, 'name': f'Switch {j}'}
            if config.has_section(output_section):
                output_settings = config[output_section]
                output_data.update({
                    'custom_name': output_settings.get('CustomName', ''),
                    'group': output_settings.get('Group', ''),
                    'MqttStateTopic': output_settings.get('MqttStateTopic'),
                    'MqttCommandTopic': output_settings.get('MqttCommandTopic')
                })
            output_configs.append(output_data)

        service_name = f'com.victronenergy.switch.virtual_{serial_number}'
        DbusSwitch(service_name, device_config, output_configs, serial_number, mqtt_config,
                   mqtt_on_state, mqtt_off_state, mqtt_on_cmd, mqtt_off_cmd)

    elif device_type == 'digital_input':
        service_name = f'com.victronenergy.digitalinput.virtual_{serial_number}'
        DbusDigitalInput(service_name, device_config, serial_number, mqtt_config)
    
    elif device_type == 'temp_sensor':
        service_name = f'com.victronenergy.temperature.virtual_{serial_number}'
        DbusTempSensor(service_name, device_config, serial_number, mqtt_config)

    elif device_type == 'tank_sensor':
        service_name = f'com.victronenergy.tank.virtual_{serial_number}'
        DbusTankSensor(service_name, device_config, serial_number, mqtt_config)
    
    elif device_type == 'battery':
        service_name = f'com.victronenergy.battery.virtual_{serial_number}'
        DbusBattery(service_name, device_config, serial_number, mqtt_config)
        
    else:
        logger.critical(f"Unknown device type: {device_type}")
        sys.exit(1)

    logger.debug('Connected to D-Bus, and switching over to GLib.MainLoop() (= event based)')
    mainloop = GLib.MainLoop()
    mainloop.run()


# ====================================================================
# Main Launcher
# ====================================================================
def main():
    logger.info("Starting D-Bus Virtual Devices service launcher.")
    config = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE_PATH):
        logger.critical(f"Config file not found: {CONFIG_FILE_PATH}")
        sys.exit(1)
    
    try:
        config.read(CONFIG_FILE_PATH)
    except configparser.Error as e:
        logger.critical(f"Error parsing config file: {e}")
        sys.exit(1)
    
    log_level = logging.INFO
    if config.has_section('Global'):
        log_level_str = config['Global'].get('LogLevel', 'INFO').upper()
        log_level = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR}.get(log_level_str, logging.INFO)
    logger.setLevel(log_level)

    processes = []
    script_path = os.path.abspath(sys.argv[0])

    device_type_map = {
        'relay_module_': 'switch',
        'temp_sensor_': 'temp_sensor',
        'tank_sensor_': 'tank_sensor',
        'virtual_battery_': 'battery',
        'input_': 'digital_input'
    }

    for section in config.sections():
        section_lower = section.lower()
        for prefix, device_type in device_type_map.items():
            if section_lower.startswith(prefix):
                logger.info(f"Found device '{section}' of type '{device_type}'. Starting process...")
                cmd = [sys.executable, script_path, device_type, section]
                try:
                    process = subprocess.Popen(cmd, env=os.environ, close_fds=True)
                    processes.append(process)
                except Exception as e:
                    logger.error(f"Failed to start process for [{section}]: {e}")
                break

    if not processes:
        logger.warning("No device sections found in the config file. Nothing to start.")

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
