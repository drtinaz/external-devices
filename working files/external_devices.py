#!/usr/bin/env python3

from gi.repository import GLib
import logging
import sys
import os
import random
import configparser
import time
import paho.mqtt.client as mqtt
import threading
import json
import re # Import regex module
import dbus.bus # Explicitly import dbus.bus for BusConnection
import traceback # Import traceback for detailed error logging

logger = logging.getLogger()

for handler in logger.handlers[:]:
    logger.removeHandler(handler)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG) # Default to DEBUG for better visibility

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
    def __init__(self, service_name, device_config, output_configs, serial_number, mqtt_client,
                 mqtt_on_state_payload, mqtt_off_state_payload, mqtt_on_command_payload, mqtt_off_command_payload, bus):
        # Pass the bus instance to the parent constructor
        super().__init__(service_name, bus=bus, register=False) 

        self.service_name = service_name # Store service_name for logging
        self.device_config = device_config
        self.device_index = device_config.getint('DeviceIndex') # This 'DeviceIndex' now comes from either Relay_Module_X or switch_X_Y
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
            # Fix: Use mqtt_off_state_payload instead of undefined mqtt_off_payload
            parsed_off = json.loads(mqtt_off_state_payload)
            if isinstance(parsed_off, dict) and len(parsed_off) == 1:
                self.mqtt_off_state_payload_json = parsed_off
        except json.JSONDecodeError:
            pass

        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.19')
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

        # Use the global MQTT client passed in
        self.mqtt_client = mqtt_client

        self.dbus_path_to_state_topic_map = {}
        self.dbus_path_to_command_topic_map = {}

        for output_data in output_configs:
            self.add_output(output_data)

        self.register() # Register all D-Bus paths at once
        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' registered on D-Bus.")

        # Now, after registration, add specific MQTT callbacks and subscribe
        for dbus_path, topic in self.dbus_path_to_state_topic_map.items():
            if topic:
                self.mqtt_client.message_callback_add(topic, self.on_mqtt_message_specific)
                self.mqtt_client.subscribe(topic)
                logger.debug(f"Added specific MQTT callback and subscribed for DbusSwitch '{self['/CustomName']}' on topic: {topic}")

    def add_output(self, output_data):
        # Construct the output prefix for D-Bus paths
        output_prefix = f'/SwitchableOutput/output_{output_data["index"]}'
        state_topic = output_data.get('MqttStateTopic')
        command_topic = output_data.get('MqttCommandTopic')
        dbus_state_path = f'{output_prefix}/State'

        if state_topic and 'path/to/mqtt' not in state_topic and command_topic and 'path/to/mqtt' not in command_topic:
            self.dbus_path_to_state_topic_map[dbus_state_path] = state_topic
            self.dbus_path_to_command_topic_map[dbus_state_path] = command_topic
            # DO NOT SUBSCRIBE OR ADD CALLBACK HERE YET
            # The subscription and callback addition is moved to __init__ after self.register()
        else:
            logger.warning(f"MQTT topics for {dbus_state_path} in DbusSwitch are invalid. Ignoring.")

        self.add_path(f'{output_prefix}/Name', output_data['name'])
        self.add_path(f'{output_prefix}/Status', 0)
        self.add_path(dbus_state_path, 0, writeable=True, onchangecallback=self.handle_dbus_change)
        settings_prefix = f'{output_prefix}/Settings'
        self.add_path(f'{settings_prefix}/CustomName', output_data['custom_name'], writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path(f'{settings_prefix}/Group', output_data['group'], writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path(f'{settings_prefix}/Type', 1, writeable=True)
        self.add_path(f'{settings_prefix}/ValidTypes', 7)

    # ... (rest of DbusSwitch class methods) ...
    def on_mqtt_message_specific(self, client, userdata, msg):
        # THIS IS A KEY DEBUG POINT - IF YOU DON'T SEE THIS, MESSAGES AREN'T REACHING HERE
        logger.debug(f"DbusSwitch specific MQTT callback triggered for {self['/CustomName']} on topic '{msg.topic}'")
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
                if new_state is None: # Fallback if JSON key/value not matched, try value in JSON as string
                    processed_payload_value = str(incoming_json.get("value", payload_str)).lower()
            except json.JSONDecodeError:
                # If not JSON, process as raw string
                processed_payload_value = payload_str.lower()
            
            if new_state is None: # If not determined by JSON parsing, try raw string matching
                if processed_payload_value == self.mqtt_on_state_payload_raw.lower():
                    new_state = 1
                elif processed_payload_value == self.mqtt_off_state_payload_raw.lower():
                    new_state = 0
                else:
                    logger.warning(f"DbusSwitch: Unrecognized payload '{payload_str}' for topic '{topic}'. Expected '{self.mqtt_on_state_payload_raw}' or '{self.mqtt_off_state_payload_raw}'.")
                    return # Exit if state not determined

            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            if dbus_path and self[dbus_path] != new_state:
                logger.debug(f"DbusSwitch: Updating D-Bus path '{dbus_path}' to {new_state} for '{self['/CustomName']}'.")
                GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, new_state)
            elif dbus_path:
                logger.debug(f"DbusSwitch: D-Bus path '{dbus_path}' already {new_state}. No update needed.")

        except Exception as e:
            logger.error(f"Error processing MQTT message for DbusSwitch {self.service_name} on topic {msg.topic}: {e}")
            traceback.print_exc() # Print full traceback for errors

    def handle_dbus_change(self, path, value):
        # Determine the correct section name for saving config
        if "/SwitchableOutput/output_" in path:
            try:
                # Extract output index from path (e.g., /SwitchableOutput/output_1/State -> 1)
                match = re.search(r'/output_(\d+)/', path)
                output_index = match.group(1) if match else None
                if output_index is None:
                    logger.error(f"Failed to parse output index from D-Bus path: {path}")
                    return False

                # This instance represents a Relay_Module, saving to its child switch_X_Y section
                parent_device_index = self.device_config.get('DeviceIndex') # From Relay_Module_X
                section_name = f'switch_{parent_device_index}_{output_index}'
                
                key_name = path.split('/')[-1]

                if "/State" in path:
                    if value in [0, 1]:
                        self.publish_mqtt_command(path, value)
                        # State is not saved to optionsSet normally, it's dynamic
                        return True
                    return False
                elif "/Settings" in path:
                    self.save_config_change(section_name, key_name, value)
                    return True
            except Exception as e:
                logger.error(f"Error handling D-Bus change for switch output {path}: {e}")
                traceback.print_exc()
                return False
        elif path == '/CustomName':
            # This handles the CustomName of the main DbusSwitch service itself (the Relay_Module)
            # The section name to save to is the one that created this service.
            self.save_config_change(self.device_config.name, 'CustomName', value)
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
            logger.info(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config file changes for key '{key}': {e}")
            traceback.print_exc()

    def publish_mqtt_command(self, path, value):
        if not self.mqtt_client or not self.mqtt_client.is_connected():
            logger.warning(f"MQTT client not connected, cannot publish command for {self.service_name}.")
            return
        if path not in self.dbus_path_to_command_topic_map:
            logger.warning(f"No command topic mapped for D-Bus path '{path}' in {self.service_name}.")
            return
        try:
            command_topic = self.dbus_path_to_command_topic_map[path]
            mqtt_payload = self.mqtt_on_command_payload if value == 1 else self.mqtt_off_command_payload
            self.mqtt_client.publish(command_topic, mqtt_payload, retain=False)
            logger.debug(f"Published MQTT command '{mqtt_payload}' to topic '{command_topic}' for {self.service_name}.")
        except Exception as e:
            logger.error(f"Error during MQTT publish for {self.service_name}: {e}")
            traceback.print_exc()

    def update_dbus_from_mqtt(self, path, value):
        try:
            if self[path] != value:
                self[path] = value
                logger.debug(f"DbusSwitch: D-Bus path '{path}' updated to {value}.")
        except Exception as e:
            logger.error(f"Error updating D-Bus path '{path}' in DbusSwitch: {e}")
            traceback.print_exc()
        return False # Run only once

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

    def __init__(self, service_name, device_config, serial_number, mqtt_client, bus):
        # Pass the bus instance to the parent constructor
        super().__init__(service_name, bus=bus, register=False)

        self.device_config = device_config
        # The section name itself (e.g., 'input_1_1') is used for saving
        self.config_section_name = device_config.name 
        self.service_name = service_name # Store service_name for logging

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

        # Use the global MQTT client passed in
        self.mqtt_client = mqtt_client

        self.mqtt_state_topic = self.device_config.get('MqttStateTopic')
        self.mqtt_on_payload = self.device_config.get('mqtt_on_state_payload', 'ON')
        self.mqtt_off_payload = self.device_config.get('mqtt_off_state_payload', 'OFF')

        self.register() # Register D-Bus paths before subscribing to MQTT

        # Add this device's specific message callback to the global client after D-Bus registration
        if self.mqtt_state_topic and 'path/to/mqtt' not in self.mqtt_state_topic:
            self.mqtt_client.message_callback_add(self.mqtt_state_topic, self.on_mqtt_message_specific)
            self.mqtt_client.subscribe(self.mqtt_state_topic) # Explicitly subscribe here
            logger.debug(f"Added specific MQTT callback and subscribed for DbusDigitalInput '{self['/CustomName']}' on topic: {self.mqtt_state_topic}")
        else:
            logger.warning(f"No valid MqttStateTopic for '{self['/CustomName']}'. State will not update from MQTT.")

        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' registered on D-Bus.")

    # Specific message handler for this digital input
    def on_mqtt_message_specific(self, client, userdata, msg):
        # THIS IS A KEY DEBUG POINT - IF YOU DON'T SEE THIS, MESSAGES AREN'T REACHING HERE
        logger.debug(f"DbusDigitalInput specific MQTT callback triggered for {self['/CustomName']} on topic '{msg.topic}'")
        
        if msg.topic != self.mqtt_state_topic:
            logger.debug(f"DbusDigitalInput: Received message on non-matching topic '{msg.topic}'. Expected '{self.mqtt_state_topic}'.")
            return
        
        try:
            payload_str = msg.payload.decode().strip()
            logger.debug(f"DbusDigitalInput: Received MQTT message on topic '{msg.topic}': {payload_str}")

            raw_state = None
            if payload_str.lower() == self.mqtt_on_payload.lower():
                raw_state = 1
            elif payload_str.lower() == self.mqtt_off_payload.lower():
                raw_state = 0
            
            if raw_state is None:
                logger.warning(f"DbusDigitalInput: Invalid MQTT payload '{payload_str}' received for '{self['/CustomName']}'. Expected '{self.mqtt_on_payload}' or '{self.mqtt_off_payload}'.")
                return

            # InputState always reflects the actual (raw) state
            if self['/InputState'] != raw_state:
                logger.debug(f"DbusDigitalInput: Updating /InputState for '{self['/CustomName']}' to {raw_state}")
                GLib.idle_add(self.update_dbus_input_state, raw_state)

            # Apply inversion for the main State D-Bus path
            invert = self['/Settings/InvertTranslation']
            final_state = (1 - raw_state) if invert == 1 else raw_state

            # Get the D-Bus State value based on the Type setting
            dbus_state = self._get_dbus_state_for_type(final_state)

            # Schedule D-Bus update for the main State in main thread
            if self['/State'] != dbus_state:
                logger.debug(f"DbusDigitalInput: Updating /State for '{self['/CustomName']}' to {dbus_state}")
                GLib.idle_add(self.update_dbus_state, dbus_state)

        except Exception as e:
            logger.error(f"Error processing MQTT message for Digital Input {self.service_name} on topic {msg.topic}: {e}")
            traceback.print_exc()

    def _get_dbus_state_for_type(self, logical_state):
        """
        Maps the logical state (0 or 1) to the specific D-Bus State value
        based on the currently configured Type.
        """
        current_type = self['/Type']
        
        if current_type == 2:  # 'door alarm'
            return 7 if logical_state == 1 else 6  # 7=alarm, 6=normal
        elif current_type == 3: # 'bilge pump' or 'generic'
            return 3 if logical_state == 1 else 2  # 3=on, 2=off
        elif 4 <= current_type <= 8: # bilge alarm, burglar alarm, smoke alarm, fire alarm, co2 alarm
            return 9 if logical_state == 1 else 8  # 9=alarm, 8=normal
        
        # For other types (disabled, pulse meter, generator, touch input control, or unmapped),
        # return the logical state directly (0 or 1)
        return logical_state

    def update_dbus_input_state(self, new_raw_state):
        self['/InputState'] = new_raw_state
        return False # Run only once

    def update_dbus_state(self, new_state_value):
        self['/State'] = new_state_value
        return False # Run only once

    def handle_dbus_change(self, path, value):
        try:
            key_name = path.split('/')[-1]
            logger.debug(f"D-Bus settings change triggered for {path} with value '{value}'. Saving to config file.")
            
            value_to_save = value
            if path == '/Type':
                value_to_save = next((name for name, num in self.DIGITAL_INPUT_TYPES.items() if num == value), 'generic')
            
            # Special handling for Alarm settings as they are under /Settings
            if path.startswith('/Settings/'):
                self.save_config_change(self.config_section_name, key_name, value)
                if path == '/Settings/InvertTranslation':
                    # Recalculate and update /State immediately when InvertTranslation changes
                    current_raw_state = self['/InputState']
                    new_invert_setting = value # 'value' is the new InvertTranslation setting (0 or 1)
                    final_state_after_inversion = (1 - current_raw_state) if new_invert_setting == 1 else current_raw_state
                    new_dbus_state_value = self._get_dbus_state_for_type(final_state_after_inversion)
                    GLib.idle_add(self.update_dbus_state, new_dbus_state_value)
            else: # For paths directly under the device root (CustomName, Count, State, Type)
                self.save_config_change(self.config_section_name, key_name, value_to_save)
            return True
        except Exception as e:
            logger.error(f"Failed to handle D-Bus change for {path}: {e}")
            traceback.print_exc()
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
            logger.info(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config file changes for key '{key}' in section '{section}': {e}")
            traceback.print_exc()

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

    def __init__(self, service_name, device_config, serial_number, mqtt_client, bus):
        # Pass the bus instance to the parent constructor
        super().__init__(service_name, bus=bus, register=False)

        self.device_config = device_config
        self.device_index = device_config.getint('DeviceIndex')
        self.service_name = service_name # Store service_name for logging

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

        # Use the global MQTT client passed in
        self.mqtt_client = mqtt_client
        
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

        self.register() # Register D-Bus paths before subscribing to MQTT

        # Add this device's specific message callbacks to the global client for each topic
        for dbus_path, topic in self.dbus_path_to_state_topic_map.items():
            if topic: # double check validity
                self.mqtt_client.message_callback_add(topic, self.on_mqtt_message_specific)
                self.mqtt_client.subscribe(topic) # Explicitly subscribe here
                logger.debug(f"Added specific MQTT callback and subscribed for DbusTempSensor '{self['/CustomName']}' on topic: {topic}")


        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' registered on D-Bus.")

    # Specific message handler for this temp sensor
    def on_mqtt_message_specific(self, client, userdata, msg):
        # THIS IS A KEY DEBUG POINT - IF YOU DON'T SEE THIS, MESSAGES AREN'T REACHING HERE
        logger.debug(f"DbusTempSensor specific MQTT callback triggered for {self['/CustomName']} on topic '{msg.topic}'")
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            
            if not dbus_path:
                logger.debug(f"DbusTempSensor: Received message on non-matching topic '{msg.topic}'. Not mapped for this sensor.")
                return

            value = None
            try:
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json:
                    value = float(incoming_json["value"])
                else:
                    logger.warning(f"DbusTempSensor: JSON payload for topic '{topic}' does not contain 'value' key or is not a dict.")
                    return
            except json.JSONDecodeError:
                try:
                    value = float(payload_str)
                except ValueError:
                    logger.warning(f"DbusTempSensor: Payload '{payload_str}' for topic '{topic}' is not valid float or JSON.")
                    return
            
            if value is None: 
                logger.warning(f"DbusTempSensor: Could not extract valid numerical value from payload '{payload_str}' for topic '{topic}'.")
                return
            
            if self[dbus_path] != value:
                logger.debug(f"DbusTempSensor: Updating D-Bus path '{dbus_path}' to {value} for '{self['/CustomName']}'.")
                GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)
            else:
                logger.debug(f"DbusTempSensor: D-Bus path '{dbus_path}' already {value}. No update needed.")

        except Exception as e:
            logger.error(f"Error processing MQTT message for TempSensor {self.service_name} on topic {msg.topic}: {e}")
            traceback.print_exc()
            
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
            logger.info(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config file changes for TempSensor key '{key}': {e}")
            traceback.print_exc()

    def update_dbus_from_mqtt(self, path, value):
        self[path] = value
        return False

# ====================================================================
# DbusTankSensor Class
# ====================================================================
class DbusTankSensor(VeDbusService):
    FLUID_TYPES = {
        'fuel': 0, 'fresh water': 1, 'waste water': 2, 'live well': 3, 'oil': 4,
        'black water': 5, 'gasoline': 6, 'diesel': 7, 'lpg': 8, 'lng': 9,
        'hydraulic oil': 10, 'raw water': 11
    }

    def __init__(self, service_name, device_config, serial_number, mqtt_client, bus):
        # Pass the bus instance to the parent constructor
        super().__init__(service_name, bus=bus, register=False)
        self.device_config = device_config
        self.device_index = device_config.getint('DeviceIndex')
        self.service_name = service_name # Store service_name for logging

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

        # Use the global MQTT client passed in
        self.mqtt_client = mqtt_client

        self.dbus_path_to_state_topic_map = {}
        self.is_level_direct = False

        def is_valid_topic(topic):
            return topic and 'path/to/mqtt' not in topic

        level_topic = self.device_config.get('LevelStateTopic')
        raw_topic = self.device_config.get('RawValueStateTopic')

        if is_valid_topic(raw_topic):
            self.dbus_path_to_state_topic_map['/RawValue'] = raw_topic
            logger.debug(f"Tank '{self['/CustomName']}' will use RawValue topic: {raw_topic}")
        elif is_valid_topic(level_topic):
            self.is_level_direct = True
            self.dbus_path_to_state_topic_map['/Level'] = level_topic
            logger.debug(f"Tank '{self['/CustomName']}' will use direct Level topic: {level_topic}")
        else:
            logger.warning(f"Tank '{self['/CustomName']}': Neither RawValueStateTopic nor LevelStateTopic are valid. Tank level will not update from MQTT.")
        
        # Add other topics if they exist
        if is_valid_topic(self.device_config.get('TemperatureStateTopic')):
            self.dbus_path_to_state_topic_map['/Temperature'] = self.device_config.get('TemperatureStateTopic')
            logger.debug(f"Tank '{self['/CustomName']}' also subscribing to Temperature topic: {self.device_config.get('TemperatureStateTopic')}")
        if is_valid_topic(self.device_config.get('BatteryStateTopic')):
            self.dbus_path_to_state_topic_map['/BatteryVoltage'] = self.device_config.get('BatteryStateTopic')
            logger.debug(f"Tank '{self['/CustomName']}' also subscribing to BatteryVoltage topic: {self.device_config.get('BatteryStateTopic')}")

        self.register() # Register D-Bus paths before subscribing to MQTT

        # Add this device's specific message callbacks to the global client for each topic
        for dbus_path, topic in self.dbus_path_to_state_topic_map.items():
            if topic: # double check validity
                self.mqtt_client.message_callback_add(topic, self.on_mqtt_message_specific)
                self.mqtt_client.subscribe(topic) # Explicitly subscribe here
                logger.debug(f"Added specific MQTT callback and subscribed for DbusTankSensor '{self['/CustomName']}' on topic: {topic}")


        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' registered on D-Bus.") 

        # Initial calculations
        if not self.is_level_direct:
            self._calculate_level_from_raw_value()
        self._calculate_remaining_from_level()

    # Specific message handler for this tank sensor
    def on_mqtt_message_specific(self, client, userdata, msg):
        # THIS IS A KEY DEBUG POINT - IF YOU DON'T SEE THIS, MESSAGES AREN'T REACHING HERE
        logger.debug(f"DbusTankSensor specific MQTT callback triggered for {self['/CustomName']} on topic '{msg.topic}'")
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            if not dbus_path: 
                logger.debug(f"DbusTankSensor: Received message on non-matching topic '{msg.topic}'. Not mapped for this sensor.")
                return

            value = None
            try:
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json:
                    value = float(incoming_json["value"])
                else:
                    logger.warning(f"DbusTankSensor: JSON payload for topic '{topic}' does not contain 'value' key or is not a dict.")
                    return
            except json.JSONDecodeError:
                try: value = float(payload_str)
                except ValueError: 
                    logger.warning(f"DbusTankSensor: Payload '{payload_str}' for topic '{topic}' is not valid float or JSON.")
                    return
            
            if value is None: 
                logger.warning(f"DbusTankSensor: Could not extract valid numerical value from payload '{payload_str}' for topic '{topic}'.")
                return
            
            if dbus_path == '/RawValue' and not self.is_level_direct:
                if self['/RawValue'] != value:
                    logger.debug(f"DbusTankSensor: Updating /RawValue to {value} and recalculating for '{self['/CustomName']}'.")
                    GLib.idle_add(self._update_raw_value_and_recalculate, value)
                else:
                    logger.debug(f"DbusTankSensor: /RawValue already {value}. No update needed.")
            elif dbus_path == '/Level' and self.is_level_direct:
                if 0.0 <= value <= 100.0 and self['/Level'] != round(value, 2):
                    logger.debug(f"DbusTankSensor: Updating /Level to {value} and recalculating for '{self['/CustomName']}'.")
                    GLib.idle_add(self._update_level_and_recalculate, value)
                else:
                    logger.debug(f"DbusTankSensor: /Level already {value} or value out of range. No update needed.")
            else: # For /Temperature or /BatteryVoltage
                if self[dbus_path] != value:
                    logger.debug(f"DbusTankSensor: Updating D-Bus path '{dbus_path}' to {value} for '{self['/CustomName']}'.")
                    GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)
                else:
                    logger.debug(f"DbusTankSensor: D-Bus path '{dbus_path}' already {value}. No update needed.")

        except Exception as e:
            logger.error(f"Error processing MQTT message for Tank {self.service_name} on topic {msg.topic}: {e}")
            traceback.print_exc()

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
        logger.debug(f"Tank '{self['/CustomName']}' calculated Level: {self['/Level']}")

    def _calculate_remaining_from_level(self):
        remaining = (self['/Level'] / 100.0) * self['/Capacity']
        self['/Remaining'] = round(remaining, 2)
        logger.debug(f"Tank '{self['/CustomName']}' calculated Remaining: {self['/Remaining']}")


    def handle_dbus_change(self, path, value):
        section_name = f'Tank_Sensor_{self.device_index}'
        key_name = path.split('/')[-1]
        
        value_to_save = value
        if key_name == 'FluidType':
            # Convert integer back to string for saving to config
            value_to_save = next((k for k, v in self.FLUID_TYPES.items() if v == value), 'fresh water')
            logger.debug(f"Tank: Converting FluidType {value} to string '{value_to_save}' for saving.")

        self.save_config_change(section_name, key_name, value_to_save)

        if path in ['/RawValueEmpty', '/RawValueFull'] and not self.is_level_direct:
            GLib.idle_add(self._calculate_level_from_raw_value)
            GLib.idle_add(self._calculate_remaining_from_level)
        elif path == '/Capacity': # Capacity also affects Remaining
            GLib.idle_add(self._calculate_remaining_from_level)
        
        return True

    def save_config_change(self, section, key, value):
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section): config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as f:
                config.write(f)
            logger.info(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config change for Tank: {e}")
            traceback.print_exc()

    def update_dbus_from_mqtt(self, path, value):
        self[path] = value
        return False

# ====================================================================
# DbusBattery Class
# ====================================================================
class DbusBattery(VeDbusService):
    def __init__(self, service_name, device_config, serial_number, mqtt_client, bus):
        # Pass the bus instance to the parent constructor
        super().__init__(service_name, bus=bus, register=False)
        self.device_config = device_config
        self.device_index = device_config.getint('DeviceIndex')
        self.service_name = service_name # Store service_name for logging

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

        # Use the global MQTT client passed in
        self.mqtt_client = mqtt_client
        
        self.dbus_path_to_state_topic_map = {
            '/Dc/0/Current': self.device_config.get('CurrentStateTopic'),
            '/Dc/0/Power': self.device_config.get('PowerStateTopic'),
            '/Dc/0/Temperature': self.device_config.get('TemperatureStateTopic'),
            '/Dc/0/Voltage': self.device_config.get('VoltageStateTopic'),
            '/Soc': self.device_config.get('SocStateTopic'),
            '/Soh': self.device_config.get('SohStateTopic'),
        }
        self.dbus_path_to_state_topic_map = {k: v for k, v in self.dbus_path_to_state_topic_map.items() if v and 'path/to/mqtt' not in v}
        
        self.register() # Register D-Bus paths before subscribing to MQTT

        # Add this device's specific message callbacks to the global client for each topic
        for dbus_path, topic in self.dbus_path_to_state_topic_map.items():
            if topic:
                self.mqtt_client.message_callback_add(topic, self.on_mqtt_message_specific)
                self.mqtt_client.subscribe(topic) # Explicitly subscribe here
                logger.debug(f"Added specific MQTT callback and subscribed for DbusBattery '{self['/CustomName']}' on topic: {topic}")


        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' registered on D-Bus.")

    # Specific message handler for this battery
    def on_mqtt_message_specific(self, client, userdata, msg):
        # THIS IS A KEY DEBUG POINT - IF YOU DON'T SEE THIS, MESSAGES AREN'T REACHING HERE
        logger.debug(f"DbusBattery specific MQTT callback triggered for {self['/CustomName']} on topic '{msg.topic}'")
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            if not dbus_path: 
                logger.debug(f"DbusBattery: Received message on non-matching topic '{msg.topic}'. Not mapped for this battery.")
                return

            value = None
            try:
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json:
                    value = incoming_json["value"]
                else:
                    logger.warning(f"DbusBattery: JSON payload for topic '{topic}' does not contain 'value' key or is not a dict.")
                    return
            except json.JSONDecodeError:
                try: value = float(payload_str)
                except ValueError: 
                    logger.warning(f"DbusBattery: Payload '{payload_str}' for topic '{topic}' is not valid float or JSON.")
                    return
            
            if value is None: 
                logger.warning(f"DbusBattery: Could not extract valid numerical value from payload '{payload_str}' for topic '{topic}'.")
                return
            
            if self[dbus_path] != value:
                logger.debug(f"DbusBattery: Updating D-Bus path '{dbus_path}' to {value} for '{self['/CustomName']}'.")
                GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)
            else:
                logger.debug(f"DbusBattery: D-Bus path '{dbus_path}' already {value}. No update needed.")
            
        except Exception as e:
            logger.error(f"Error processing MQTT message for Battery {self.service_name} on topic {msg.topic}: {e}")
            traceback.print_exc()

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
            logger.info(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config change for Battery: {e}")
            traceback.print_exc()
            
    def update_dbus_from_mqtt(self, path, value):
        self[path] = value
        return False


# ====================================================================
# Global MQTT Callbacks
# ====================================================================
# --- Original Global MQTT Connect Callback (renamed for clarity) ---
def on_mqtt_connect_original_global(client, userdata, flags, rc, properties):
    if rc == 0:
        logger.info("Connected to MQTT Broker!")
        # Subscriptions are handled by individual service instances' message_callback_add
        # or in the debug_on_connect if that's active.
    else:
        logger.error(f"Failed to connect to MQTT Broker, return code {rc}")
        # Note: If connection fails, the paho client will attempt to reconnect by default.
        # Consider more robust error handling / retry logic here if needed for critical systems.

# This global on_message callback is for debugging to ensure ANY message is received by the client.
# If messages appear here but not in device-specific on_mqtt_message_specific, then
# the issue is with how message_callback_add is being handled or topic filtering.
# --- Original Global MQTT Message Callback (renamed for clarity) ---
def on_mqtt_message_original_global(client, userdata, msg):
    logger.debug(f"GLOBAL MQTT MESSAGE RECEIVED: Topic='{msg.topic}', Payload='{msg.payload.decode()}'")

# --- ADDED: Global MQTT Disconnect Callback ---
def on_mqtt_disconnect(client, userdata, rc):
    logger.warning(f"MQTT client disconnected with result code: {rc}")
    # You might want to add re-connection logic here if 'loop_forever' is not used,
    # or if you need to handle specific disconnect codes.

# --- ADDED: Global MQTT Subscribe Callback ---
def on_mqtt_subscribe(client, userdata, mid, granted_qos, properties=None):
    logger.info(f"MQTT Subscription acknowledged by broker. Message ID: {mid}, Granted QoS: {granted_qos}")


# ====================================================================
# Main Launcher (Refactored to run all services in one process)
# ====================================================================
def main():
    logger.info("Starting D-Bus Virtual Devices main service.")
    
    # Setup GLib MainLoop for D-Bus
    from dbus.mainloop.glib import DBusGMainLoop
    DBusGMainLoop(set_as_default=True)

    config = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE_PATH):
        logger.critical(f"Config file not found: {CONFIG_FILE_PATH}")
        sys.exit(1)
    
    try:
        config.read(CONFIG_FILE_PATH)
    except configparser.Error as e:
        logger.critical(f"Error parsing config file: {e}")
        sys.exit(1)
    
    # Configure logging level based on config
    log_level = logging.INFO
    if config.has_section('Global'):
        log_level_str = config['Global'].get('LogLevel', 'INFO').upper()
        log_level = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR}.get(log_level_str, logging.INFO)
    logger.setLevel(log_level)

    # --- START DEBUGGING OVERRIDE: Force DEBUG level ---
    logger.setLevel(logging.DEBUG)
    logger.debug("DEBUG_OVERRIDE: Logging level forced to DEBUG for troubleshooting.")
    # --- END DEBUGGING OVERRIDE ---


    # --- Setup a single global MQTT client ---
    mqtt_config = config['MQTT'] if config.has_section('MQTT') else {}
    MQTT_HOST = mqtt_config.get('BrokerAddress', 'localhost')
    MQTT_PORT = mqtt_config.getint('Port', 1883)
    MQTT_USERNAME = mqtt_config.get('Username')
    MQTT_PASSWORD = mqtt_config.get('Password')

    client_id = f"external-devices-main-script-{os.getpid()}"
    logger.info(f"Using MQTT Client ID: {client_id}")
    # FIX: Corrected CallbackAPIVersion to start with an uppercase 'V'
    mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)

    # --- START DEBUGGING ADDITIONS: Specific callbacks for test topic ---
    TEST_MQTT_TOPIC = "venus-home/N/c0619ab1f730/battery/1/Dc/0/Power" # This is already a DbusBattery topic.

    def debug_on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            logger.info("DEBUG_TEST_CONNECT: Connected to MQTT Broker successfully!")
            # Explicitly subscribe to the test topic here to ensure it's always active
            client.subscribe(TEST_MQTT_TOPIC) 
            logger.info(f"DEBUG_TEST_CONNECT: Subscribed to '{TEST_MQTT_TOPIC}' for global monitoring.")
            on_mqtt_connect_original_global(client, userdata, flags, rc, properties)
        else:
            logger.error(f"DEBUG_TEST_CONNECT: Failed to connect, return code {rc}")

    # Re-enable the global on_message handler
    def debug_on_message(client, userdata, msg):
        logger.debug(f"DEBUG_TEST_MESSAGE: RAW MESSAGE (from main debug callback): Topic='{msg.topic}', Payload='{msg.payload.decode()}'")
        on_mqtt_message_original_global(client, userdata, msg)

    mqtt_client.on_connect = debug_on_connect
    mqtt_client.on_message = debug_on_message 
    mqtt_client.on_subscribe = on_mqtt_subscribe # <--- ADDED: Global on_subscribe callback
    # --- END DEBUGGING ADDITIONS ---

    mqtt_client.on_disconnect = on_mqtt_disconnect
    
    if MQTT_USERNAME and MQTT_PASSWORD:
        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        logger.info("MQTT Username/Password set.")
    
    # Connect and start MQTT client loop in a non-blocking way
    try:
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        mqtt_client.loop_start() # Start the MQTT network loop in a separate thread
        logger.info(f"Attempting to connect to MQTT broker at {MQTT_HOST}:{MQTT_PORT}...")
    except Exception as e:
        logger.critical(f"Initial connection to MQTT broker failed: {e}. Exiting.")
        traceback.print_exc()
        sys.exit(1)

    active_services = [] # List to hold references to our D-Bus services

    device_type_map = {
        'relay_module_': DbusSwitch, # This is the only prefix for a DbusSwitch parent
        'temp_sensor_': DbusTempSensor,
        'tank_sensor_': DbusTankSensor,
        'virtual_battery_': DbusBattery,
        'input_': DbusDigitalInput
    }

    sections_to_process = []
    for section in config.sections():
        section_lower = section.lower()
        if section_lower in ['global', 'mqtt']:
            continue
        # RE-ENABLED: This correctly skips [switch_X_Y] sections from being processed as top-level devices
        if section_lower.startswith('switch_') and re.match(r'^switch_\d+_\d+$', section_lower):
            logger.debug(f"Section '{section}' appears to be a switch output configuration. It will be processed by its parent Relay_Module. Skipping direct device creation.")
            continue
        sections_to_process.append(section)


    for section in sections_to_process:
        section_lower = section.lower()
        device_class = None
        device_type_string = None 

        for prefix, cls in device_type_map.items():
            if section_lower.startswith(prefix): # This is the key comparison for identifying the device type
                device_class = cls
                device_type_string = prefix.strip('_')
                logger.debug(f"Section '{section}' matched device type '{device_type_string}' (prefix '{prefix}').")
                break
        
        if device_class:
            try:
                device_config = config[section]
                
                # Determine device_index for the current section
                device_index_match = re.search(r'_(\d+)', section)
                device_index = device_index_match.group(1) if device_index_match else '0'
                device_config['DeviceIndex'] = device_index # Inject DeviceIndex into config for class access

                serial_number = device_config.get('Serial')
                if not serial_number:
                    # Generate a random serial number if not provided
                    serial_number = str(random.randint(1000000000000000, 9999999999999999))
                    logger.warning(f"Serial number not found or is empty for [{section}]. Generating random serial: {serial_number}")
                    # Optionally, save this back to config, but for a virtual device, usually not critical.

                # IMPORTANT: Create a NEW, independent BusConnection instance for each service
                # This ensures each service gets its own D-Bus name.
                device_bus = dbus.bus.BusConnection(dbus.Bus.TYPE_SYSTEM)
                
                # Default service name uses 'external_' prefix and device type
                base_service_name_type = device_type_string.replace("_", "")
                if base_service_name_type == 'relaymodule': base_service_name_type = 'switch' # Special case for service name
                elif base_service_name_type == 'input': base_service_name_type = 'digitalinput' # Special case
                # START MODIFICATION (Previous fix for temp/tank, now adding battery)
                elif base_service_name_type == 'tanksensor': base_service_name_type = 'tank'
                elif base_service_name_type == 'tempsensor': base_service_name_type = 'temperature'
                elif base_service_name_type == 'virtualbattery': base_service_name_type = 'battery' # NEW FIX FOR BATTERY
                # END MODIFICATION
                
                service_name = f'com.victronenergy.{base_service_name_type}.external_{serial_number}'

                if device_class == DbusSwitch:
                    # This branch is now ONLY for Relay_Module_X sections (multi-output switch modules)
                    output_configs = []
                    
                    mqtt_on_state = device_config.get('mqtt_on_state_payload', '1')
                    mqtt_off_state = device_config.get('mqtt_off_state_payload', '0')
                    mqtt_on_cmd = device_config.get('mqtt_on_command_payload', '1')
                    mqtt_off_cmd = device_config.get('mqtt_off_command_payload', '0')

                    num_switches = device_config.getint('NumberOfSwitches', 1)
                    for j in range(1, num_switches + 1):
                        output_section_name = f'switch_{device_index}_{j}' # e.g., switch_1_1, switch_1_2
                        output_data = {'index': j, 'name': f'Switch {j}'} # Default name
                        if config.has_section(output_section_name):
                            output_settings = config[output_section_name]
                            output_data.update({
                                'custom_name': output_settings.get('CustomName', ''),
                                'group': output_settings.get('Group', ''),
                                'MqttStateTopic': output_settings.get('MqttStateTopic'),
                                'MqttCommandTopic': output_settings.get('MqttCommandTopic')
                            })
                            logger.debug(f"Found and added output config for {output_section_name} to Relay_Module_{device_index}.")
                        else:
                            logger.warning(f"Expected switch output section '{output_section_name}' for Relay_Module_{device_index} not found. Skipping this output.")
                        output_configs.append(output_data)
                    
                    # The service name for a Relay_Module is based on its serial.
                    service = device_class(service_name, device_config, output_configs, serial_number, mqtt_client,
                                        mqtt_on_state, mqtt_off_state, mqtt_on_cmd, mqtt_off_cmd, device_bus)
                else: # For all other device types (DigitalInput, TempSensor, TankSensor, Battery)
                    service = device_class(service_name, device_config, serial_number, mqtt_client, device_bus)
                
                active_services.append(service)
                logger.info(f"Successfully initialized and registered D-Bus service for [{section}] of type '{device_type_string}'.")

            except Exception as e:
                logger.error(f"Failed to initialize D-Bus service for [{section}] ({device_type_string}): {e}")
                traceback.print_exc()
        else:
            logger.warning(f"Section '{section}' does not match any known device type prefix. Skipping.")

    if not active_services:
        logger.warning("No device services were started. Exiting.")
        if mqtt_client:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        sys.exit(0)

    logger.info('All identified external device services created. Starting GLib.MainLoop(). Press Ctrl+C to exit.')
    
    # Keep the main loop running to maintain D-Bus services and MQTT client
    mainloop = GLib.MainLoop()
    try:
        mainloop.run()
    except KeyboardInterrupt:
        logger.info("Exiting D-Bus Virtual Devices main service.")
    except Exception as e:
        logger.error(f"An unexpected error occurred in the main loop: {e}")
        traceback.print_exc()
    finally:
        # Cleanup: Disconnect MQTT client cleanly
        if mqtt_client:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            logger.info("MQTT client disconnected.")
        logger.info("Script finished.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        logger.warning("Command line arguments for device type/section are deprecated in this version. Running main launcher directly.")
    main()
