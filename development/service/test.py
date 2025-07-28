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

# A helper function to check if a topic is valid
def is_valid_topic(topic):
    return topic and topic.strip() and 'path/to/mqtt' not in topic

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

        if is_valid_topic(state_topic) and is_valid_topic(command_topic):
            self.dbus_path_to_state_topic_map[dbus_state_path] = state_topic
            self.dbus_path_to_command_topic_map[dbus_state_path] = command_topic
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

    def on_mqtt_message_specific(self, client, userdata, msg):
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
                    logger.warning(f"DbusSwitch: Unrecognized payload '{payload_str}' for topic '{topic}'.")
                    return

            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            if dbus_path and self[dbus_path] != new_state:
                GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, new_state)

        except Exception as e:
            logger.error(f"Error processing MQTT message for DbusSwitch {self.service_name} on topic {msg.topic}: {e}")
            traceback.print_exc()

    def handle_dbus_change(self, path, value):
        if "/SwitchableOutput/output_" in path:
            try:
                match = re.search(r'/output_(\d+)/', path)
                if not match: return False
                output_index = match.group(1)
                parent_device_index = self.device_config.get('DeviceIndex')
                section_name = f'switch_{parent_device_index}_{output_index}'
                key_name = path.split('/')[-1]

                if "/State" in path:
                    if value in [0, 1]: self.publish_mqtt_command(path, value)
                    return True
                elif "/Settings" in path:
                    self.save_config_change(section_name, key_name, value)
                    return True
            except Exception as e:
                logger.error(f"Error handling D-Bus change for switch output {path}: {e}")
                return False
        elif path == '/CustomName':
            self.save_config_change(self.device_config.name, 'CustomName', value)
            return True
        return False

    def save_config_change(self, section, key, value):
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section): config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as configfile:
                config.write(configfile)
            logger.info(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config file changes for key '{key}': {e}")

    def publish_mqtt_command(self, path, value):
        if not self.mqtt_client or not self.mqtt_client.is_connected(): return
        if path not in self.dbus_path_to_command_topic_map: return
        try:
            command_topic = self.dbus_path_to_command_topic_map[path]
            mqtt_payload = self.mqtt_on_command_payload if value == 1 else self.mqtt_off_command_payload
            self.mqtt_client.publish(command_topic, mqtt_payload, retain=False)
        except Exception as e:
            logger.error(f"Error during MQTT publish for {self.service_name}: {e}")

    def update_dbus_from_mqtt(self, path, value):
        try:
            if self[path] != value: self[path] = value
        except Exception as e:
            logger.error(f"Error updating D-Bus path '{path}' in DbusSwitch: {e}")
        return False

# ====================================================================
# DbusDigitalInput Class
# ====================================================================
class DbusDigitalInput(VeDbusService):
    DIGITAL_INPUT_TYPES = {'disabled': 0, 'pulse meter': 1, 'door alarm': 2, 'bilge pump': 3, 'bilge alarm': 4, 'burglar alarm': 5, 'smoke alarm': 6, 'fire alarm': 7, 'co2 alarm': 8, 'generator': 9, 'touch input control': 10, 'generic': 3}

    def __init__(self, service_name, device_config, serial_number, mqtt_client, bus):
        super().__init__(service_name, bus=bus, register=False)
        self.device_config = device_config
        self.config_section_name = device_config.name 
        self.service_name = service_name
        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.19')
        self.add_path('/Mgmt/Connection', 'Virtual')
        self.add_path('/DeviceInstance', self.device_config.getint('DeviceInstance'))
        self.add_path('/ProductId', 41318)
        self.add_path('/ProductName', 'Virtual digital input')
        self.add_path('/Serial', serial_number)
        self.add_path('/CustomName', self.device_config.get('CustomName', 'Digital Input'), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Count', self.device_config.getint('Count', 0), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/State', self.device_config.getint('State', 0), writeable=True, onchangecallback=self.handle_dbus_change)
        initial_type_str = self.device_config.get('Type', 'generic').lower()
        initial_type_int = self.DIGITAL_INPUT_TYPES.get(initial_type_str, self.DIGITAL_INPUT_TYPES['generic'])
        self.add_path('/Type', initial_type_int, writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Settings/InvertTranslation', self.device_config.getint('InvertTranslation', 0), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Settings/InvertAlarm', self.device_config.getint('InvertAlarm', 0), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Settings/AlarmSetting', self.device_config.getint('AlarmSetting', 0), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Connected', 1)
        self.add_path('/InputState', 0)
        self.add_path('/Alarm', 0)
        self.mqtt_client = mqtt_client
        self.mqtt_state_topic = self.device_config.get('MqttStateTopic')
        self.mqtt_on_payload = self.device_config.get('mqtt_on_state_payload', 'ON')
        self.mqtt_off_payload = self.device_config.get('mqtt_off_state_payload', 'OFF')
        self.register()
        if is_valid_topic(self.mqtt_state_topic):
            self.mqtt_client.message_callback_add(self.mqtt_state_topic, self.on_mqtt_message_specific)
            self.mqtt_client.subscribe(self.mqtt_state_topic)
        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' registered on D-Bus.")

    def on_mqtt_message_specific(self, client, userdata, msg):
        if msg.topic != self.mqtt_state_topic: return
        try:
            payload_str = msg.payload.decode().strip()
            raw_state = 1 if payload_str.lower() == self.mqtt_on_payload.lower() else 0 if payload_str.lower() == self.mqtt_off_payload.lower() else None
            if raw_state is None: return
            if self['/InputState'] != raw_state: GLib.idle_add(self.update_dbus_input_state, raw_state)
            invert = self['/Settings/InvertTranslation']
            final_state = (1 - raw_state) if invert == 1 else raw_state
            dbus_state = self._get_dbus_state_for_type(final_state)
            if self['/State'] != dbus_state: GLib.idle_add(self.update_dbus_state, dbus_state)
        except Exception as e:
            logger.error(f"Error processing MQTT message for Digital Input {self.service_name}: {e}")

    def _get_dbus_state_for_type(self, logical_state):
        current_type = self['/Type']
        if current_type == 2: return 7 if logical_state == 1 else 6
        elif current_type == 3: return 3 if logical_state == 1 else 2
        elif 4 <= current_type <= 8: return 9 if logical_state == 1 else 8
        return logical_state

    def update_dbus_input_state(self, new_raw_state):
        self['/InputState'] = new_raw_state
        return False

    def update_dbus_state(self, new_state_value):
        self['/State'] = new_state_value
        return False

    def handle_dbus_change(self, path, value):
        try:
            key_name = path.split('/')[-1]
            value_to_save = value
            if path == '/Type': value_to_save = next((name for name, num in self.DIGITAL_INPUT_TYPES.items() if num == value), 'generic')
            
            if path.startswith('/Settings/'):
                self.save_config_change(self.config_section_name, key_name, value)
                if path == '/Settings/InvertTranslation':
                    current_raw_state = self['/InputState']
                    final_state_after_inversion = (1 - current_raw_state) if value == 1 else current_raw_state
                    new_dbus_state_value = self._get_dbus_state_for_type(final_state_after_inversion)
                    GLib.idle_add(self.update_dbus_state, new_dbus_state_value)
            else:
                self.save_config_change(self.config_section_name, key_name, value_to_save)
            return True
        except Exception as e:
            logger.error(f"Failed to handle D-Bus change for {path}: {e}")
            return False

    def save_config_change(self, section, key, value):
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section): config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as f: config.write(f)
            logger.info(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config changes for key '{key}': {e}")

# ====================================================================
# DbusTempSensor Class (MODIFIED)
# ====================================================================
class DbusTempSensor(VeDbusService):
    TEMPERATURE_TYPES = {'battery': 0, 'fridge': 1, 'generic': 2, 'room': 3, 'outdoor': 4, 'water heater': 5, 'freezer': 6}

    def __init__(self, service_name, device_config, serial_number, mqtt_client, bus):
        super().__init__(service_name, bus=bus, register=False)
        self.device_config = device_config
        self.device_index = device_config.getint('DeviceIndex')
        self.service_name = service_name
        self.mqtt_client = mqtt_client
        self.dbus_path_to_state_topic_map = {}

        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.19')
        self.add_path('/Mgmt/Connection', 'Virtual')
        self.add_path('/DeviceInstance', self.device_index)
        self.add_path('/ProductId', 49248)
        self.add_path('/ProductName', 'Virtual temperature')
        self.add_path('/CustomName', self.device_config.get('CustomName'), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Serial', serial_number)
        self.add_path('/Status', 0)
        self.add_path('/Connected', 1)

        # Always add Temperature path
        self.add_path('/Temperature', 0.0)
        temp_topic = self.device_config.get('TemperatureStateTopic')
        if is_valid_topic(temp_topic):
            self.dbus_path_to_state_topic_map['/Temperature'] = temp_topic

        # Conditionally add Humidity path
        humidity_topic = self.device_config.get('HumidityStateTopic')
        if is_valid_topic(humidity_topic):
            self.add_path('/Humidity', 0.0)
            self.dbus_path_to_state_topic_map['/Humidity'] = humidity_topic
        
        # Conditionally add BatteryVoltage path
        battery_topic = self.device_config.get('BatteryStateTopic')
        if is_valid_topic(battery_topic):
            self.add_path('/BatteryVoltage', 0.0)
            self.dbus_path_to_state_topic_map['/BatteryVoltage'] = battery_topic

        initial_type_str = self.device_config.get('Type', 'generic').lower()
        initial_type_int = self.TEMPERATURE_TYPES.get(initial_type_str, self.TEMPERATURE_TYPES['generic'])
        self.add_path('/TemperatureType', initial_type_int, writeable=True, onchangecallback=self.handle_dbus_change)

        self.register()

        for dbus_path, topic in self.dbus_path_to_state_topic_map.items():
            self.mqtt_client.message_callback_add(topic, self.on_mqtt_message_specific)
            self.mqtt_client.subscribe(topic)
            logger.debug(f"Subscribed DbusTempSensor '{self['/CustomName']}' to topic: {topic}")

        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' registered on D-Bus.")

    def on_mqtt_message_specific(self, client, userdata, msg):
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            if not dbus_path: return

            value = None
            try:
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json: value = float(incoming_json["value"])
            except (json.JSONDecodeError, ValueError):
                try: value = float(payload_str)
                except ValueError: return
            
            if value is not None and self[dbus_path] != value:
                GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)

        except Exception as e:
            logger.error(f"Error processing MQTT message for TempSensor {self.service_name}: {e}")
            
    def handle_dbus_change(self, path, value):
        section_name = f'Temp_Sensor_{self.device_index}'
        if path == '/CustomName':
            self.save_config_change(section_name, 'CustomName', value)
        elif path == '/TemperatureType':
            type_str = next((k for k, v in self.TEMPERATURE_TYPES.items() if v == value), 'generic')
            self.save_config_change(section_name, 'Type', type_str)
        return True

    def save_config_change(self, section, key, value):
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section): config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as f: config.write(f)
            logger.info(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config for TempSensor key '{key}': {e}")

    def update_dbus_from_mqtt(self, path, value):
        self[path] = value
        return False

# ====================================================================
# DbusTankSensor Class (MODIFIED)
# ====================================================================
class DbusTankSensor(VeDbusService):
    FLUID_TYPES = {'fuel': 0, 'fresh water': 1, 'waste water': 2, 'live well': 3, 'oil': 4, 'black water': 5, 'gasoline': 6, 'diesel': 7, 'lpg': 8, 'lng': 9, 'hydraulic oil': 10, 'raw water': 11}

    def __init__(self, service_name, device_config, serial_number, mqtt_client, bus):
        super().__init__(service_name, bus=bus, register=False)
        self.device_config = device_config
        self.device_index = device_config.getint('DeviceIndex')
        self.service_name = service_name
        self.mqtt_client = mqtt_client
        self.dbus_path_to_state_topic_map = {}
        self.is_level_direct = False

        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.19')
        self.add_path('/Mgmt/Connection', 'Virtual')
        self.add_path('/DeviceInstance', self.device_index)
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
        
        raw_topic = self.device_config.get('RawValueStateTopic')
        level_topic = self.device_config.get('LevelStateTopic')

        if is_valid_topic(raw_topic):
            self.add_path('/RawValue', 0.0)
            self.add_path('/RawValueEmpty', self.device_config.getfloat('RawValueEmpty', 0.0), writeable=True, onchangecallback=self.handle_dbus_change)
            self.add_path('/RawValueFull', self.device_config.getfloat('RawValueFull', 1.0), writeable=True, onchangecallback=self.handle_dbus_change)
            self.dbus_path_to_state_topic_map['/RawValue'] = raw_topic
        elif is_valid_topic(level_topic):
            self.is_level_direct = True
            self.dbus_path_to_state_topic_map['/Level'] = level_topic

        # Conditionally add Temperature path
        temp_topic = self.device_config.get('TemperatureStateTopic')
        if is_valid_topic(temp_topic):
            self.add_path('/Temperature', 0.0)
            self.dbus_path_to_state_topic_map['/Temperature'] = temp_topic

        # Conditionally add BatteryVoltage path
        battery_topic = self.device_config.get('BatteryStateTopic')
        if is_valid_topic(battery_topic):
            self.add_path('/BatteryVoltage', 0.0)
            self.dbus_path_to_state_topic_map['/BatteryVoltage'] = battery_topic

        self.register()

        for dbus_path, topic in self.dbus_path_to_state_topic_map.items():
            self.mqtt_client.message_callback_add(topic, self.on_mqtt_message_specific)
            self.mqtt_client.subscribe(topic)
            logger.debug(f"Subscribed DbusTankSensor '{self['/CustomName']}' to topic: {topic}")

        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' registered on D-Bus.")
        if not self.is_level_direct: self._calculate_level_from_raw_value()
        self._calculate_remaining_from_level()

    def on_mqtt_message_specific(self, client, userdata, msg):
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            if not dbus_path: return

            value = None
            try:
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json: value = float(incoming_json["value"])
            except (json.JSONDecodeError, ValueError):
                try: value = float(payload_str)
                except ValueError: return
            
            if value is None: return
            
            if dbus_path == '/RawValue' and not self.is_level_direct:
                if self['/RawValue'] != value: GLib.idle_add(self._update_raw_value_and_recalculate, value)
            elif dbus_path == '/Level' and self.is_level_direct:
                if 0.0 <= value <= 100.0 and self['/Level'] != round(value, 2): GLib.idle_add(self._update_level_and_recalculate, value)
            else:
                if self[dbus_path] != value: GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)

        except Exception as e:
            logger.error(f"Error processing MQTT message for Tank {self.service_name}: {e}")

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
        raw_value, raw_empty, raw_full = self['/RawValue'], self['/RawValueEmpty'], self['/RawValueFull']
        level = 0.0
        if raw_full != raw_empty: level = max(0.0, min(100.0, ((raw_value - raw_empty) / (raw_full - raw_empty)) * 100.0))
        self['/Level'] = round(level, 2)

    def _calculate_remaining_from_level(self):
        self['/Remaining'] = round((self['/Level'] / 100.0) * self['/Capacity'], 2)

    def handle_dbus_change(self, path, value):
        section_name = f'Tank_Sensor_{self.device_index}'
        key_name = path.split('/')[-1]
        value_to_save = next((k for k, v in self.FLUID_TYPES.items() if v == value), 'fresh water') if key_name == 'FluidType' else value
        self.save_config_change(section_name, key_name, value_to_save)
        if path in ['/RawValueEmpty', '/RawValueFull'] and not self.is_level_direct:
            GLib.idle_add(self._calculate_level_from_raw_value)
        GLib.idle_add(self._calculate_remaining_from_level)
        return True

    def save_config_change(self, section, key, value):
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section): config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as f: config.write(f)
            logger.info(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config change for Tank: {e}")

    def update_dbus_from_mqtt(self, path, value):
        self[path] = value
        return False

# ====================================================================
# DbusBattery Class
# ====================================================================
class DbusBattery(VeDbusService):
    def __init__(self, service_name, device_config, serial_number, mqtt_client, bus):
        super().__init__(service_name, bus=bus, register=False)
        self.device_config = device_config
        self.device_index = device_config.getint('DeviceIndex')
        self.service_name = service_name
        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.1.19')
        self.add_path('/Mgmt/Connection', 'Virtual')
        self.add_path('/DeviceInstance', self.device_index)
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
        self.add_path('/ErrorCode', 0)
        self.add_path('/Info/MaxChargeCurrent', None)
        self.add_path('/Info/MaxDischargeCurrent', None)
        self.add_path('/Info/MaxChargeVoltage', None)
        self.mqtt_client = mqtt_client
        self.dbus_path_to_state_topic_map = {k: v for k, v in {
            '/Dc/0/Current': self.device_config.get('CurrentStateTopic'),
            '/Dc/0/Power': self.device_config.get('PowerStateTopic'),
            '/Dc/0/Temperature': self.device_config.get('TemperatureStateTopic'),
            '/Dc/0/Voltage': self.device_config.get('VoltageStateTopic'),
            '/Soc': self.device_config.get('SocStateTopic'),
            '/Soh': self.device_config.get('SohStateTopic'),
        }.items() if is_valid_topic(v)}
        self.register()
        for dbus_path, topic in self.dbus_path_to_state_topic_map.items():
            self.mqtt_client.message_callback_add(topic, self.on_mqtt_message_specific)
            self.mqtt_client.subscribe(topic)
        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' registered on D-Bus.")

    def on_mqtt_message_specific(self, client, userdata, msg):
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            if not dbus_path: return

            value = None
            try:
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json: value = incoming_json["value"]
            except (json.JSONDecodeError, ValueError):
                try: value = float(payload_str)
                except ValueError: return
            
            if value is not None and self[dbus_path] != value:
                GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)

        except Exception as e:
            logger.error(f"Error processing MQTT message for Battery {self.service_name}: {e}")

    def handle_dbus_change(self, path, value):
        section_name = f'Virtual_Battery_{self.device_index}'
        if path == '/CustomName': self.save_config_change(section_name, 'CustomName', value)
        elif path == '/Capacity': self.save_config_change(section_name, 'CapacityAh', value)
        return True

    def save_config_change(self, section, key, value):
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section): config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as f: config.write(f)
            logger.info(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config change for Battery: {e}")
            
    def update_dbus_from_mqtt(self, path, value):
        self[path] = value
        return False

# ====================================================================
# DbusPvCharger Class
# ====================================================================
class DbusPvCharger(VeDbusService):
    def __init__(self, service_name, device_config, serial_number, mqtt_client, bus):
        super().__init__(service_name, bus=bus, register=False)
        self.device_config = device_config
        self.device_index = device_config.getint('DeviceIndex')
        self.service_name = service_name
        self.add_path('/Mgmt/ProcessName', 'dbus-victron-virtual')
        self.add_path('/Mgmt/ProcessVersion', '0.0.1')
        self.add_path('/Mgmt/Connection', 'Virtual')
        self.add_path('/DeviceInstance', self.device_index)
        self.add_path('/ProductId', 41318)
        self.add_path('/ProductName', 'Virtual MPPT')
        self.add_path('/CustomName', self.device_config.get('CustomName'), writeable=True, onchangecallback=self.handle_dbus_change)
        self.add_path('/Serial', serial_number)
        self.add_path('/Connected', 1)
        self.add_path('/Dc/0/Current', 0.0)
        self.add_path('/Dc/0/Voltage', 0.0)
        self.add_path('/Link/ChargeVoltage', None)
        self.add_path('/Link/ChargeCurrent', None)
        self.add_path('/Load/State', None)
        self.add_path('/State', 0)
        self.add_path('/Pv/V', 0.0)
        self.add_path('/Yield/Power', 0.0)
        self.add_path('/Yield/User', 0.0)
        self.add_path('/Yield/System', 0.0)
        self.mqtt_client = mqtt_client
        self.dbus_path_to_state_topic_map = {k: v for k, v in {
            '/Dc/0/Current': self.device_config.get('BatteryCurrentStateTopic'),
            '/Dc/0/Voltage': self.device_config.get('BatteryVoltageStateTopic'),
            '/Link/ChargeVoltage': self.device_config.get('MaxChargeVoltageStateTopic'),
            '/Link/ChargeCurrent': self.device_config.get('MaxChargeCurrentStateTopic'),
            '/Load/State': self.device_config.get('LoadStateTopic'),
            '/State': self.device_config.get('ChargerStateTopic'),
            '/Pv/V': self.device_config.get('PvVoltageStateTopic'),
            '/Yield/Power': self.device_config.get('PvPowerStateTopic'),
            '/Yield/User': self.device_config.get('TotalYield'),
            '/Yield/System': self.device_config.get('SystemYield')
        }.items() if is_valid_topic(v)}
        self.register()
        for dbus_path, topic in self.dbus_path_to_state_topic_map.items():
            self.mqtt_client.message_callback_add(topic, self.on_mqtt_message_specific)
            self.mqtt_client.subscribe(topic)
        logger.info(f"Service '{service_name}' for device '{self['/CustomName']}' registered on D-Bus.")

    def on_mqtt_message_specific(self, client, userdata, msg):
        try:
            payload_str = msg.payload.decode().strip()
            topic = msg.topic
            dbus_path = next((k for k, v in self.dbus_path_to_state_topic_map.items() if v == topic), None)
            if not dbus_path: return

            value = None
            try:
                incoming_json = json.loads(payload_str)
                if isinstance(incoming_json, dict) and "value" in incoming_json: value = incoming_json["value"]
                else: value = float(payload_str)
            except (json.JSONDecodeError, ValueError):
                if dbus_path == '/State': value = {'off': 0, 'bulk': 3, 'absorption': 4, 'float': 5}.get(payload_str.lower(), None)
                elif dbus_path == '/Load/State': value = {'off': 0, 'on': 1}.get(payload_str.lower(), None)
                else:
                    try: value = float(payload_str)
                    except ValueError: return
            if value is None: return
            if self[dbus_path] != value: GLib.idle_add(self.update_dbus_from_mqtt, dbus_path, value)
        except Exception as e:
            logger.error(f"Error processing MQTT message for PV Charger {self.service_name}: {e}")

    def handle_dbus_change(self, path, value):
        if path == '/CustomName':
            self.save_config_change(f'Pv_Charger_{self.device_index}', 'CustomName', value)
        return True

    def save_config_change(self, section, key, value):
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH)
            if not config.has_section(section): config.add_section(section)
            config.set(section, key, str(value))
            with open(CONFIG_FILE_PATH, 'w') as f: config.write(f)
            logger.info(f"Saved config: Section=[{section}], Key='{key}', Value='{value}'")
        except Exception as e:
            logger.error(f"Failed to save config change for PV Charger: {e}")

    def update_dbus_from_mqtt(self, path, value):
        self[path] = value
        return False

# ====================================================================
# Global MQTT Callbacks & Main Function
# ====================================================================
def on_mqtt_connect(client, userdata, flags, rc, properties):
    if rc == 0: logger.info("Connected to MQTT Broker!")
    else: logger.error(f"Failed to connect to MQTT Broker, return code {rc}")

def on_mqtt_disconnect(client, userdata, rc, properties=None, reason=None):
    logger.warning(f"MQTT client disconnected with result code: {rc}, Reason: {reason}")

def on_mqtt_subscribe(client, userdata, mid, granted_qos, properties=None):
    logger.info(f"MQTT Subscription acknowledged. MID: {mid}, Granted QoS: {granted_qos}")

def main():
    logger.info("Starting D-Bus Virtual Devices main service.")
    DBusGMainLoop(set_as_default=True)
    from dbus.mainloop.glib import DBusGMainLoop

    config = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE_PATH):
        logger.critical(f"Config file not found: {CONFIG_FILE_PATH}")
        sys.exit(1)
    config.read(CONFIG_FILE_PATH)
    
    log_level_str = config['Global'].get('LogLevel', 'INFO').upper() if config.has_section('Global') else 'INFO'
    logger.setLevel(getattr(logging, log_level_str, logging.INFO))

    mqtt_config = config['MQTT'] if config.has_section('MQTT') else {}
    mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_subscribe = on_mqtt_subscribe
    mqtt_client.on_disconnect = on_mqtt_disconnect
    
    if mqtt_config.get('Username'):
        mqtt_client.username_pw_set(mqtt_config.get('Username'), mqtt_config.get('Password'))
    
    try:
        mqtt_client.connect(mqtt_config.get('BrokerAddress', 'localhost'), mqtt_config.getint('Port', 1883), 60)
        mqtt_client.loop_start()
    except Exception as e:
        logger.critical(f"Initial MQTT connection failed: {e}. Exiting.")
        sys.exit(1)

    active_services = []
    device_type_map = {
        'relay_module_': DbusSwitch, 'temp_sensor_': DbusTempSensor, 'tank_sensor_': DbusTankSensor,
        'virtual_battery_': DbusBattery, 'input_': DbusDigitalInput, 'pv_charger_': DbusPvCharger
    }

    for section in config.sections():
        if section.lower() in ['global', 'mqtt'] or (section.lower().startswith('switch_') and re.match(r'^switch_\d+_\d+$', section.lower())):
            continue

        for prefix, device_class in device_type_map.items():
            if section.lower().startswith(prefix):
                try:
                    device_config = config[section]
                    match = re.search(r'_(\d+)', section)
                    device_config['DeviceIndex'] = match.group(1) if match else '0'
                    serial = device_config.get('Serial') or str(random.randint(10**15, 10**16 - 1))
                    
                    device_bus = dbus.bus.BusConnection(dbus.Bus.TYPE_SYSTEM)
                    
                    base_type = prefix.strip('_').replace("_", "")
                    type_map = {'relaymodule': 'switch', 'input': 'digitalinput', 'tanksensor': 'tank', 'tempsensor': 'temperature', 'virtualbattery': 'battery', 'pvcharger': 'solarcharger'}
                    service_name = f'com.victronenergy.{type_map.get(base_type, base_type)}.external_{serial}'

                    if device_class == DbusSwitch:
                        outputs = []
                        num_switches = device_config.getint('NumberOfSwitches', 1)
                        for j in range(1, num_switches + 1):
                            out_sec = f'switch_{device_config["DeviceIndex"]}_{j}'
                            if config.has_section(out_sec):
                                outputs.append({'index': j, **config[out_sec]})
                        service = device_class(service_name, device_config, outputs, serial, mqtt_client, 
                                            device_config.get('mqtt_on_state_payload', '1'), device_config.get('mqtt_off_state_payload', '0'),
                                            device_config.get('mqtt_on_command_payload', '1'), device_config.get('mqtt_off_command_payload', '0'), device_bus)
                    else:
                        service = device_class(service_name, device_config, serial, mqtt_client, device_bus)
                    active_services.append(service)
                except Exception as e:
                    logger.error(f"Failed to initialize service for [{section}]: {e}")
                break
    
    if not active_services:
        logger.warning("No services started. Exiting.")
        mqtt_client.loop_stop(); mqtt_client.disconnect()
        sys.exit(0)

    logger.info('All services created. Starting GLib.MainLoop().')
    mainloop = GLib.MainLoop()
    try:
        mainloop.run()
    except KeyboardInterrupt:
        logger.info("Exiting.")
    finally:
        mqtt_client.loop_stop(); mqtt_client.disconnect()
        logger.info("Script finished.")

if __name__ == "__main__":
    main()
