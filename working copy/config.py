#!/usr/bin/env python3
import configparser
import os
import random
import subprocess
import paho.mqtt.client as mqtt
import time
import re
import logging
import sys

# Configure logging for this script itself
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global variable to store discovered topics and device info
discovered_modules_and_topics_global = {}

# --- Existing functions (unchanged) ---
def generate_serial():
    """Generates a random 16-digit serial number."""
    return ''.join([str(random.randint(0, 9)) for _ in range(16)])

# --- MQTT Callbacks for Discovery ---
def parse_mqtt_device_topic(topic):
    """
    Parses MQTT topics to extract device information (Dingtian or Shelly).
    Returns (device_type, module_serial, component_type, component_id, full_topic_base)
    or (None, None, None, None, None) if not a recognized device topic.

    device_type: 'dingtian' or 'shelly'
    module_serial: e.g., 'relay1a76f' or 'shellyplus1pm-08f9e0fe4034'
    component_type: 'out', 'in' (for dingtian relays), 'relay' (for shelly), 'temperature', etc.
    component_id: 'r1', '0' (for shelly relay 0), None for general device topics
    full_topic_base: The base path for the device, e.g., 'dingtian/relay1a76f' or 'shellyplus1pm-08f9e0fe4034'
    """
    logger.debug(f"Attempting to parse topic: {topic}")

    # NEW: Dingtian Input Regex: Flexible 'dingtian' path segment, then 'relay[alphanumeric]', then optional path, then 'out/i[digits]'
    # User specified that 'out/ix' topics are for digital inputs.
    dingtian_input_match = re.search(r'(?:^|.*/)([a-zA-Z0-9_-]*dingtian[a-zA-Z0-9_-]*)/(relay[a-zA-Z0-9]+)/(?:.*/)?out/i([0-9]+)$', topic)
    if dingtian_input_match:
        path_segment_with_dingtian = dingtian_input_match.group(1)
        module_serial = dingtian_input_match.group(2)
        full_topic_base = f"{path_segment_with_dingtian}/{module_serial}"
        logger.debug(f"Matched Dingtian Input (out/iX): Type=dingtian, Serial={module_serial}, ComponentType=in, ComponentID={dingtian_input_match.group(3)}, Base={full_topic_base}")
        return 'dingtian', module_serial, 'in', dingtian_input_match.group(3), full_topic_base


    # Existing Dingtian Output/Input Regex: Flexible 'dingtian' path segment, then 'relay[alphanumeric]', then optional path, then 'out'|'in', then 'r[digits]'
    dingtian_match = re.search(r'(?:^|.*/)([a-zA-Z0-9_-]*dingtian[a-zA-Z0-9_-]*)/(relay[a-zA-Z0-9]+)/(?:.*/)?(out|in)/r([0-9]+)$', topic)
    if dingtian_match:
        path_segment_with_dingtian = dingtian_match.group(1)
        module_serial = dingtian_match.group(2)
        full_topic_base = f"{path_segment_with_dingtian}/{module_serial}"
        logger.debug(f"Matched Dingtian (out/in/rX): Type=dingtian, Serial={module_serial}, ComponentType={dingtian_match.group(3)}, ComponentID={dingtian_match.group(4)}, Base={full_topic_base}")
        return 'dingtian', module_serial, dingtian_match.group(3), dingtian_match.group(4), full_topic_base

    # Shelly Regex (Updated for broader path matching and case-insensitivity)
    shelly_match = re.search(r'(?:^|.*/)(shelly[a-zA-Z0-9_-]+)(?:/.*)?/status/switch:([0-9]+)$', topic, re.IGNORECASE)
    if shelly_match:
        module_serial = shelly_match.group(1)
        full_topic_base = module_serial
        component_id = shelly_match.group(2)
        component_type = 'relay'
        logger.debug(f"Matched Shelly: Type=shelly, Serial={module_serial}, ComponentType={component_type}, ComponentID={component_id}, Base={full_topic_base}")
        return 'shelly', module_serial, component_type, component_id, full_topic_base

    logger.debug("No Dingtian or Shelly pattern matched.")
    return None, None, None, None, None

def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with result code {rc}")
    client.subscribe("#")
    print("Subscribed to '#' for device discovery, will filter for 'dingtian' or 'shelly' in topics.")

def on_message(client, userdata, msg):
    """Callback for when a PUBLISH message is received from the server."""
    topic = msg.topic
    logger.debug(f"Received MQTT message on topic: {topic}")
    device_type, module_serial, component_type, component_id, full_topic_base = parse_mqtt_device_topic(topic)

    if module_serial:
        if module_serial not in discovered_modules_and_topics_global:
            discovered_modules_and_topics_global[module_serial] = {
                "device_type": device_type,
                "topics": set(),
                "base_topic_path": full_topic_base
            }
            logger.debug(f"Discovered new module: {device_type} with serial {module_serial}")
        discovered_modules_and_topics_global[module_serial]["topics"].add(topic)
        logger.debug(f"Added topic {topic} to module {module_serial}")
    else:
        logger.debug(f"Topic '{topic}' did not match any known device patterns.")

# --- Modified Function for MQTT Connection and Discovery ---
def get_mqtt_broker_info(current_broker_address=None, current_port=None, current_username=None, current_password=None):
    """Prompts user for MQTT broker details, showing existing values as defaults."""
    print("\n--- MQTT Broker Configuration ---")

    broker_address = input(f"Enter MQTT broker address (current: {current_broker_address if current_broker_address else 'not set'}): ") or (current_broker_address if current_broker_address else '')
    port = input(f"Enter MQTT port (current: {current_port if current_port else '1883'}): ") or (current_port if current_port else '1883')
    username = input(f"Enter MQTT username (current: {current_username if current_username else 'not set'}; leave blank if none): ") or (current_username if current_username else '')

    password_display = '******' if current_password else 'not set'
    password = input(f"Enter MQTT password (current: {password_display}; leave blank if none): ") or (current_password if current_password else '')

    return broker_address, int(port), username if username else None, password if password else None

def discover_devices_via_mqtt(client):
    """
    Connects to MQTT broker and attempts to discover Dingtian and Shelly devices by listening to topics.
    """
    global discovered_modules_and_topics_global
    discovered_modules_and_topics_global.clear()

    print("\nAttempting to discover Dingtian and Shelly devices via MQTT by listening to topics...")
    print(" (This requires devices to be actively publishing data on topics containing 'dingtian' or 'shelly'.)")

    client.on_connect = on_connect
    client.on_message = on_message

    client.loop_start()

    discovery_duration = 60
    print(f"Listening for messages for {discovery_duration} seconds...")
    time.sleep(discovery_duration)

    client.loop_stop()

    print(f"Found {len(discovered_modules_and_topics_global)} potential Dingtian/Shelly modules.")
    return discovered_modules_and_topics_global

def create_or_edit_config():
    """
    Creates or edits a config file based on user input.
    The file will be located in /data/setupOptions/venus-os_virtual-devices and named optionsSet.
    """
    config_dir = '/data/setupOptions/venus-os_virtual-devices'
    config_path = os.path.join(config_dir, 'optionsSet')

    os.makedirs(config_dir, exist_ok=True)

    config = configparser.ConfigParser()
    file_exists = os.path.exists(config_path)

    existing_relay_modules_by_index = {}
    existing_switches_by_module_and_switch_idx = {}
    existing_inputs_by_module_and_input_idx = {}
    existing_temp_sensors_by_index = {}
    existing_tank_sensors_by_index = {}
    existing_virtual_batteries_by_index = {}


    highest_relay_module_idx_in_file = 0
    highest_temp_sensor_idx_in_file = 0
    highest_tank_sensor_idx_in_file = 0
    highest_virtual_battery_idx_in_file = 0

    highest_existing_device_instance = 99
    highest_existing_device_index = 0

    existing_mqtt_broker = ''
    existing_mqtt_port = '1883'
    existing_mqtt_username = ''
    existing_mqtt_password = ''
    current_loglevel_from_config = 'INFO'

    if file_exists:
        print(f"Existing config file found at {config_path}.")
        while True:
            print("\n--- Configuration Options ---")
            print("1) Continue to configuration (update existing)")
            print("2) Create new configuration (WARNING: Existing configuration will be overwritten!)")
            print("3) Delete existing configuration and exit (WARNING: This cannot be undone!)")

            choice = input("Enter your choice (1, 2 or 3): ")

            if choice == '1':
                config.read(config_path)
                current_loglevel_from_config = config.get('Global', 'loglevel', fallback='INFO')

                existing_mqtt_broker = config.get('MQTT', 'brokeraddress', fallback='')
                existing_mqtt_port = config.get('MQTT', 'port', fallback='1883')
                existing_mqtt_username = config.get('MQTT', 'username', fallback='')
                existing_mqtt_password = config.get('MQTT', 'password', fallback='')

                for section in config.sections():
                    if config.has_option(section, 'deviceinstance'):
                        try:
                            instance = config.getint(section, 'deviceinstance')
                            if instance > highest_existing_device_instance:
                                highest_existing_device_instance = instance
                        except ValueError:
                            pass
                    if config.has_option(section, 'deviceindex'):
                        try:
                            index = config.getint(section, 'deviceindex')
                            if index > highest_existing_device_index:
                                highest_existing_device_index = index
                        except ValueError:
                            pass

                    if section.startswith('Relay_Module_'):
                        try:
                            module_idx = int(section.split('_')[2])
                            highest_relay_module_idx_in_file = max(highest_relay_module_idx_in_file, module_idx)
                            existing_relay_modules_by_index[module_idx] = {
                                'serial': config.get(section, 'serial', fallback=''),
                                'deviceinstance': config.getint(section, 'deviceinstance', fallback=0),
                                'deviceindex': config.getint(section, 'deviceindex', fallback=0),
                                'customname': config.get(section, 'customname', fallback=f'Relay Module {module_idx}'),
                                'numberofswitches': config.getint(section, 'numberofswitches', fallback=0),
                                'numberofinputs': config.getint(section, 'numberofinputs', fallback=0),
                                'mqtt_on_state_payload': config.get(section, 'mqtt_on_state_payload', fallback='ON'),
                                'mqtt_off_state_payload': config.get(section, 'mqtt_off_state_payload', fallback='OFF'),
                                'mqtt_on_command_payload': config.get(section, 'mqtt_on_command_payload', fallback='ON'),
                                'mqtt_off_command_payload': config.get(section, 'mqtt_off_command_payload', fallback='OFF'),
                                # Added moduleserial for discovery exclusion
                                'moduleserial': config.get(section, 'moduleserial', fallback=''),
                            }
                        except (ValueError, IndexError):
                            logger.warning(f"Skipping malformed Relay_Module section: {section}")

                    elif section.startswith('switch_'):
                        try:
                            parts = section.split('_')
                            module_idx = int(parts[1])
                            switch_idx = int(parts[2])
                            existing_switches_by_module_and_switch_idx[(module_idx, switch_idx)] = {
                                'customname': config.get(section, 'customname', fallback=f'switch {switch_idx}'),
                                'group': config.get(section, 'group', fallback=f'Group{module_idx}'),
                                'mqttstatetopic': config.get(section, 'mqttstatetopic', fallback='path/to/mqtt/topic'),
                                'mqttcommandtopic': config.get(section, 'mqttcommandtopic', fallback='path/to/mqtt/topic'),
                            }
                        except (ValueError, IndexError):
                            logger.warning(f"Skipping malformed switch section: {section}")

                    elif section.startswith('input_'):
                        try:
                            parts = section.split('_')
                            module_idx = int(parts[1])
                            input_idx = int(parts[2])
                            existing_inputs_by_module_and_input_idx[(module_idx, input_idx)] = {
                                'customname': config.get(section, 'customname', fallback=f'input {input_idx}'),
                                'serial': config.get(section, 'serial', fallback=''),
                                'deviceinstance': config.getint(section, 'deviceinstance', fallback=0),
                                'deviceindex': config.getint(section, 'deviceindex', fallback=0),
                                'mqttstatetopic': config.get(section, 'mqttstatetopic', fallback='path/to/mqtt/topic'),
                                'mqtt_on_state_payload': config.get(section, 'mqtt_on_state_payload', fallback='ON'),
                                'mqtt_off_state_payload': config.get(section, 'mqtt_off_state_payload', fallback='OFF'),
                                'type': config.get(section, 'type', fallback='disabled'),
                            }
                        except (ValueError, IndexError):
                            logger.warning(f"Skipping malformed input section: {section}")

                    elif section.startswith('Temp_Sensor_'):
                        try:
                            sensor_idx = int(section.split('_')[2])
                            highest_temp_sensor_idx_in_file = max(highest_temp_sensor_idx_in_file, sensor_idx)
                            existing_temp_sensors_by_index[sensor_idx] = {key: val for key, val in config.items(section)}
                        except (ValueError, IndexError):
                            logger.warning(f"Skipping malformed Temp_Sensor section: {section}")

                    elif section.startswith('Tank_Sensor_'):
                        try:
                            sensor_idx = int(section.split('_')[2])
                            highest_tank_sensor_idx_in_file = max(highest_tank_sensor_idx_in_file, sensor_idx)
                            existing_tank_sensors_by_index[sensor_idx] = {key: val for key, val in config.items(section)}
                        except (ValueError, IndexError):
                            logger.warning(f"Skipping malformed Tank_Sensor section: {section}")

                    elif section.startswith('Virtual_Battery_'):
                        try:
                            battery_idx = int(section.split('_')[2])
                            highest_virtual_battery_idx_in_file = max(highest_virtual_battery_idx_in_file, battery_idx)
                            existing_virtual_batteries_by_index[battery_idx] = {key: val for key, val in config.items(section)}
                        except (ValueError, IndexError):
                            logger.warning(f"Skipping malformed Virtual_Battery section: {section}")

                print("Continuing to update existing configuration.")
                break
            elif choice == '2':
                confirm = input("Are you absolutely sure you want to overwrite the existing configuration file? This cannot be undone! (yes/no): ")
                if confirm.lower() == 'yes':
                    os.remove(config_path)
                    print(f"Existing configuration file deleted: {config_path}")
                    file_exists = False
                    config = configparser.ConfigParser()
                    break
                else:
                    print("Creation of new configuration cancelled.")
            elif choice == '3':
                confirm = input("Are you absolutely sure you want to delete the configuration file? This cannot be undone! (yes/no): ")
                if confirm.lower() == 'yes':
                    os.remove(config_path)
                    print(f"Configuration file deleted: {config_path}")
                else:
                    print("Deletion cancelled.")
                print("Exiting script.")
                return
            else:
                print("Invalid choice. Please enter 1, 2 or 3.")
    else:
        print(f"No existing config file found. A new one will be created at {config_path}.")

    device_instance_counter = highest_existing_device_instance + 1
    device_index_sequencer = highest_existing_device_index + 1

    # --- Global settings ---
    if not config.has_section('Global'):
        config.add_section('Global')

    current_loglevel_prompt_default = config.get('Global', 'loglevel', fallback='INFO')
    loglevel = input(f"Enter log level (options: DEBUG, INFO, WARNING, ERROR, CRITICAL; default: {current_loglevel_prompt_default}): ") or current_loglevel_prompt_default
    config.set('Global', 'loglevel', loglevel)

    default_num_relay_modules_initial = 1 if not file_exists else config.getint('Global', 'numberofmodules', fallback=0)
    current_num_relay_modules_setting = config.getint('Global', 'numberofmodules', fallback=default_num_relay_modules_initial)
    while True:
        try:
            num_relay_modules_input = input(f"Enter the number of relay modules (current: {current_num_relay_modules_setting if current_num_relay_modules_setting > 0 else 'not set'}): ")
            if num_relay_modules_input:
                new_num_relay_modules = int(num_relay_modules_input)
                if new_num_relay_modules < 0:
                    raise ValueError
                break
            elif current_num_relay_modules_setting > 0:
                new_num_relay_modules = current_num_relay_modules_setting
                break
            elif not file_exists:
                new_num_relay_modules = default_num_relay_modules_initial
                break
            else:
                print("Invalid input. Please enter a non-negative integer for the number of relay modules.")
        except ValueError:
            print("Invalid input. Please enter a non-negative integer for the number of relay modules.")
    config.set('Global', 'numberofmodules', str(new_num_relay_modules))

    default_num_temp_sensors_initial = 0 if not file_exists else config.getint('Global', 'numberoftempsensors', fallback=0)
    current_num_temp_sensors = config.getint('Global', 'numberoftempsensors', fallback=default_num_temp_sensors_initial)
    while True:
        try:
            num_temp_sensors_input = input(f"Enter the number of temperature sensors (current: {current_num_temp_sensors if current_num_temp_sensors >= 0 else 'not set'}): ")
            if num_temp_sensors_input:
                num_temp_sensors = int(num_temp_sensors_input)
                if num_temp_sensors < 0:
                    raise ValueError
                break
            elif current_num_temp_sensors >= 0:
                num_temp_sensors = current_num_temp_sensors
                break
            elif not file_exists:
                num_temp_sensors = default_num_temp_sensors_initial
                break
            else:
                print("Invalid input. Please enter a non-negative integer for the number of temperature sensors.")
        except ValueError:
            print("Invalid input. Please enter a non-negative integer for the number of temperature sensors.")
    config.set('Global', 'numberoftempsensors', str(num_temp_sensors))

    default_num_tank_sensors_initial = 0 if not file_exists else config.getint('Global', 'numberoftanksensors', fallback=0)
    current_num_tank_sensors = config.getint('Global', 'numberoftanksensors', fallback=default_num_tank_sensors_initial)
    while True:
        try:
            num_tank_sensors_input = input(f"Enter the number of tank sensors (current: {current_num_tank_sensors if current_num_tank_sensors >= 0 else 'not set'}): ")
            if num_tank_sensors_input:
                num_tank_sensors = int(num_tank_sensors_input)
                if num_tank_sensors < 0:
                    raise ValueError
                break
            elif current_num_tank_sensors >= 0:
                num_tank_sensors = current_num_tank_sensors
                break
            elif not file_exists:
                num_tank_sensors = default_num_tank_sensors_initial
                break
            else:
                print("Invalid input. Please enter a non-negative integer for the number of tank sensors.")
        except ValueError:
            print("Invalid input. Please enter a non-negative integer for the number of tank sensors.")
    config.set('Global', 'numberoftanksensors', str(num_tank_sensors))

    default_num_virtual_batteries_initial = 0 if not file_exists else config.getint('Global', 'numberofvirtualbatteries', fallback=0)
    current_num_virtual_batteries = config.getint('Global', 'numberofvirtualbatteries', fallback=default_num_virtual_batteries_initial)
    while True:
        try:
            num_virtual_batteries_input = input(f"Enter the number of virtual batteries (current: {current_num_virtual_batteries if current_num_virtual_batteries >= 0 else 'not set'}): ")
            if num_virtual_batteries_input:
                num_virtual_batteries = int(num_virtual_batteries_input)
                if num_virtual_batteries < 0:
                    raise ValueError
                break
            elif current_num_virtual_batteries >= 0:
                num_virtual_batteries = current_num_virtual_batteries
                break
            elif not file_exists:
                num_virtual_batteries = default_num_virtual_batteries_initial
                break
            else:
                print("Invalid input. Please enter a non-negative integer for the number of virtual batteries.")
        except ValueError:
            print("Invalid input. Please enter a non-negative integer for the number of virtual batteries.")
    config.set('Global', 'numberofvirtualbatteries', str(num_virtual_batteries))

    # --- MQTT Broker Info ---
    broker_address, port, username, password = get_mqtt_broker_info(
        current_broker_address=existing_mqtt_broker,
        current_port=existing_mqtt_port,
        current_username=existing_mqtt_username,
        current_password=existing_mqtt_password
    )

    # --- Device Discovery ---
    should_attempt_discovery = False
    if new_num_relay_modules > highest_relay_module_idx_in_file:
        should_attempt_discovery = True

    auto_configured_serials_to_info = {}

    if should_attempt_discovery:
        while True:
            discovery_choice = input("\nDo you want to try to discover Dingtian/Shelly modules via MQTT?(yes/no): ").lower()
            if discovery_choice in ['yes', 'no']:
                break
            else:
                print("Invalid choice. Please enter 'yes' or 'no'.")

        if discovery_choice == 'yes':
            mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
            if username:
                mqtt_client.username_pw_set(username, password)

            try:
                print(f"Connecting to MQTT broker at {broker_address}:{port}...")
                mqtt_client.connect(broker_address, port, 60)
                print("Connected to MQTT broker.")

                all_discovered_modules_with_topics = discover_devices_via_mqtt(mqtt_client)

                mqtt_client.disconnect()
                print("Disconnected from MQTT broker.")

                newly_discovered_modules_to_propose = {}
                skipped_modules_count = 0
                for module_serial, module_info in all_discovered_modules_with_topics.items():
                    is_already_in_config = False
                    for existing_mod_data in existing_relay_modules_by_index.values():
                        # Check both 'serial' and 'moduleserial' for exclusion
                        if existing_mod_data.get('serial') == module_serial or \
                           existing_mod_data.get('moduleserial') == module_serial:
                            is_already_in_config = True
                            break
                    if not is_already_in_config:
                        newly_discovered_modules_to_propose[module_serial] = module_info
                    else:
                        skipped_modules_count += 1
                if skipped_modules_count > 0:
                    print(f"\nSkipped {skipped_modules_count} discovered modules as they appear to be already configured by serial or moduleserial.")


                if newly_discovered_modules_to_propose:
                    print("\n--- Newly Discovered Modules (by Serial Number) ---")
                    discovered_module_serials_list = sorted(list(newly_discovered_modules_to_propose.keys()))
                    for i, module_serial in enumerate(discovered_module_serials_list):
                        module_info = newly_discovered_modules_to_propose[module_serial]
                        print(f"{i+1}) Device Type: {module_info['device_type'].capitalize()}, Module Serial: {module_serial}")

                    selected_indices_input = input("Enter the numbers of the modules you want to auto-configure (e.g., 1,3,4 or 'all'): ")
                    selected_serials_for_auto_config = []

                    if selected_indices_input.lower() == 'all':
                        selected_serials_for_auto_config = discovered_module_serials_list
                    else:
                        try:
                            indices = [int(x.strip()) - 1 for x in selected_indices_input.split(',')]
                            for idx in indices:
                                if 0 <= idx < len(discovered_module_serials_list):
                                    selected_serials_for_auto_config.append(discovered_module_serials_list[idx])
                                else:
                                    print(f"Warning: Invalid selection number {idx+1} ignored.")
                        except ValueError:
                            print("Invalid input for selection. No specific modules selected for auto-configuration.")

                    if selected_serials_for_auto_config:
                        print(f"\n--- Staging Auto-Configuration for Selected Modules ---")

                        for module_serial in selected_serials_for_auto_config:
                            auto_configured_serials_to_info[module_serial] = newly_discovered_modules_to_propose[module_serial]
                            print(f"  Module {module_serial} selected for auto-configuration.")
                            print(f"\n--- continuing to manual configuration of existing devices, if present ---")
                    else:
                        print("\nNo specific modules selected for auto-configuration.")
                else:
                    print("\nNo new Dingtian or Shelly modules found via MQTT topic discovery to auto-configure.")

            except Exception as e:
                logger.error(f"\nCould not connect to MQTT broker or perform discovery: {e}")
                print("Proceeding with manual configuration without MQTT discovery.")
        else:
            print("\nSkipping MQTT discovery.")
    else:
        print("\nSkipping Dingtian/Shelly module discovery (number of relay modules not increased from previous setting or is 0 for new config).")


    # --- Main Configuration Loop for Relay Modules ---
    print("\n--- Configuring Relay Modules ---")
    used_auto_configured_serials = set()

    for i in range(1, new_num_relay_modules + 1):
        relay_module_section = f'Relay_Module_{i}'

        module_data_from_file = existing_relay_modules_by_index.get(i, {})

        current_serial = module_data_from_file.get('serial', None)
        is_auto_configured_for_this_slot = False
        module_info_from_discovery = None
        discovered_module_serial_for_slot = None # To hold the specific auto_serial_key for this slot


        if i not in existing_relay_modules_by_index and (len(auto_configured_serials_to_info) > len(used_auto_configured_serials)):
            for auto_serial_key in sorted(auto_configured_serials_to_info.keys()):
                if auto_serial_key not in used_auto_configured_serials:
                    # 'serial' remains randomized as per user's request to avoid MQTT client ID issues
                    current_serial = generate_serial()
                    discovered_module_serial_for_slot = auto_serial_key # Store the discovered serial for 'moduleserial' field
                    module_info_from_discovery = auto_configured_serials_to_info[auto_serial_key]
                    is_auto_configured_for_this_slot = True
                    used_auto_configured_serials.add(auto_serial_key)
                    print(f"Auto-configuring NEW Relay Module slot {i}. Assigned generated serial {current_serial} and discovered module serial {discovered_module_serial_for_slot}.")
                    break

        # If current_serial is still None (meaning it's a new slot not picked for auto-config, or no auto-config available)
        if current_serial is None:
            current_serial = generate_serial()
            logger.debug(f"Generated new serial {current_serial} for Relay Module slot {i}.")

        is_new_module_slot = (i > highest_relay_module_idx_in_file)

        if not config.has_section(relay_module_section):
            config.add_section(relay_module_section)

        config.set(relay_module_section, 'serial', current_serial) # Set the (generated or existing) serial

        # Handle 'moduleserial' field
        if is_auto_configured_for_this_slot and discovered_module_serial_for_slot:
            # If this is a newly auto-configured slot, set moduleserial to the discovered one
            config.set(relay_module_section, 'moduleserial', discovered_module_serial_for_slot)
        elif module_data_from_file.get('moduleserial'):
            # If it's an existing module and it already has a moduleserial, retain it
            config.set(relay_module_section, 'moduleserial', module_data_from_file['moduleserial'])
        else:
            # Otherwise, ensure moduleserial is not present
            if config.has_option(relay_module_section, 'moduleserial'):
                config.remove_option(relay_module_section, 'moduleserial')


        if is_new_module_slot:
            config.set(relay_module_section, 'deviceinstance', str(device_instance_counter))
            device_instance_counter += 1
        else:
            current_device_instance = module_data_from_file.get('deviceinstance', highest_existing_device_instance + 1)
            device_instance_input = input(f"Enter device instance for Relay Module {i} (current: {current_device_instance}): ")
            val = device_instance_input if device_instance_input else str(current_device_instance)
            config.set(relay_module_section, 'deviceinstance', val)

        if is_new_module_slot:
            config.set(relay_module_section, 'deviceindex', str(device_index_sequencer))
            device_index_sequencer += 1
        else:
            current_device_index = module_data_from_file.get('deviceindex', highest_existing_device_index + 1)
            device_index_input = input(f"Enter device index for Relay Module {i} (current: {current_device_index}): ")
            val = device_index_input if device_index_input else str(current_device_index)
            config.set(relay_module_section, 'deviceindex', val)

        current_custom_name = module_data_from_file.get('customname', f'Relay Module {i}')
        if is_auto_configured_for_this_slot and module_info_from_discovery:
            config.set(relay_module_section, 'customname', f"{module_info_from_discovery['device_type'].capitalize()} Module {i} (Auto)")
        else:
            custom_name = input(f"Enter custom name for Relay Module {i} (current: {current_custom_name}): ")
            config.set(relay_module_section, 'customname', custom_name if custom_name else current_custom_name)

        current_num_switches_for_module = module_data_from_file.get('numberofswitches', 2)
        if is_auto_configured_for_this_slot and module_info_from_discovery:
            if module_info_from_discovery['device_type'] == 'dingtian':
                dingtian_out_switches = set()
                for t in module_info_from_discovery['topics']:
                    parsed_type, parsed_serial, comp_type, comp_id, _ = parse_mqtt_device_topic(t)
                    if parsed_serial == auto_serial_key and comp_type == 'out' and comp_id:
                        dingtian_out_switches.add(comp_id)
                current_num_switches_for_module = len(dingtian_out_switches) if dingtian_out_switches else 1
            elif module_info_from_discovery['device_type'] == 'shelly':
                shelly_relays = set()
                for t in module_info_from_discovery['topics']:
                    parsed_type, parsed_serial, comp_type, comp_id, _ = parse_mqtt_device_topic(t)
                    if parsed_serial == auto_serial_key and comp_type == 'relay' and comp_id:
                        shelly_relays.add(comp_id)
                current_num_switches_for_module = len(shelly_relays) if shelly_relays else 1
            config.set(relay_module_section, 'numberofswitches', str(current_num_switches_for_module))
        else:
            while True:
                try:
                    num_switches_input = input(f"Enter the number of switches for Relay Module {i} (current: {current_num_switches_for_module if current_num_switches_for_module > 0 else 'not set'}): ")
                    if num_switches_input:
                        num_switches = int(num_switches_input)
                        if num_switches <= 0:
                            raise ValueError
                        break
                    elif current_num_switches_for_module > 0:
                        num_switches = current_num_switches_for_module
                        break
                    else:
                        print("Invalid input. Please enter a positive integer for the number of switches.")
                except ValueError:
                    print("Invalid input. Please enter a positive integer for the number of switches.")
            config.set(relay_module_section, 'numberofswitches', str(num_switches))

        current_num_inputs_for_module = module_data_from_file.get('numberofinputs', 0)
        if is_auto_configured_for_this_slot and module_info_from_discovery:
            if module_info_from_discovery['device_type'] == 'dingtian':
                dingtian_in_inputs = set()
                for t in module_info_from_discovery['topics']:
                    parsed_type, parsed_serial, comp_type, comp_id, _ = parse_mqtt_device_topic(t)
                    if parsed_serial == auto_serial_key and comp_type == 'in' and comp_id: # comp_type will be 'in' if the 'out/iX' regex matched
                        dingtian_in_inputs.add(comp_id)
                current_num_inputs_for_module = len(dingtian_in_inputs)
                config.set(relay_module_section, 'numberofinputs', str(current_num_inputs_for_module))
            elif module_info_from_discovery['device_type'] == 'shelly':
                current_num_inputs_for_module = 0
                config.set(relay_module_section, 'numberofinputs', str(current_num_inputs_for_module))
        else:
            while True:
                try:
                    num_inputs_input = input(f"Enter the number of inputs for Relay Module {i} (current: {current_num_inputs_for_module}): ")
                    if num_inputs_input:
                        num_inputs = int(num_inputs_input)
                        if num_inputs < 0:
                            raise ValueError
                        break
                    elif current_num_inputs_for_module >= 0:
                        num_inputs = current_num_inputs_for_module
                        break
                    else:
                        print("Invalid input. Please enter a non-negative integer for the number of inputs.")
                except ValueError:
                    print("Invalid input. Please enter a non-negative integer for the number of inputs.")
            config.set(relay_module_section, 'numberofinputs', str(num_inputs))


        payload_defaults_dingtian = {'on_state': 'ON', 'off_state': 'OFF', 'on_cmd': 'ON', 'off_cmd': 'OFF'}
        payload_defaults_shelly = {'on_state': '{"output": true}', 'off_state': '{"output": false}', 'on_cmd': 'on', 'off_cmd': 'off'}

        default_payloads = payload_defaults_dingtian
        if is_auto_configured_for_this_slot and module_info_from_discovery and module_info_from_discovery['device_type'] == 'shelly':
            default_payloads = payload_defaults_shelly

        if is_auto_configured_for_this_slot:
            config.set(relay_module_section, 'mqtt_on_state_payload', default_payloads['on_state'])
            config.set(relay_module_section, 'mqtt_off_state_payload', default_payloads['off_state'])
            config.set(relay_module_section, 'mqtt_on_command_payload', default_payloads['on_cmd'])
            config.set(relay_module_section, 'mqtt_off_command_payload', default_payloads['off_cmd'])
        else:
            current_mqtt_on_state_payload = module_data_from_file.get('mqtt_on_state_payload', default_payloads['on_state'])
            mqtt_on_state_payload = input(f"Enter MQTT ON state payload for Relay Module {i} (current: {current_mqtt_on_state_payload}): ")
            config.set(relay_module_section, 'mqtt_on_state_payload', mqtt_on_state_payload if mqtt_on_state_payload else current_mqtt_on_state_payload)

            current_mqtt_off_state_payload = module_data_from_file.get('mqtt_off_state_payload', default_payloads['off_state'])
            mqtt_off_state_payload = input(f"Enter MQTT OFF state payload for Relay Module {i} (current: {current_mqtt_off_state_payload}): ")
            config.set(relay_module_section, 'mqtt_off_state_payload', mqtt_off_state_payload if mqtt_off_state_payload else current_mqtt_off_state_payload)

            current_mqtt_on_command_payload = module_data_from_file.get('mqtt_on_command_payload', default_payloads['on_cmd'])
            mqtt_on_command_payload = input(f"Enter MQTT ON command payload for Relay Module {i} (current: {current_mqtt_on_command_payload}): ")
            config.set(relay_module_section, 'mqtt_on_command_payload', mqtt_on_command_payload if mqtt_on_command_payload else current_mqtt_on_command_payload)

            current_mqtt_off_command_payload = module_data_from_file.get('mqtt_off_command_payload', default_payloads['off_cmd'])
            mqtt_off_command_payload = input(f"Enter MQTT OFF command payload for Relay Module {i} (current: {current_mqtt_off_command_payload}): ")
            config.set(relay_module_section, 'mqtt_off_command_payload', mqtt_off_command_payload if mqtt_off_command_payload else current_mqtt_off_command_payload)

        num_switches_for_module_section = int(config.get(relay_module_section, 'numberofswitches'))
        for j in range(1, num_switches_for_module_section + 1):
            switch_section = f'switch_{i}_{j}'

            switch_data_from_file = existing_switches_by_module_and_switch_idx.get((i, j), {})

            if not config.has_section(switch_section):
                config.add_section(switch_section)

            auto_discovered_state_topic = None
            auto_discovered_command_topic = None

            if is_auto_configured_for_this_slot and module_info_from_discovery:
                base_topic_path = module_info_from_discovery['base_topic_path']
                device_type = module_info_from_discovery['device_type']

                if device_type == 'dingtian':
                    for t in module_info_from_discovery['topics']:
                        parsed_type, parsed_serial, parsed_comp_type, parsed_comp_id, _ = parse_mqtt_device_topic(t)
                        if parsed_serial == discovered_module_serial_for_slot and parsed_comp_type == 'out' and parsed_comp_id == str(j):
                            auto_discovered_state_topic = t
                            auto_discovered_command_topic = t.replace('/out/r', '/in/r', 1)
                            break
                elif device_type == 'shelly':
                    shelly_switch_idx = j - 1
                    auto_discovered_state_topic = f'{base_topic_path}/status/switch:{shelly_switch_idx}'
                    auto_discovered_command_topic = f'{base_topic_path}/command/switch:{shelly_switch_idx}'


            current_switch_custom_name = switch_data_from_file.get('customname', f'switch {j}')
            if is_auto_configured_for_this_slot:
                config.set(switch_section, 'customname', current_switch_custom_name)
            else:
                switch_custom_name = input(f"Enter custom name for Relay Module {i}, switch {j} (current: {current_switch_custom_name}): ")
                config.set(switch_section, 'customname', switch_custom_name if switch_custom_name else current_switch_custom_name)

            current_switch_group = switch_data_from_file.get('group', f'Group{i}')
            if is_auto_configured_for_this_slot:
                config.set(switch_section, 'group', current_switch_group)
            else:
                switch_group = input(f"Enter group for Relay Module {i}, switch {j} (current: {current_switch_group}): ")
                config.set(switch_section, 'group', switch_group if switch_group else current_switch_group)

            current_mqtt_state_topic = switch_data_from_file.get('mqttstatetopic', auto_discovered_state_topic if auto_discovered_state_topic else 'path/to/mqtt/topic')
            if is_auto_configured_for_this_slot:
                config.set(switch_section, 'mqttstatetopic', current_mqtt_state_topic)
            else:
                mqtt_state_topic = input(f"Enter MQTT state topic for Relay Module {i}, switch {j} (current: {current_mqtt_state_topic}): ")
                config.set(switch_section, 'mqttstatetopic', mqtt_state_topic if mqtt_state_topic else current_mqtt_state_topic)

            current_mqtt_command_topic = switch_data_from_file.get('mqttcommandtopic', auto_discovered_command_topic if auto_discovered_command_topic else 'path/to/mqtt/topic')
            if is_auto_configured_for_this_slot:
                config.set(switch_section, 'mqttcommandtopic', current_mqtt_command_topic)
            else:
                mqtt_command_topic = input(f"Enter MQTT command topic for Relay Module {i}, switch {j} (current: {current_mqtt_command_topic}): ")
                config.set(switch_section, 'mqttcommandtopic', mqtt_command_topic if mqtt_command_topic else current_mqtt_command_topic)

        num_inputs_for_module_section = int(config.get(relay_module_section, 'numberofinputs'))
        for k in range(1, num_inputs_for_module_section + 1):
            input_section = f'input_{i}_{k}'

            input_data_from_file = existing_inputs_by_module_and_input_idx.get((i, k), {})

            is_new_input_slot = (i > highest_relay_module_idx_in_file or (i == highest_relay_module_idx_in_file and k > len([s for s in existing_inputs_by_module_and_input_idx if s[0] == i])))

            if not config.has_section(input_section):
                config.add_section(input_section)

            current_input_serial = input_data_from_file.get('serial', None)
            if current_input_serial is None:
                current_input_serial = generate_serial()
                logger.debug(f"Generated new serial {current_input_serial} for Relay Module {i}, Input {k}.")
            config.set(input_section, 'serial', current_input_serial)

            if is_new_input_slot:
                config.set(input_section, 'deviceinstance', str(device_instance_counter))
                device_instance_counter += 1
            else:
                current_device_instance = input_data_from_file.get('deviceinstance', highest_existing_device_instance + 1)
                device_instance_input = input(f"Enter device instance for Relay Module {i}, Input {k} (current: {current_device_instance}): ")
                val = device_instance_input if device_instance_input else str(current_device_instance)
                config.set(input_section, 'deviceinstance', val)

            if is_new_input_slot:
                config.set(input_section, 'deviceindex', str(device_index_sequencer))
                device_index_sequencer += 1
            else:
                current_device_index = input_data_from_file.get('deviceindex', highest_existing_device_index + 1)
                device_index_input = input(f"Enter device index for Relay Module {i}, Input {k} (current: {current_device_index}): ")
                val = device_index_input if device_index_input else str(current_device_index)
                config.set(input_section, 'deviceindex', val)

            current_input_custom_name = input_data_from_file.get('customname', f'Input {k}')
            if is_auto_configured_for_this_slot and module_info_from_discovery and module_info_from_discovery['device_type'] == 'dingtian':
                config.set(input_section, 'customname', f"Dingtian Input {k} (Auto)")
            else:
                input_custom_name = input(f"Enter custom name for Relay Module {i}, Input {k} (current: {current_input_custom_name}): ")
                config.set(input_section, 'customname', input_custom_name if input_custom_name else current_input_custom_name)

            auto_discovered_input_state_topic = None
            if is_auto_configured_for_this_slot and module_info_from_discovery and module_info_from_discovery['device_type'] == 'dingtian':
                base_topic_path = module_info_from_discovery['base_topic_path']
                for t in module_info_from_discovery['topics']:
                    parsed_type, parsed_serial, parsed_comp_type, parsed_comp_id, _ = parse_mqtt_device_topic(t)
                    if parsed_serial == discovered_module_serial_for_slot and parsed_comp_type == 'in' and parsed_comp_id == str(k):
                        auto_discovered_input_state_topic = t
                        break

            current_mqtt_input_state_topic = input_data_from_file.get('mqttstatetopic', auto_discovered_input_state_topic if auto_discovered_input_state_topic else 'path/to/mqtt/input/topic')
            if is_auto_configured_for_this_slot and module_info_from_discovery and module_info_from_discovery['device_type'] == 'dingtian':
                config.set(input_section, 'mqttstatetopic', current_mqtt_input_state_topic)
            else:
                mqtt_input_state_topic = input(f"Enter MQTT state topic for Relay Module {i}, Input {k} (current: {current_mqtt_input_state_topic}): ")
                config.set(input_section, 'mqttstatetopic', mqtt_input_state_topic if mqtt_input_state_topic else current_mqtt_input_state_topic)

            current_mqtt_input_on_state_payload = input_data_from_file.get('mqtt_on_state_payload', 'ON')
            if is_auto_configured_for_this_slot and module_info_from_discovery and module_info_from_discovery['device_type'] == 'dingtian':
                config.set(input_section, 'mqtt_on_state_payload', 'ON')
            else:
                mqtt_input_on_state_payload = input(f"Enter MQTT ON state payload for Relay Module {i}, Input {k} (current: {current_mqtt_input_on_state_payload}): ")
                config.set(input_section, 'mqtt_on_state_payload', mqtt_input_on_state_payload if mqtt_input_on_state_payload else current_mqtt_input_on_state_payload)

            current_mqtt_input_off_state_payload = input_data_from_file.get('mqtt_off_state_payload', 'OFF')
            if is_auto_configured_for_this_slot and module_info_from_discovery and module_info_from_discovery['device_type'] == 'dingtian':
                config.set(input_section, 'mqtt_off_state_payload', 'OFF')
            else:
                mqtt_input_off_state_payload = input(f"Enter MQTT OFF state payload for Relay Module {i}, Input {k} (current: {current_mqtt_input_off_state_payload}): ")
                config.set(input_section, 'mqtt_off_state_payload', mqtt_input_off_state_payload if mqtt_input_off_state_payload else current_mqtt_input_off_state_payload)

            input_types = ['disabled', 'pulse meter', 'door alarm', 'bilge pump', 'bilge alarm', 'burglar alarm', 'smoke alarm', 'fire alarm', 'CO2 alarm', 'generator', 'touch input control']
            current_input_type = input_data_from_file.get('type', 'disabled')
            if is_auto_configured_for_this_slot and module_info_from_discovery and module_info_from_discovery['device_type'] == 'dingtian':
                config.set(input_section, 'type', 'disabled')
            else:
                while True:
                    input_type_input = input(f"Enter type for Relay Module {i}, Input {k} (options: {', '.join(input_types)}; current: {current_input_type}): ")
                    if input_type_input:
                        if input_type_input.lower() in input_types:
                            config.set(input_section, 'type', input_type_input.lower())
                            break
                        else:
                            print(f"Invalid type. Please choose from: {', '.join(input_types)}")
                    else:
                        config.set(input_section, 'type', current_input_type)
                        break

    print("\n--- Configuring Temperature Sensors ---")
    for i in range(1, num_temp_sensors + 1):
        temp_sensor_section = f'Temp_Sensor_{i}'
        sensor_data_from_file = existing_temp_sensors_by_index.get(i, {})

        is_new_sensor_slot = (i > highest_temp_sensor_idx_in_file)

        if not config.has_section(temp_sensor_section):
            config.add_section(temp_sensor_section)

        if is_new_sensor_slot:
            config.set(temp_sensor_section, 'deviceinstance', str(device_instance_counter))
            device_instance_counter += 1
        else:
            current_device_instance = sensor_data_from_file.get('deviceinstance', highest_existing_device_instance + 1)
            device_instance_input = input(f"Enter device instance for Temperature Sensor {i} (current: {current_device_instance}): ")
            config.set(temp_sensor_section, 'deviceinstance', device_instance_input if device_instance_input else str(current_device_instance))

        if is_new_sensor_slot:
            config.set(temp_sensor_section, 'deviceindex', str(device_index_sequencer))
            device_index_sequencer += 1
        else:
            current_device_index = sensor_data_from_file.get('deviceindex', highest_existing_device_index + 1)
            device_index_input = input(f"Enter device index for Temperature Sensor {i} (current: {current_device_index}): ")
            config.set(temp_sensor_section, 'deviceindex', device_index_input if device_index_input else str(current_device_index))


        current_custom_name = sensor_data_from_file.get('customname', f'Temperature Sensor {i}')
        custom_name = input(f"Enter custom name for Temperature Sensor {i} (current: {current_custom_name}): ")
        config.set(temp_sensor_section, 'customname', custom_name if custom_name else current_custom_name)

        current_serial = sensor_data_from_file.get('serial', generate_serial())
        if not sensor_data_from_file.get('serial'):
            print(f"Generated new serial for Temperature Sensor {i}: {current_serial}")
        config.set(temp_sensor_section, 'serial', current_serial)

        temp_sensor_types = ['battery', 'fridge', 'room', 'outdoor', 'water heater', 'freezer', 'generic']
        current_temp_sensor_type = sensor_data_from_file.get('type', 'generic')
        while True:
            temp_type_input = input(f"Enter type for Temperature Sensor {i} (options: {', '.join(temp_sensor_types)}; current: {current_temp_sensor_type}): ")
            if temp_type_input:
                if temp_type_input.lower() in temp_sensor_types:
                    config.set(temp_sensor_section, 'type', temp_type_input.lower())
                    break
                else:
                    print(f"Invalid type. Please choose from: {', '.join(temp_sensor_types)}")
            else:
                config.set(temp_sensor_section, 'type', current_temp_sensor_type)
                break

        current_temp_state_topic = sensor_data_from_file.get('temperaturestatetopic', 'path/to/mqtt/temperature')
        temp_state_topic = input(f"Enter MQTT temperature state topic for Temperature Sensor {i} (current: {current_temp_state_topic}): ")
        config.set(temp_sensor_section, 'temperaturestatetopic', temp_state_topic if temp_state_topic else current_temp_state_topic)

        current_humidity_state_topic = sensor_data_from_file.get('humiditystatetopic', 'path/to/mqtt/humidity')
        humidity_state_topic = input(f"Enter MQTT humidity state topic for Temperature Sensor {i} (current: {current_humidity_state_topic}): ")
        config.set(temp_sensor_section, 'humiditystatetopic', humidity_state_topic if humidity_state_topic else current_humidity_state_topic)

        current_battery_state_topic = sensor_data_from_file.get('batterystatetopic', 'path/to/mqtt/battery')
        battery_state_topic = input(f"Enter MQTT battery state topic for Temperature Sensor {i} (current: {current_battery_state_topic}): ")
        config.set(temp_sensor_section, 'batterystatetopic', battery_state_topic if battery_state_topic else current_battery_state_topic)

    print("\n--- Configuring Tank Sensors ---")
    fluid_types_map = {
        'fuel': 0, 'fresh water': 1, 'waste water': 2, 'live well': 3,
        'oil': 4, 'black water': 5, 'gasoline': 6, 'diesel': 7,
        'lpg': 8, 'lng': 9, 'hydraulic oil': 10, 'raw water': 11
    }
    for i in range(1, num_tank_sensors + 1):
        tank_sensor_section = f'Tank_Sensor_{i}'
        sensor_data_from_file = existing_tank_sensors_by_index.get(i, {})

        is_new_sensor_slot = (i > highest_tank_sensor_idx_in_file)

        if not config.has_section(tank_sensor_section):
            config.add_section(tank_sensor_section)

        if is_new_sensor_slot:
            config.set(tank_sensor_section, 'deviceinstance', str(device_instance_counter))
            device_instance_counter += 1
        else:
            current_device_instance = sensor_data_from_file.get('deviceinstance', highest_existing_device_instance + 1)
            device_instance_input = input(f"Enter device instance for Tank Sensor {i} (current: {current_device_instance}): ")
            config.set(tank_sensor_section, 'deviceinstance', device_instance_input if device_instance_input else str(current_device_instance))

        if is_new_sensor_slot:
            config.set(tank_sensor_section, 'deviceindex', str(device_index_sequencer))
            device_index_sequencer += 1
        else:
            current_device_index = sensor_data_from_file.get('deviceindex', highest_existing_device_index + 1)
            device_index_input = input(f"Enter device index for Tank Sensor {i} (current: {current_device_index}): ")
            config.set(tank_sensor_section, 'deviceindex', device_index_input if device_index_input else str(current_device_index))


        current_custom_name = sensor_data_from_file.get('customname', f'Tank Sensor {i}')
        custom_name = input(f"Enter custom name for Tank Sensor {i} (current: {current_custom_name}): ")
        config.set(tank_sensor_section, 'customname', custom_name if custom_name else current_custom_name)

        current_serial = sensor_data_from_file.get('serial', generate_serial())
        if not sensor_data_from_file.get('serial'):
            print(f"Generated new serial for Tank Sensor {i}: {current_serial}")
        config.set(tank_sensor_section, 'serial', current_serial)

        current_level_state_topic = sensor_data_from_file.get('levelstatetopic', 'path/to/mqtt/level')
        level_state_topic = input(f"Enter MQTT level state topic for Tank Sensor {i} (current: {current_level_state_topic}): ")
        config.set(tank_sensor_section, 'levelstatetopic', level_state_topic if level_state_topic else current_level_state_topic)

        current_battery_state_topic = sensor_data_from_file.get('batterystatetopic', 'path/to/mqtt/battery')
        battery_state_topic = input(f"Enter MQTT battery state topic for Tank Sensor {i} (current: {current_battery_state_topic}): ")
        config.set(tank_sensor_section, 'batterystatetopic', battery_state_topic if battery_state_topic else current_battery_state_topic)

        current_temp_state_topic = sensor_data_from_file.get('temperaturestatetopic', 'path/to/mqtt/temperature')
        temp_state_topic = input(f"Enter MQTT temperature state topic for Tank Sensor {i} (current: {current_temp_state_topic}): ")
        config.set(tank_sensor_section, 'temperaturestatetopic', temp_state_topic if temp_state_topic else current_temp_state_topic)

        current_raw_value_state_topic = sensor_data_from_file.get('rawvaluestatetopic', 'path/to/mqtt/rawvalue')
        raw_value_state_topic = input(f"Enter MQTT raw value state topic for Tank Sensor {i} (current: {current_raw_value_state_topic}): ")
        config.set(tank_sensor_section, 'rawvaluestatetopic', raw_value_state_topic if raw_value_state_topic else current_raw_value_state_topic)

        fluid_types_display = ", ".join([f"'{name}'" for name in fluid_types_map.keys()])
        current_fluid_type_name = sensor_data_from_file.get('fluidtype', 'fresh water')
        while True:
            fluid_type_input = input(f"Enter fluid type for Tank Sensor {i} (options: {fluid_types_display}; current: '{current_fluid_type_name}'): ")
            if fluid_type_input:
                if fluid_type_input.lower() in fluid_types_map:
                    config.set(tank_sensor_section, 'fluidtype', fluid_type_input.lower())
                    break
                else:
                    print("Invalid fluid type. Please choose from the available options.")
            else:
                config.set(tank_sensor_section, 'fluidtype', current_fluid_type_name)
                break

        current_raw_value_empty = sensor_data_from_file.get('rawvalueempty', '0')
        config.set(tank_sensor_section, 'rawvalueempty', input(f"Enter raw value for empty tank (current: {current_raw_value_empty}): ") or current_raw_value_empty)

        current_raw_value_full = sensor_data_from_file.get('rawvaluefull', '50')
        config.set(tank_sensor_section, 'rawvaluefull', input(f"Enter raw value for full tank (current: {current_raw_value_full}): ") or current_raw_value_full)

        current_capacity = sensor_data_from_file.get('capacity', '0.2')
        config.set(tank_sensor_section, 'capacity', input(f"Enter tank capacity in m³ (current: {current_capacity}): ") or current_capacity)


    print("\n--- Configuring Virtual Batteries ---")
    for i in range(1, num_virtual_batteries + 1):
        virtual_battery_section = f'Virtual_Battery_{i}'
        battery_data_from_file = existing_virtual_batteries_by_index.get(i, {})

        is_new_sensor_slot = (i > highest_virtual_battery_idx_in_file)

        if not config.has_section(virtual_battery_section):
            config.add_section(virtual_battery_section)

        if is_new_sensor_slot:
            config.set(virtual_battery_section, 'deviceinstance', str(device_instance_counter))
            device_instance_counter += 1
        else:
            current_device_instance = battery_data_from_file.get('deviceinstance', highest_existing_device_instance + 1)
            device_instance_input = input(f"Enter device instance for Virtual Battery {i} (current: {current_device_instance}): ")
            config.set(virtual_battery_section, 'deviceinstance', device_instance_input if device_instance_input else str(current_device_instance))

        if is_new_sensor_slot:
            config.set(virtual_battery_section, 'deviceindex', str(device_index_sequencer))
            device_index_sequencer += 1
        else:
            current_device_index = battery_data_from_file.get('deviceindex', highest_existing_device_index + 1)
            device_index_input = input(f"Enter device index for Virtual Battery {i} (current: {current_device_index}): ")
            config.set(virtual_battery_section, 'deviceindex', device_index_input if device_index_input else str(current_device_index))


        current_custom_name = battery_data_from_file.get('customname', f'Virtual Battery {i}')
        custom_name = input(f"Enter custom name for Virtual Battery {i} (current: {current_custom_name}): ")
        config.set(virtual_battery_section, 'customname', custom_name if custom_name else current_custom_name)

        current_serial = battery_data_from_file.get('serial', generate_serial())
        if not battery_data_from_file.get('serial'):
            print(f"Generated new serial for Virtual Battery {i}: {current_serial}")
        config.set(virtual_battery_section, 'serial', current_serial)

        current_capacity = battery_data_from_file.get('capacityah', '100')
        capacity = input(f"Enter capacity for Virtual Battery {i} in Ah (current: {current_capacity}): ")
        config.set(virtual_battery_section, 'capacityah', capacity if capacity else current_capacity)

        current_current_state_topic = battery_data_from_file.get('currentstatetopic', 'path/to/mqtt/battery/current')
        current_state_topic = input(f"Enter MQTT current state topic for Virtual Battery {i} (current: {current_current_state_topic}): ")
        config.set(virtual_battery_section, 'currentstatetopic', current_state_topic if current_state_topic else current_current_state_topic)

        current_power_state_topic = battery_data_from_file.get('powerstatetopic', 'path/to/mqtt/battery/power')
        power_state_topic = input(f"Enter MQTT power state topic for Virtual Battery {i} (current: {current_power_state_topic}): ")
        config.set(virtual_battery_section, 'powerstatetopic', power_state_topic if power_state_topic else current_power_state_topic)

        current_temperature_state_topic = battery_data_from_file.get('temperaturestatetopic', 'path/to/mqtt/battery/temperature')
        temperature_state_topic = input(f"Enter MQTT temperature state topic for Virtual Battery {i} (current: {current_temperature_state_topic}): ")
        config.set(virtual_battery_section, 'temperaturestatetopic', temperature_state_topic if temperature_state_topic else current_temperature_state_topic)

        current_voltage_state_topic = battery_data_from_file.get('voltagestatetopic', 'path/to/mqtt/battery/voltage')
        voltage_state_topic = input(f"Enter MQTT voltage state topic for Virtual Battery {i} (current: {current_voltage_state_topic}): ")
        config.set(virtual_battery_section, 'voltagestatetopic', voltage_state_topic if voltage_state_topic else current_voltage_state_topic)

        current_max_charge_current_state_topic = battery_data_from_file.get('maxchargecurrentstatetopic', 'path/to/mqtt/battery/maxchargecurrent')
        max_charge_current_state_topic = input(f"Enter MQTT max charge current state topic for Virtual Battery {i} (current: {current_max_charge_current_state_topic}): ")
        config.set(virtual_battery_section, 'maxchargecurrentstatetopic', max_charge_current_state_topic if max_charge_current_state_topic else current_max_charge_current_state_topic)

        current_max_charge_voltage_state_topic = battery_data_from_file.get('maxchargevoltagestatetopic', 'path/to/mqtt/battery/maxchargevoltage')
        max_charge_voltage_state_topic = input(f"Enter MQTT max charge voltage state topic for Virtual Battery {i} (current: {current_max_charge_voltage_state_topic}): ")
        config.set(virtual_battery_section, 'maxchargevoltagestatetopic', max_charge_voltage_state_topic if max_charge_voltage_state_topic else current_max_charge_voltage_state_topic)

        current_max_discharge_current_state_topic = battery_data_from_file.get('maxdischargecurrentstatetopic', 'path/to/mqtt/battery/maxdischargecurrent')
        max_discharge_current_state_topic = input(f"Enter MQTT max discharge current state topic for Virtual Battery {i} (current: {current_max_discharge_current_state_topic}): ")
        config.set(virtual_battery_section, 'maxdischargecurrentstatetopic', max_discharge_current_state_topic if max_discharge_current_state_topic else current_max_discharge_current_state_topic)

        current_soc_state_topic = battery_data_from_file.get('socstatetopic', 'path/to/mqtt/battery/soc')
        soc_state_topic = input(f"Enter MQTT SOC state topic for Virtual Battery {i} (current: {current_soc_state_topic}): ")
        config.set(virtual_battery_section, 'socstatetopic', soc_state_topic if soc_state_topic else current_soc_state_topic)

        current_soh_state_topic = battery_data_from_file.get('sohstatetopic', 'path/to/mqtt/battery/soh')
        soh_state_topic = input(f"Enter MQTT SOH state topic for Virtual Battery {i} (current: {current_soh_state_topic}): ")
        config.set(virtual_battery_section, 'sohstatetopic', soh_state_topic if soh_state_topic else current_soh_state_topic)


    if not config.has_section('MQTT'):
        config.add_section('MQTT')

    config.set('MQTT', 'brokeraddress', broker_address)
    config.set('MQTT', 'port', str(port))
    config.set('MQTT', 'username', username if username is not None else '')
    config.set('MQTT', 'password', password if password is not None else '')

    print("\n--- Cleaning up unused sections ---")
    all_sections_in_current_config = set(config.sections())
    sections_to_remove = set()

    expected_relay_modules = {f'Relay_Module_{i}' for i in range(1, new_num_relay_modules + 1)}
    for section in all_sections_in_current_config:
        if section.startswith('Relay_Module_') and section not in expected_relay_modules:
            sections_to_remove.add(section)
            try:
                module_idx_to_remove = int(section.split('_')[2])
                for sub_section in all_sections_in_current_config:
                    if sub_section.startswith(f'switch_{module_idx_to_remove}_') or sub_section.startswith(f'input_{module_idx_to_remove}_'):
                        sections_to_remove.add(sub_section)
            except (ValueError, IndexError):
                pass

    for i in range(1, new_num_relay_modules + 1):
        if config.has_section(f'Relay_Module_{i}'):
            num_switches_for_module = config.getint(f'Relay_Module_{i}', 'numberofswitches', fallback=0)
            expected_switches_for_this_module = {f'switch_{i}_{j}' for j in range(1, num_switches_for_module + 1)}
            for section in all_sections_in_current_config:
                if section.startswith(f'switch_{i}_') and section not in expected_switches_for_this_module:
                    sections_to_remove.add(section)

            num_inputs_for_module = config.getint(f'Relay_Module_{i}', 'numberofinputs', fallback=0)
            expected_inputs_for_this_module = {f'input_{i}_{k}' for k in range(1, num_inputs_for_module + 1)}
            for section in all_sections_in_current_config:
                if section.startswith(f'input_{i}_') and section not in expected_inputs_for_this_module:
                    sections_to_remove.add(section)

    expected_temp_sensors = {f'Temp_Sensor_{i}' for i in range(1, num_temp_sensors + 1)}
    for section in all_sections_in_current_config:
        if section.startswith('Temp_Sensor_') and section not in expected_temp_sensors:
            sections_to_remove.add(section)

    expected_tank_sensors = {f'Tank_Sensor_{i}' for i in range(1, num_tank_sensors + 1)}
    for section in all_sections_in_current_config:
        if section.startswith('Tank_Sensor_') and section not in expected_tank_sensors:
            sections_to_remove.add(section)

    expected_virtual_batteries = {f'Virtual_Battery_{i}' for i in range(1, num_virtual_batteries + 1)}
    for section in all_sections_in_current_config:
        if section.startswith('Virtual_Battery_') and section not in expected_virtual_batteries:
            sections_to_remove.add(section)

    for section_to_remove in sections_to_remove:
        if config.has_section(section_to_remove):
            config.remove_section(section_to_remove)
            print(f"Removed unused section: {section_to_remove}")

    for section in all_sections_in_current_config:
        if section.startswith('NEW_MODULE_'):
            if config.has_section(section):
                config.remove_section(section)
                print(f"Removed placeholder section: {section}")


    with open(config_path, 'w') as configfile:
        config.write(configfile)
    print(f"\nconfig successfully created/updated at {config_path}")

    while True:
        print("\n--- Service Options ---")
        print("1) Install and activate service (system will reboot)")
        print("2) Restart service (system will reboot)")
        print("3) Quit and exit")

        choice = input("Enter your choice (1, 2, or 3): ")

        if choice == '1':
            print("Running: /data/venus-os_virtual-devices/setup install")
            try:
                subprocess.run(['/data/venus-os_virtual-devices/setup', 'install'], check=True)
                print("Service installed and activated successfully. Rebooting system...")
                subprocess.run(['reboot'], check=True)
            except subprocess.CalledProcessError as e:
                logger.error(f"Error installing service or rebooting: {e}")
            except FileNotFoundError:
                logger.error("Error: '/data/venus-os_virtual-devices/setup' command not found. Please ensure the setup script exists.")
            break
        elif choice == '2':
            print("Rebooting system...")
            try:
                subprocess.run(['reboot'], check=True)
            except subprocess.CalledProcessError as e:
                logger.error(f"Error rebooting system: {e}")
            except FileNotFoundError:
                logger.error("Error: 'sudo' command not found. Please ensure sudo is in your PATH.")
            break
        elif choice == '3':
            print("Exiting script.")
            break
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

if __name__ == "__main__":
    create_or_edit_config()