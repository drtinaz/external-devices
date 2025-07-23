#!/usr/bin/env python3
import configparser
import os
import random
import subprocess
import paho.mqtt.client as mqtt
import time
import re
import logging # Import the logging module
import sys     # Import sys for stdout logging

# Configure logging for this script itself
# This level applies to config.py's own output and is independent of the 'loglevel' saved in the config file.
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global variable to store discovered topics and device info
# Will store { "module_serial": {"device_type": "dingtian/shelly", "topics": set(), "base_topic_path": "..."} }
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

    # Dingtian Regex: Flexible 'dingtian' path segment, then 'relay[alphanumeric]', then optional path, then 'out'|'in', then 'r[digits]'
    # Group 1: path_segment_with_dingtian (e.g., 'dingtian' or 'home/automation/dingtian_xyz')
    # Group 2: module_serial (e.g., 'relay1a76f')
    # Group 3: relay_type ('out' or 'in')
    # Group 4: relay_number (digits after 'r')
    dingtian_match = re.search(r'(?:^|.*/)([a-zA-Z0-9_-]*dingtian[a-zA-Z0-9_-]*)/(relay[a-zA-Z0-9]+)/(?:.*/)?(out|in)/r([0-9]+)$', topic)
    if dingtian_match:
        path_segment_with_dingtian = dingtian_match.group(1)
        module_serial = dingtian_match.group(2)
        full_topic_base = f"{path_segment_with_dingtian}/{module_serial}"
        logger.debug(f"Matched Dingtian: Type=dingtian, Serial={module_serial}, ComponentType={dingtian_match.group(3)}, ComponentID={dingtian_match.group(4)}, Base={full_topic_base}")
        return 'dingtian', module_serial, dingtian_match.group(3), dingtian_match.group(4), full_topic_base

    # Shelly Regex (Updated for broader path matching and case-insensitivity)
    # Group 1: The module_serial (e.g., 'shellyplus1pm-08f9e0fe4034')
    # Group 2: The switch ID (digit after switch:)
    shelly_match = re.search(r'(?:^|.*/)(shelly[a-zA-Z0-9_-]+)(?:/.*)?/status/switch:([0-9]+)$', topic, re.IGNORECASE)
    if shelly_match:
        module_serial = shelly_match.group(1) # e.g., 'shellyplus1pm-08f9e0fe4034'
        full_topic_base = module_serial # The device ID itself forms the base for Shelly
        component_id = shelly_match.group(2) # e.g., '0' or '1'
        component_type = 'relay' # This pattern is for relays/switches
        logger.debug(f"Matched Shelly: Type=shelly, Serial={module_serial}, ComponentType={component_type}, ComponentID={component_id}, Base={full_topic_base}")
        return 'shelly', module_serial, component_type, component_id, full_topic_base

    logger.debug("No Dingtian or Shelly pattern matched.")
    return None, None, None, None, None

def on_connect(client, userdata, flags, rc):
    logger.info(f"Connected to MQTT broker with result code {rc}")
    # Subscribe to a wildcard topic '#' to catch all messages.
    client.subscribe("#")
    logger.info("Subscribed to '#' for device discovery, will filter for 'dingtian' or 'shelly' in topics.")

def on_message(client, userdata, msg):
    """Callback for when a PUBLISH message is received from the server."""
    topic = msg.topic
    logger.debug(f"Received MQTT message on topic: {topic}")
    device_type, module_serial, component_type, component_id, full_topic_base = parse_mqtt_device_topic(topic)

    if module_serial: # Ensure a module serial was found
        if module_serial not in discovered_modules_and_topics_global:
            discovered_modules_and_topics_global[module_serial] = {
                "device_type": device_type,
                "topics": set(),
                "base_topic_path": full_topic_base # Store the base topic path
            }
            logger.debug(f"Discovered new module: {device_type} with serial {module_serial}")
        discovered_modules_and_topics_global[module_serial]["topics"].add(topic)
        logger.debug(f"Added topic {topic} to module {module_serial}")
    else:
        logger.debug(f"Topic '{topic}' did not match any known device patterns.")

# --- Modified Function for MQTT Connection and Discovery ---
def get_mqtt_broker_info(current_broker_address=None, current_port=None, current_username=None, current_password=None):
    """Prompts user for MQTT broker details, showing existing values as defaults."""
    logger.info("\n--- MQTT Broker Configuration ---")

    # Use fallback 'not set' or specific defaults for display
    broker_address = input(f"Enter MQTT broker address (current: {current_broker_address if current_broker_address else 'not set'}): ") or (current_broker_address if current_broker_address else '')
    port = input(f"Enter MQTT port (current: {current_port if current_port else '1883'}): ") or (current_port if current_port else '1883')
    username = input(f"Enter MQTT username (current: {current_username if current_username else 'not set'}; leave blank if none): ") or (current_username if current_username else '')

    # Mask password if existing, only show 'not set' or '******'
    password_display = '******' if current_password else 'not set'
    password = input(f"Enter MQTT password (current: {password_display}; leave blank if none): ") or (current_password if current_password else '')

    return broker_address, int(port), username if username else None, password if password else None

def discover_devices_via_mqtt(client):
    """
    Connects to MQTT broker and attempts to discover Dingtian and Shelly devices by listening to topics.
    """
    global discovered_modules_and_topics_global
    discovered_modules_and_topics_global.clear() # Clear any previous discovery results

    logger.info("\nAttempting to discover Dingtian and Shelly devices via MQTT by listening to topics...")
    logger.info(" (This requires devices to be actively publishing data on topics containing 'dingtian' or 'shelly'.)")

    client.on_connect = on_connect
    client.on_message = on_message

    client.loop_start() # Start the non-blocking loop in a separate thread

    # Give some time for messages to arrive and be processed
    discovery_duration = 60 # seconds (Increased from 30 to 60)
    logger.info(f"Listening for messages for {discovery_duration} seconds...")
    time.sleep(discovery_duration) # Wait for messages

    client.loop_stop() # Stop the loop

    logger.info(f"Found {len(discovered_modules_and_topics_global)} potential Dingtian/Shelly modules.")
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

    original_num_relay_modules = 0 # Initialize for new configurations
    existing_relay_module_serials = [] # To store serials from existing config

    # Initialize highest_existing_device_instance to a value lower than any possible device instance
    # so that 100 is chosen if no devices exist.
    highest_existing_device_instance = 99
    # Initialize highest_existing_device_index for the new sequencing requirement
    highest_existing_device_index = 0

    # Variables to hold existing MQTT settings
    existing_mqtt_broker = ''
    existing_mqtt_port = '1883'
    existing_mqtt_username = ''
    existing_mqtt_password = ''
    current_loglevel_from_config = 'INFO' # Default for config file loglevel

    if file_exists:
        logger.info(f"Existing config file found at {config_path}.")
        while True:
            logger.info("\n--- Configuration Options ---")
            logger.info("1) Continue to configuration (update existing)")
            logger.info("2) Create new configuration (WARNING: Existing configuration will be overwritten!)") # New option 2
            logger.info("3) Delete existing configuration and exit (WARNING: This cannot be undone!)") # Old option 2, now option 3

            choice = input("Enter your choice (1, 2 or 3): ") # Updated prompt

            if choice == '1':
                config.read(config_path)
                # Capture original number of relay modules for conditional discovery
                original_num_relay_modules = config.getint('Global', 'numberofmodules', fallback=0)
                current_loglevel_from_config = config.get('Global', 'loglevel', fallback='INFO') # Read loglevel from existing config

                # Load existing MQTT settings
                existing_mqtt_broker = config.get('MQTT', 'brokeraddress', fallback='')
                existing_mqtt_port = config.get('MQTT', 'port', fallback='1883')
                existing_mqtt_username = config.get('MQTT', 'username', fallback='')
                existing_mqtt_password = config.get('MQTT', 'password', fallback='')

                # New: Extract existing relay module serials and determine max index
                for section in config.sections():
                    # Check for highest device instance
                    if config.has_option(section, 'deviceinstance'):
                        try:
                            instance = config.getint(section, 'deviceinstance')
                            if instance > highest_existing_device_instance:
                                highest_existing_device_instance = instance
                        except ValueError:
                            pass # Ignore if deviceinstance is not a valid integer

                    # Check for highest device index
                    if config.has_option(section, 'deviceindex'):
                        try:
                            index = config.getint(section, 'deviceindex')
                            if index > highest_existing_device_index:
                                highest_existing_device_index = index
                        except ValueError:
                            pass # Ignore if deviceindex is not a valid integer

                    if section.startswith('Relay_Module_'):
                        serial = config.get(section, 'serial', fallback=None)
                        if serial:
                            existing_relay_module_serials.append(serial)

                logger.info("Continuing to update existing configuration.")
                break
            elif choice == '2': # New option: Create new configuration
                confirm = input("Are you absolutely sure you want to overwrite the existing configuration file? This cannot be undone! (yes/no): ")
                if confirm.lower() == 'yes':
                    os.remove(config_path)
                    logger.info(f"Existing configuration file deleted: {config_path}")
                    file_exists = False # Set to False to proceed with new configuration flow
                    config = configparser.ConfigParser() # Clear in-memory config
                    break # Exit the loop to proceed to new config creation
                else:
                    logger.info("Creation of new configuration cancelled.")
            elif choice == '3': # Old option 2, now option 3
                confirm = input("Are you absolutely sure you want to delete the configuration file? This cannot be undone! (yes/no): ")
                if confirm.lower() == 'yes':
                    os.remove(config_path)
                    logger.info(f"Configuration file deleted: {config_path}")
                else:
                    logger.info("Deletion cancelled.")
                logger.info("Exiting script.")
                return
            else:
                logger.info("Invalid choice. Please enter 1, 2 or 3.")
    else:
        logger.info(f"No existing config file found. A new one will be created at {config_path}.")

    # Initialize device_instance_counter based on discovery of existing instances
    device_instance_counter = highest_existing_device_instance + 1
    # Initialize device_index_sequencer based on discovery of existing indices
    device_index_sequencer = highest_existing_device_index + 1

    # --- Global settings - ALL prompted first ---
    if not config.has_section('Global'):
        config.add_section('Global')

    # Prompt for loglevel, this applies only to the setting saved in the config file.
    current_loglevel_prompt_default = config.get('Global', 'loglevel', fallback='INFO')
    loglevel = input(f"Enter log level (options: DEBUG, INFO, WARNING, ERROR, CRITICAL; default: {current_loglevel_prompt_default}): ") or current_loglevel_prompt_default
    config.set('Global', 'loglevel', loglevel)

    # Prompt for number of relay modules
    default_num_relay_modules_initial = 1 if not file_exists else 0
    current_num_relay_modules_setting = config.getint('Global', 'numberofmodules', fallback=default_num_relay_modules_initial)
    while True:
        try:
            num_relay_modules_input = input(f"Enter the number of relay modules (current: {current_num_relay_modules_setting if current_num_relay_modules_setting > 0 else 'not set'}): ")
            if num_relay_modules_input:
                new_num_relay_modules = int(num_relay_modules_input)
                if new_num_relay_modules < 0: # Allow 0 for skipping modules
                    raise ValueError
                break
            elif current_num_relay_modules_setting > 0:
                new_num_relay_modules = current_num_relay_modules_setting
                break
            elif not file_exists:
                new_num_relay_modules = default_num_relay_modules_initial
                break
            else:
                logger.info("Invalid input. Please enter a non-negative integer for the number of relay modules.")
        except ValueError:
            logger.info("Invalid input. Please enter a non-negative integer for the number of relay modules.")
    config.set('Global', 'numberofmodules', str(new_num_relay_modules))

    # Remaining global settings for other device types
    default_num_temp_sensors_initial = 0 if not file_exists else 0
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
                logger.info("Invalid input. Please enter a non-negative integer for the number of temperature sensors.")
        except ValueError:
            logger.info("Invalid input. Please enter a non-negative integer for the number of temperature sensors.")
    config.set('Global', 'numberoftempsensors', str(num_temp_sensors))

    default_num_tank_sensors_initial = 0 if not file_exists else 0
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
                logger.info("Invalid input. Please enter a non-negative integer for the number of tank sensors.")
        except ValueError:
            logger.info("Invalid input. Please enter a non-negative integer for the number of tank sensors.")
    config.set('Global', 'numberoftanksensors', str(num_tank_sensors))

    default_num_virtual_batteries_initial = 0 if not file_exists else 0
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
                logger.info("Invalid input. Please enter a non-negative integer for the number of virtual batteries.")
        except ValueError:
            logger.info("Invalid input. Please enter a non-negative integer for the number of virtual batteries.")
    config.set('Global', 'numberofvirtualbatteries', str(num_virtual_batteries))


    # --- MQTT Broker Info (Always Prompted with defaults) ---
    broker_address, port, username, password = get_mqtt_broker_info(
        current_broker_address=existing_mqtt_broker,
        current_port=existing_mqtt_port,
        current_username=existing_mqtt_username,
        current_password=existing_mqtt_password
    )

    # --- Determine if Dingtian/Shelly module discovery should be attempted ---
    should_attempt_discovery = False
    if not file_exists: # New configuration
        if new_num_relay_modules > 0:
            should_attempt_discovery = True
    else: # Editing existing configuration
        # Only attempt discovery if the number of modules has *increased*
        if new_num_relay_modules > original_num_relay_modules:
            should_attempt_discovery = True

    # Initialize auto_configured_module_serials here, before the discovery block
    # This list will hold serials of modules auto-configured *in the current run*.
    auto_configured_module_serials = []

    if should_attempt_discovery:
        if new_num_relay_modules > 0: # Only attempt discovery if user wants at least one relay module
            while True:
                discovery_choice = input("\nDo you want to try to discover Dingtian/Shelly modules via MQTT, or proceed to manual configuration? (discover/manual): ").lower()
                if discovery_choice in ['discover', 'manual']:
                    break
                else:
                    logger.info("Invalid choice. Please enter 'discover' or 'manual'.")

            if discovery_choice == 'discover':
                mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
                if username:
                    mqtt_client.username_pw_set(username, password)

                try:
                    logger.info(f"Connecting to MQTT broker at {broker_address}:{port}...")
                    mqtt_client.connect(broker_address, port, 60)
                    logger.info("Connected to MQTT broker.")

                    all_discovered_modules_with_topics = discover_devices_via_mqtt(mqtt_client) # Call generic discovery

                    mqtt_client.disconnect()
                    logger.info("Disconnected from MQTT broker.")

                    # New: Filter out already configured modules if editing existing config
                    newly_discovered_modules_with_topics = {}
                    skipped_modules_count = 0
                    if file_exists:
                        for module_serial, module_info in all_discovered_modules_with_topics.items():
                            if module_serial not in existing_relay_module_serials: # Check against all existing relay module serials
                                newly_discovered_modules_with_topics[module_serial] = module_info
                            else:
                                skipped_modules_count += 1
                        if skipped_modules_count > 0:
                            logger.info(f"\nSkipped {skipped_modules_count} discovered modules as they are already configured.")
                    else: # If it's a new config, all discovered modules are "new"
                        newly_discovered_modules_with_topics = all_discovered_modules_with_topics


                    if newly_discovered_modules_with_topics:
                        logger.info("\n--- Newly Discovered Modules (by Serial Number) ---")
                        # Sort for consistent display
                        discovered_module_serials_list = sorted(list(newly_discovered_modules_with_topics.keys()))
                        for i, module_serial in enumerate(discovered_module_serials_list):
                            module_info = newly_discovered_modules_with_topics[module_serial]
                            logger.info(f"{i+1}) Device Type: {module_info['device_type'].capitalize()}, Module Serial: {module_serial}")

                        selected_indices_input = input("Enter the numbers of the modules you want to auto-configure (e.g., 1,3,4 or 'all'): ")

                        if selected_indices_input.lower() == 'all':
                            auto_configured_module_serials = discovered_module_serials_list
                        else:
                            try:
                                indices = [int(x.strip()) - 1 for x in selected_indices_input.split(',')]
                                for idx in indices:
                                    if 0 <= idx < len(discovered_module_serials_list):
                                        auto_configured_module_serials.append(discovered_module_serials_list[idx])
                                    else:
                                        logger.info(f"Warning: Invalid selection number {idx+1} ignored.")
                            except ValueError:
                                logger.info("Invalid input for selection. No specific modules selected for auto-configuration.")

                        if auto_configured_module_serials:
                            logger.info(f"\n--- Automatically Configuring Selected Modules ---")

                            for module_serial in auto_configured_module_serials:
                                module_info = newly_discovered_modules_with_topics[module_serial]
                                device_type = module_info['device_type']
                                module_topics = module_info['topics']
                                base_topic_path = module_info['base_topic_path'] # Get base topic path

                                # Use serial for section name, strip 'relay' or 'shelly' part for a cleaner name if desired, but serial itself is unique
                                # For consistency with existing Dingtian sections, we'll use Relay_Module_ and then the serial
                                relay_module_section_name = f'Relay_Module_{module_serial}'
                                if not config.has_section(relay_module_section_name):
                                    config.add_section(relay_module_section_name)

                                config.set(relay_module_section_name, 'deviceinstance', str(device_instance_counter))
                                device_instance_counter += 1 # Increment for next device
                                config.set(relay_module_section_name, 'customname', f'{device_type.capitalize()} Module {module_serial}')
                                config.set(relay_module_section_name, 'serial', module_serial)
                                config.set(relay_module_section_name, 'deviceindex', str(device_index_sequencer)) # Set deviceindex as a sequential number
                                device_index_sequencer += 1 # Increment sequencer for next device


                                found_relay_numbers = set()
                                if device_type == 'dingtian':
                                    config.set(relay_module_section_name, 'mqtt_on_state_payload', 'ON')
                                    config.set(relay_module_section_name, 'mqtt_off_state_payload', 'OFF')
                                    config.set(relay_module_section_name, 'mqtt_on_command_payload', 'ON')
                                    config.set(relay_module_section_name, 'mqtt_off_command_payload', 'OFF')

                                    for topic in module_topics:
                                        _, _, relay_type, relay_num, _ = parse_mqtt_device_topic(topic)
                                        if relay_type == 'out' and relay_num: # Only count 'out' topics for Dingtian switches
                                            found_relay_numbers.add(int(relay_num))

                                elif device_type == 'shelly':
                                    # Updated state payloads as per user's last request
                                    config.set(relay_module_section_name, 'mqtt_on_state_payload', '{"output": true}')
                                    config.set(relay_module_section_name, 'mqtt_off_state_payload', '{"output": false}')
                                    config.set(relay_module_section_name, 'mqtt_on_command_payload', 'on') # Command payloads are plain strings
                                    config.set(relay_module_section_name, 'mqtt_off_command_payload', 'off') # Command payloads are plain strings

                                    for topic in module_topics:
                                        _, _, component_type, component_id, _ = parse_mqtt_device_topic(topic)
                                        if component_type == 'relay' and component_id: # Count 'relay' topics for Shelly switches
                                            found_relay_numbers.add(int(component_id))

                                num_switches_for_module = len(found_relay_numbers)
                                config.set(relay_module_section_name, 'numberofswitches', str(num_switches_for_module))
                                logger.info(f"  Configuring {device_type.capitalize()} Module {module_serial} with {num_switches_for_module} switches.")

                                for j_raw in sorted(list(found_relay_numbers)): # Ensure switches are ordered
                                    switch_section_name = f'switch_{module_serial}_{j_raw}' # Use serial for switch section name consistency
                                    if not config.has_section(switch_section_name):
                                        config.add_section(switch_section_name)

                                    config.set(switch_section_name, 'customname', f'Relay {j_raw} of {module_serial}')
                                    config.set(switch_section_name, 'group', f'Group {module_serial}')

                                    if device_type == 'dingtian':
                                        # Find an example state topic for this Dingtian module and relay number
                                        state_topic_example = None
                                        for t in module_topics:
                                            # Adjusted to use only relevant parts from parse_mqtt_device_topic
                                            parsed_type, parsed_serial, parsed_comp_type, parsed_comp_id, _ = parse_mqtt_device_topic(t)
                                            if parsed_serial == module_serial and parsed_comp_type == 'out' and parsed_comp_id == str(j_raw):
                                                state_topic_example = t
                                                break

                                        if state_topic_example:
                                            state_topic = state_topic_example
                                            command_topic = state_topic.replace('/out/r', '/in/r', 1) # Replace first instance
                                        else:
                                            # Fallback (shouldn't happen with robust discovery)
                                            state_topic = f'{base_topic_path}/out/r{j_raw}'
                                            command_topic = f'{base_topic_path}/in/r{j_raw}'
                                    elif device_type == 'shelly':
                                        # State topic: DEVICE_ID/status/switch:X
                                        state_topic = f'{base_topic_path}/status/switch:{j_raw}'
                                        # Command topic: Inferred by replacing 'status' with 'command' as per user's last request
                                        command_topic = state_topic.replace('/status/', '/command/', 1)


                                    config.set(switch_section_name, 'mqttstatetopic', state_topic)
                                    config.set(switch_section_name, 'mqttcommandtopic', command_topic)

                        else:
                            logger.info("\nNo specific modules selected for auto-configuration.")

                    else:
                        logger.info("\nNo new Dingtian or Shelly modules found via MQTT topic discovery to auto-configure.")

                except Exception as e:
                    logger.error(f"\nCould not connect to MQTT broker or perform discovery: {e}")
                    logger.info("Proceeding with manual configuration without MQTT discovery.")
            else:
                logger.info("\nSkipping MQTT discovery.")
        else:
            logger.info("\nNumber of relay modules is 0. Skipping Dingtian/Shelly module discovery (no modules to configure).")
    else:
        logger.info("\nSkipping Dingtian/Shelly module discovery (number of relay modules not increased from previous setting or is 0 for new config).")

    # Relay module settings - this loop will now start from 1 to allow editing all modules
    # This loop should only handle *manual* configuration of modules.
    # Auto-configured modules are handled in the discovery block.
    # The serials in `auto_configured_module_serials` are the ones we just added automatically.

    sections_to_process_manually = []

    # 1. Collect existing modules that were NOT auto-configured in this run for potential manual editing
    for section in config.sections():
        if section.startswith('Relay_Module_'):
            serial_in_section = config.get(section, 'serial', fallback=None)
            if serial_in_section not in auto_configured_module_serials:
                sections_to_process_manually.append(section) # These are existing sections that user might want to edit manually

    # 2. Determine how many *more* modules are needed to reach new_num_relay_modules
    # Count how many modules are currently in the config (auto-configured + existing non-auto-configured)
    actual_current_module_count = len([s for s in config.sections() if s.startswith('Relay_Module_')])

    # Add placeholders for new modules needed up to `new_num_relay_modules`
    modules_to_add_manually = max(0, new_num_relay_modules - actual_current_module_count)

    for i in range(1, modules_to_add_manually + 1):
        sections_to_process_manually.append(f'NEW_MODULE_{i}')

    # Now, iterate through the list of sections to process manually.
    manual_module_idx = 1
    for section_name_candidate in sections_to_process_manually:
        is_new_module_placeholder = section_name_candidate.startswith('NEW_MODULE_')

        # Determine the *final* section name we will work with for this module
        current_relay_module_section = ""

        if is_new_module_placeholder:
            # For a new module, generate a serial and use it for the section name immediately
            newly_generated_serial = generate_serial()
            current_relay_module_section = f'Relay_Module_{newly_generated_serial}'
            if not config.has_section(current_relay_module_section):
                config.add_section(current_relay_module_section)
            config.set(current_relay_module_section, 'serial', newly_generated_serial)
            logger.info(f"\n--- Configuring NEW Relay Module {manual_module_idx} (Serial: {newly_generated_serial}) ---")
        else:
            # For an existing module, use its current section name
            current_relay_module_section = section_name_candidate
            logger.info(f"\n--- Configuring EXISTING Relay Module {manual_module_idx} ({current_relay_module_section}) ---")

        # Ensure the serial is set for this section (might be missing for existing sections if they didn't have one)
        current_serial = config.get(current_relay_module_section, 'serial', fallback='')
        if not current_serial:
            # This case should ideally not happen if serials are always set, but as a fallback
            current_serial = generate_serial()
            logger.info(f"Generated new serial for {current_relay_module_section}: {current_serial}")
            config.set(current_relay_module_section, 'serial', current_serial)
        else:
            logger.info(f"Using existing serial for {current_relay_module_section}: {current_serial}")
            config.set(current_relay_module_section, 'serial', current_serial)

        # Set deviceindex for ALL modules using the sequencer
        config.set(current_relay_module_section, 'deviceindex', str(device_index_sequencer))
        device_index_sequencer += 1 # Increment sequencer for next device


        # Use current_relay_module_section for all subsequent operations in this loop iteration

        current_device_instance = config.getint(current_relay_module_section, 'deviceinstance', fallback=device_instance_counter)
        device_instance_input = input(f"Enter device instance for Relay Module {manual_module_idx} (current: {current_device_instance}): ")
        config.set(current_relay_module_section, 'deviceinstance', device_instance_input if device_instance_input else str(current_device_instance))
        device_instance_counter = int(config.get(current_relay_module_section, 'deviceinstance')) + 1 # Increment for next device

        current_custom_name = config.get(current_relay_module_section, 'customname', fallback=f'Relay Module {manual_module_idx}')
        custom_name = input(f"Enter custom name for Relay Module {manual_module_idx} (current: {current_custom_name}): ")
        config.set(current_relay_module_section, 'customname', custom_name if custom_name else current_custom_name)

        default_num_switches_initial = 2 # Default for a brand new module if not loaded from config
        current_num_switches = config.getint(current_relay_module_section, 'numberofswitches', fallback=default_num_switches_initial)
        while True:
            try:
                # Corrected variable name: num_switches_input
                num_switches_input = input(f"Enter the number of switches for Relay Module {manual_module_idx} (current: {current_num_switches if current_num_switches > 0 else 'not set'}): ")
                if num_switches_input:
                    num_switches = int(num_switches_input)
                    if num_switches <= 0:
                        raise ValueError
                    break
                elif current_num_switches > 0: # Use existing if valid
                    num_switches = current_num_switches
                    break
                elif is_new_module_placeholder: # For a brand new section being added
                    num_switches = default_num_switches_initial
                    break
                else:
                    logger.info("Invalid input. Please enter a positive integer for the number of switches.")
            except ValueError:
                logger.info("Invalid input. Please enter a positive integer for the number of switches.")
        config.set(current_relay_module_section, 'numberofswitches', str(num_switches))

        current_mqtt_on_state_payload = config.get(current_relay_module_section, 'mqtt_on_state_payload', fallback='ON')
        mqtt_on_state_payload = input(f"Enter MQTT ON state payload for Relay Module {manual_module_idx} (current: {current_mqtt_on_state_payload}): ")
        config.set(current_relay_module_section, 'mqtt_on_state_payload', mqtt_on_state_payload if mqtt_on_state_payload else current_mqtt_on_state_payload)

        current_mqtt_off_state_payload = config.get(current_relay_module_section, 'mqtt_off_state_payload', fallback='OFF')
        mqtt_off_state_payload = input(f"Enter MQTT OFF state payload for Relay Module {manual_module_idx} (current: {current_mqtt_off_state_payload}): ")
        config.set(current_relay_module_section, 'mqtt_off_state_payload', mqtt_off_state_payload if mqtt_off_state_payload else current_mqtt_off_state_payload)

        current_mqtt_on_command_payload = config.get(current_relay_module_section, 'mqtt_on_command_payload', fallback='ON')
        mqtt_on_command_payload = input(f"Enter MQTT ON command payload for Relay Module {manual_module_idx} (current: {current_mqtt_on_command_payload}): ")
        config.set(current_relay_module_section, 'mqtt_on_command_payload', mqtt_on_command_payload if mqtt_on_command_payload else current_mqtt_on_command_payload)

        current_mqtt_off_command_payload = config.get(current_relay_module_section, 'mqtt_off_command_payload', fallback='OFF')
        mqtt_off_command_payload = input(f"Enter MQTT OFF command payload for Relay Module {manual_module_idx} (current: {current_mqtt_off_command_payload}): ")
        config.set(current_relay_module_section, 'mqtt_off_command_payload', mqtt_off_command_payload if mqtt_off_command_payload else current_mqtt_off_command_payload)


        # Get the serial associated with this module for consistent switch naming
        module_base_serial_for_switches = config.get(current_relay_module_section, 'serial', fallback='')

        for j in range(1, num_switches + 1):
            # Use the consistent naming convention for switch sections
            switch_section = f'switch_{module_base_serial_for_switches}_{j}'
            if not config.has_section(switch_section):
                config.add_section(switch_section)

            current_switch_custom_name = config.get(switch_section, 'customname', fallback=f'switch {j}')
            switch_custom_name = input(f"Enter custom name for Relay Module {manual_module_idx}, switch {j} (current: {current_switch_custom_name}): ")
            config.set(switch_section, 'customname', switch_custom_name if switch_custom_name else current_switch_custom_name)

            current_switch_group = config.get(switch_section, 'group', fallback=f'Group{manual_module_idx}')
            switch_group = input(f"Enter group for Relay Module {manual_module_idx}, switch {j} (current: {current_switch_group}): ")
            config.set(switch_section, 'group', switch_group if switch_group else current_switch_group)

            current_mqtt_state_topic = config.get(switch_section, 'mqttstatetopic', fallback='path/to/mqtt/topic')
            mqtt_state_topic = input(f"Enter MQTT state topic for Relay Module {manual_module_idx}, switch {j} (current: {current_mqtt_state_topic}): ")
            config.set(switch_section, 'mqttstatetopic', mqtt_state_topic if mqtt_state_topic else current_mqtt_state_topic)

            current_mqtt_command_topic = config.get(switch_section, 'mqttcommandtopic', fallback='path/to/mqtt/topic')
            mqtt_command_topic = input(f"Enter MQTT command topic for Relay Module {manual_module_idx}, switch {j} (current: {current_mqtt_command_topic}): ")
            config.set(switch_section, 'mqttcommandtopic', mqtt_command_topic if mqtt_command_topic else current_mqtt_command_topic)

        manual_module_idx += 1 # Increment logical index for user prompt

    # Clean up sections that are no longer needed (e.g., if numberofmodules decreased)
    # This part remains a known limitation for this iteration as discussed previously.
    # The script currently doesn't remove sections when `new_num_relay_modules` is less than
    # the existing number of sections.

    # Temperature Sensor settings
    for i in range(1, num_temp_sensors + 1):
        temp_sensor_section = f'Temp_Sensor_{i}'
        if not config.has_section(temp_sensor_section):
            config.add_section(temp_sensor_section)

        current_device_instance = config.getint(temp_sensor_section, 'deviceinstance', fallback=device_instance_counter)
        device_instance_input = input(f"Enter device instance for Temperature Sensor {i} (current: {current_device_instance}): ")
        config.set(temp_sensor_section, 'deviceinstance', device_instance_input if device_instance_input else str(current_device_instance))
        device_instance_counter = int(config.get(temp_sensor_section, 'deviceinstance')) + 1

        config.set(temp_sensor_section, 'deviceindex', str(device_index_sequencer)) # Set deviceindex as a sequential number
        device_index_sequencer += 1 # Increment sequencer for next device

        current_custom_name = config.get(temp_sensor_section, 'customname', fallback=f'Temperature Sensor {i}')
        custom_name = input(f"Enter custom name for Temperature Sensor {i} (current: {current_custom_name}): ")
        config.set(temp_sensor_section, 'customname', custom_name if custom_name else current_custom_name)

        current_serial = config.get(temp_sensor_section, 'serial', fallback='')
        if not current_serial:
            new_serial = generate_serial()
            logger.info(f"No existing serial for Temperature Sensor {i}. Generating new serial: {new_serial}")
            config.set(temp_sensor_section, 'serial', new_serial)
        else:
            logger.info(f"Using existing serial for Temperature Sensor {i}: {current_serial}")
            config.set(temp_sensor_section, 'serial', current_serial)

        temp_sensor_types = ['battery', 'fridge', 'generic', 'room', 'outdoor', 'water heater', 'freezer']
        current_temp_sensor_type = config.get(temp_sensor_section, 'type', fallback='generic')
        while True:
            temp_type_input = input(f"Enter type for Temperature Sensor {i} (options: {', '.join(temp_sensor_types)}; current: {current_temp_sensor_type}): ")
            if temp_type_input:
                if temp_type_input.lower() in temp_sensor_types:
                    config.set(temp_sensor_section, 'type', temp_type_input.lower())
                    break
                else:
                    logger.info(f"Invalid type. Please choose from: {', '.join(temp_sensor_types)}")
            else:
                config.set(temp_sensor_section, 'type', current_temp_sensor_type)
                break

        current_temp_state_topic = config.get(temp_sensor_section, 'temperaturestatetopic', fallback='path/to/mqtt/temperature')
        temp_state_topic = input(f"Enter MQTT temperature state topic for Temperature Sensor {i} (current: {current_temp_state_topic}): ")
        config.set(temp_sensor_section, 'temperaturestatetopic', temp_state_topic if temp_state_topic else current_temp_state_topic)

        current_humidity_state_topic = config.get(temp_sensor_section, 'humiditystatetopic', fallback='path/to/mqtt/humidity')
        humidity_state_topic = input(f"Enter MQTT humidity state topic for Temperature Sensor {i} (current: {current_humidity_state_topic}): ")
        config.set(temp_sensor_section, 'humiditystatetopic', humidity_state_topic if humidity_state_topic else current_humidity_state_topic)

        current_battery_state_topic = config.get(temp_sensor_section, 'batterystatetopic', fallback='path/to/mqtt/battery')
        battery_state_topic = input(f"Enter MQTT battery state topic for Temperature Sensor {i} (current: {current_battery_state_topic}): ")
        config.set(temp_sensor_section, 'batterystatetopic', battery_state_topic if battery_state_topic else current_battery_state_topic)

    # Tank Sensor settings
    fluid_types_map = {
        'fuel': 0, 'fresh water': 1, 'waste water': 2, 'live well': 3,
        'oil': 4, 'black water': 5, 'gasoline': 6, 'diesel': 7,
        'lpg': 8, 'lng': 9, 'hydraulic oil': 10, 'raw water': 11
    }
    for i in range(1, num_tank_sensors + 1):
        tank_sensor_section = f'Tank_Sensor_{i}'
        if not config.has_section(tank_sensor_section):
            config.add_section(tank_sensor_section)

        current_device_instance = config.getint(tank_sensor_section, 'deviceinstance', fallback=device_instance_counter)
        device_instance_input = input(f"Enter device instance for Tank Sensor {i} (current: {current_device_instance}): ")
        config.set(tank_sensor_section, 'deviceinstance', device_instance_input if device_instance_input else str(current_device_instance))
        device_instance_counter = int(config.get(tank_sensor_section, 'deviceinstance')) + 1

        config.set(tank_sensor_section, 'deviceindex', str(device_index_sequencer)) # Set deviceindex as a sequential number
        device_index_sequencer += 1 # Increment sequencer for next device

        current_custom_name = config.get(tank_sensor_section, 'customname', fallback=f'Tank Sensor {i}')
        custom_name = input(f"Enter custom name for Tank Sensor {i} (current: {current_custom_name}): ")
        config.set(tank_sensor_section, 'customname', custom_name if custom_name else current_custom_name)

        current_serial = config.get(tank_sensor_section, 'serial', fallback='')
        if not current_serial:
            new_serial = generate_serial()
            logger.info(f"No existing serial for Tank Sensor {i}. Generating new serial: {new_serial}")
            config.set(tank_sensor_section, 'serial', new_serial)
        else:
            logger.info(f"Using existing serial for Tank Sensor {i}: {current_serial}")
            config.set(tank_sensor_section, 'serial', current_serial)

        current_level_state_topic = config.get(tank_sensor_section, 'levelstatetopic', fallback='path/to/mqtt/level')
        level_state_topic = input(f"Enter MQTT level state topic for Tank Sensor {i} (current: {current_level_state_topic}): ")
        config.set(tank_sensor_section, 'levelstatetopic', level_state_topic if level_state_topic else current_level_state_topic)

        current_battery_state_topic = config.get(tank_sensor_section, 'batterystatetopic', fallback='path/to/mqtt/battery')
        battery_state_topic = input(f"Enter MQTT battery state topic for Tank Sensor {i} (current: {current_battery_state_topic}): ")
        config.set(tank_sensor_section, 'batterystatetopic', battery_state_topic if battery_state_topic else current_battery_state_topic)

        current_temp_state_topic = config.get(tank_sensor_section, 'temperaturestatetopic', fallback='path/to/mqtt/temperature')
        temp_state_topic = input(f"Enter MQTT temperature state topic for Tank Sensor {i} (current: {current_temp_state_topic}): ")
        config.set(tank_sensor_section, 'temperaturestatetopic', temp_state_topic if temp_state_topic else current_temp_state_topic)

        current_raw_value_state_topic = config.get(tank_sensor_section, 'rawvaluestatetopic', fallback='path/to/mqtt/rawvalue')
        raw_value_state_topic = input(f"Enter MQTT raw value state topic for Tank Sensor {i} (current: {current_raw_value_state_topic}): ")
        config.set(tank_sensor_section, 'rawvaluestatetopic', raw_value_state_topic if raw_value_state_topic else current_raw_value_state_topic)

        fluid_types_display = ", ".join([f"'{name}'" for name in fluid_types_map.keys()])
        current_fluid_type_name = config.get(tank_sensor_section, 'fluidtype', fallback='fresh water')
        while True:
            fluid_type_input = input(f"Enter fluid type for Tank Sensor {i} (options: {fluid_types_display}; current: '{current_fluid_type_name}'): ")
            if fluid_type_input:
                if fluid_type_input.lower() in fluid_types_map:
                    config.set(tank_sensor_section, 'fluidtype', fluid_type_input.lower())
                    break
                else:
                    logger.info("Invalid fluid type. Please choose from the available options.")
            else:
                config.set(tank_sensor_section, 'fluidtype', current_fluid_type_name)
                break

        current_raw_value_empty = config.get(tank_sensor_section, 'rawvalueempty', fallback='0')
        config.set(tank_sensor_section, 'rawvalueempty', current_raw_value_empty)

        current_raw_value_full = config.get(tank_sensor_section, 'rawvaluefull', fallback='50')
        config.set(tank_sensor_section, 'rawvaluefull', current_raw_value_full)

        current_capacity = config.get(tank_sensor_section, 'capacity', fallback='0.2')
        config.set(tank_sensor_section, 'capacity', current_capacity)


    # Virtual Battery settings
    for i in range(1, num_virtual_batteries + 1):
        virtual_battery_section = f'Virtual_Battery_{i}'
        if not config.has_section(virtual_battery_section):
            config.add_section(virtual_battery_section)

        current_device_instance = config.getint(virtual_battery_section, 'deviceinstance', fallback=device_instance_counter)
        device_instance_input = input(f"Enter device instance for Virtual Battery {i} (current: {current_device_instance}): ")
        config.set(virtual_battery_section, 'deviceinstance', device_instance_input if device_instance_input else str(current_device_instance))
        device_instance_counter = int(config.get(virtual_battery_section, 'deviceinstance')) + 1

        config.set(virtual_battery_section, 'deviceindex', str(device_index_sequencer)) # Set deviceindex as a sequential number
        device_index_sequencer += 1 # Increment sequencer for next device

        current_custom_name = config.get(virtual_battery_section, 'customname', fallback=f'Virtual Battery {i}')
        custom_name = input(f"Enter custom name for Virtual Battery {i} (current: {current_custom_name}): ")
        config.set(virtual_battery_section, 'customname', custom_name if custom_name else current_custom_name)

        current_serial = config.get(virtual_battery_section, 'serial', fallback='')
        if not current_serial:
            new_serial = generate_serial()
            logger.info(f"No existing serial for Virtual Battery {i}. Generating new serial: {new_serial}")
            config.set(virtual_battery_section, 'serial', new_serial)
        else:
            logger.info(f"Using existing serial for Virtual Battery {i}: {current_serial}")
            config.set(virtual_battery_section, 'serial', current_serial)

        current_capacity = config.get(virtual_battery_section, 'capacityah', fallback='100')
        capacity = input(f"Enter capacity for Virtual Battery {i} in Ah (current: {current_capacity}): ")
        config.set(virtual_battery_section, 'capacityah', capacity if capacity else current_capacity)

        current_current_state_topic = config.get(virtual_battery_section, 'currentstatetopic', fallback='path/to/mqtt/battery/current')
        current_state_topic = input(f"Enter MQTT current state topic for Virtual Battery {i} (current: {current_current_state_topic}): ")
        config.set(virtual_battery_section, 'currentstatetopic', current_state_topic if current_state_topic else current_current_state_topic)

        current_power_state_topic = config.get(virtual_battery_section, 'powerstatetopic', fallback='path/to/mqtt/battery/power')
        power_state_topic = input(f"Enter MQTT power state topic for Virtual Battery {i} (current: {current_power_state_topic}): ")
        config.set(virtual_battery_section, 'powerstatetopic', power_state_topic if power_state_topic else current_power_state_topic)

        current_temperature_state_topic = config.get(virtual_battery_section, 'temperaturestatetopic', fallback='path/to/mqtt/battery/temperature')
        temperature_state_topic = input(f"Enter MQTT temperature state topic for Virtual Battery {i} (current: {current_temperature_state_topic}): ")
        config.set(virtual_battery_section, 'temperaturestatetopic', temperature_state_topic if temperature_state_topic else current_temperature_state_topic)

        current_voltage_state_topic = config.get(virtual_battery_section, 'voltagestatetopic', fallback='path/to/mqtt/battery/voltage')
        voltage_state_topic = input(f"Enter MQTT voltage state topic for Virtual Battery {i} (current: {current_voltage_state_topic}): ")
        config.set(virtual_battery_section, 'voltagestatetopic', voltage_state_topic if voltage_state_topic else current_voltage_state_topic)

        current_max_charge_current_state_topic = config.get(virtual_battery_section, 'maxchargecurrentstatetopic', fallback='path/to/mqtt/battery/maxchargecurrent')
        max_charge_current_state_topic = input(f"Enter MQTT max charge current state topic for Virtual Battery {i} (current: {current_max_charge_current_state_topic}): ")
        config.set(virtual_battery_section, 'maxchargecurrentstatetopic', max_charge_current_state_topic if max_charge_current_state_topic else current_max_charge_current_state_topic)

        current_max_charge_voltage_state_topic = config.get(virtual_battery_section, 'maxchargevoltagestatetopic', fallback='path/to/mqtt/battery/maxchargevoltage')
        max_charge_voltage_state_topic = input(f"Enter MQTT max charge voltage state topic for Virtual Battery {i} (current: {current_max_charge_voltage_state_topic}): ")
        config.set(virtual_battery_section, 'maxchargevoltagestatetopic', max_charge_voltage_state_topic if max_charge_voltage_state_topic else current_max_charge_voltage_state_topic)

        current_max_discharge_current_state_topic = config.get(virtual_battery_section, 'maxdischargecurrentstatetopic', fallback='path/to/mqtt/battery/maxdischargecurrent')
        max_discharge_current_state_topic = input(f"Enter MQTT max discharge current state topic for Virtual Battery {i} (current: {current_max_discharge_current_state_topic}): ")
        config.set(virtual_battery_section, 'maxdischargecurrentstatetopic', max_discharge_current_state_topic if max_discharge_current_state_topic else current_max_discharge_current_state_topic)

        current_soc_state_topic = config.get(virtual_battery_section, 'socstatetopic', fallback='path/to/mqtt/battery/soc')
        soc_state_topic = input(f"Enter MQTT SOC state topic for Virtual Battery {i} (current: {current_soc_state_topic}): ")
        config.set(virtual_battery_section, 'socstatetopic', soc_state_topic if soc_state_topic else current_soc_state_topic)

        current_soh_state_topic = config.get(virtual_battery_section, 'sohstatetopic', fallback='path/to/mqtt/battery/soh')
        soh_state_topic = input(f"Enter MQTT SOH state topic for Virtual Battery {i} (current: {current_soh_state_topic}): ")
        config.set(virtual_battery_section, 'sohstatetopic', soh_state_topic if soh_state_topic else current_soh_state_topic)

    # MQTT broker settings (these will now be populated with the values obtained earlier)
    if not config.has_section('MQTT'):
        config.add_section('MQTT')

    config.set('MQTT', 'brokeraddress', broker_address)
    config.set('MQTT', 'port', str(port))
    config.set('MQTT', 'username', username if username is not None else '')
    config.set('MQTT', 'password', password if password is not None else '')

    with open(config_path, 'w') as configfile:
        config.write(configfile)
    logger.info(f"\nconfig successfully created/updated at {config_path}")

    # Post-configuration menu
    while True:
        logger.info("\n--- Service Options ---")
        logger.info("1) Install and activate service (system will reboot)")
        logger.info("2) Restart service (system will reboot)")
        logger.info("3) Quit and exit")

        choice = input("Enter your choice (1, 2, or 3): ")

        if choice == '1':
            logger.info("Running: /data/venus-os_virtual-devices/setup install")
            try:
                subprocess.run(['/data/venus-os_virtual-devices/setup', 'install'], check=True)
                logger.info("Service installed and activated successfully. Rebooting system...")
                subprocess.run(['reboot'], check=True)
            except subprocess.CalledProcessError as e:
                logger.error(f"Error installing service or rebooting: {e}")
            except FileNotFoundError:
                logger.error("Error: '/data/venus-os_virtual-devices/setup' command not found. Please ensure the setup script exists.")
            break
        elif choice == '2':
            logger.info("Rebooting system...")
            try:
                subprocess.run(['reboot'], check=True)
            except subprocess.CalledProcessError as e:
                logger.error(f"Error rebooting system: {e}")
            except FileNotFoundError:
                logger.error("Error: 'sudo' command not found. Please ensure sudo is in your PATH.")
            break
        elif choice == '3':
            logger.info("Exiting script.")
            break
        else:
            logger.info("Invalid choice. Please enter 1, 2, or 3.")

if __name__ == "__main__":
    create_or_edit_config()
