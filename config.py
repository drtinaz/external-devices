#!/usr/bin/env python3
import configparser
import os
import random
import subprocess
import paho.mqtt.client as mqtt # Import the MQTT client library

# --- Existing functions (unchanged) ---
def generate_serial():
    """Generates a random 16-digit serial number."""
    return ''.join([str(random.randint(0, 9)) for _ in range(16)])

# --- Modified Function for MQTT Connection and Discovery ---
def get_mqtt_broker_info(current_broker_address=None, current_port=None, current_username=None, current_password=None):
    """Prompts user for MQTT broker details, showing existing values as defaults."""
    print("\n--- MQTT Broker Configuration ---")
    
    # Use fallback 'not set' or specific defaults for display
    broker_address = input(f"Enter MQTT broker address (current: {current_broker_address if current_broker_address else 'not set'}): ") or (current_broker_address if current_broker_address else '')
    port = input(f"Enter MQTT port (current: {current_port if current_port else '1883'}): ") or (current_port if current_port else '1883')
    username = input(f"Enter MQTT username (current: {current_username if current_username else 'not set'}; leave blank if none): ") or (current_username if current_username else '')
    
    # Mask password if existing, only show 'not set' or '******'
    password_display = '******' if current_password else 'not set'
    password = input(f"Enter MQTT password (current: {password_display}; leave blank if none): ") or (current_password if current_password else '')

    return broker_address, int(port), username if username else None, password if password else None

def discover_dingtian_devices_via_mqtt(client):
    """
    Connects to MQTT broker and attempts to discover Dingtian devices.
    (This is a placeholder and needs specific MQTT topic knowledge for Dingtian devices)
    """
    print("\nAttempting to discover Dingtian devices via MQTT...")
    found_devices = {}
    
    # --- REAL-WORLD MQTT DISCOVERY LOGIC WOULD GO HERE ---
    # This is the complex part that depends on how Dingtian devices announce themselves.
    # Examples:
    # 1. Subscribing to $SYS/brokers/+/clients/# and parsing client IDs/status (requires broker support and permissions)
    # 2. Subscribing to known Dingtian discovery topics (e.g., "dingtian/+/discovery")
    # 3. Subscribing to base topics and inferring devices (e.g., "dingtian/device_X/#")
    
    # For this starting point, we'll simulate a discovery based on typical paths
    print(" (Simulating device discovery. In a real scenario, this would involve subscribing to MQTT topics.)")
    print(" Please specify the MQTT topics or patterns Dingtian devices use for discovery.")
    
    simulated_dingtian_clients = {
        "Dingtian-Relay-12345": "Dingtian Relay Module (SN: 12345)",
        "Dingtian-Input-67890": "Dingtian Digital Input Module (SN: 67890)",
        "Another-Device-Not-Dingtian": "Some Other Device"
    }

    for client_id, description in simulated_dingtian_clients.items():
        if "dingtian" in client_id.lower():
            found_devices[client_id] = description

    print(f" (Simulated) Found {len(found_devices)} potential Dingtian clients.")
    return found_devices

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

    # Variables to hold existing MQTT settings
    existing_mqtt_broker = ''
    existing_mqtt_port = '1883'
    existing_mqtt_username = ''
    existing_mqtt_password = ''

    if file_exists:
        print(f"Existing config file found at {config_path}.")
        while True:
            print("\n--- Configuration Options ---")
            print("1) Continue to configuration (update existing)")
            print("2) Delete existing configuration and exit (WARNING: This cannot be undone!)")
            
            choice = input("Enter your choice (1 or 2): ")

            if choice == '1':
                config.read(config_path)
                # Capture original number of relay modules for conditional discovery
                original_num_relay_modules = config.getint('Global', 'numberofmodules', fallback=0)
                
                # Load existing MQTT settings if section exists
                if config.has_section('MQTT'):
                    existing_mqtt_broker = config.get('MQTT', 'brokeraddress', fallback='')
                    existing_mqtt_port = config.get('MQTT', 'port', fallback='1883')
                    existing_mqtt_username = config.get('MQTT', 'username', fallback='')
                    existing_mqtt_password = config.get('MQTT', 'password', fallback='')

                print("Continuing to update existing configuration.")
                break
            elif choice == '2':
                confirm = input("Are you absolutely sure you want to delete the configuration file? This cannot be undone! (yes/no): ")
                if confirm.lower() == 'yes':
                    os.remove(config_path)
                    print(f"Configuration file deleted: {config_path}")
                else:
                    print("Deletion cancelled.")
                print("Exiting script.")
                return
            else:
                print("Invalid choice. Please enter 1 or 2.")
    else:
        print(f"No existing config file found. A new one will be created at {config_path}.")

    # --- Global settings - ALL prompted first ---
    if not config.has_section('Global'):
        config.add_section('Global')
    
    current_loglevel = config.get('Global', 'loglevel', fallback='INFO')
    loglevel = input(f"Enter log level (options: DEBUG, INFO, WARNING, ERROR, CRITICAL; default: {current_loglevel}): ") or current_loglevel
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
                print("Invalid input. Please enter a non-negative integer for the number of relay modules.")
        except ValueError:
            print("Invalid input. Please enter a non-negative integer for the number of relay modules.")
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
                print("Invalid input. Please enter a non-negative integer for the number of temperature sensors.")
        except ValueError:
            print("Invalid input. Please enter a non-negative integer for the number of temperature sensors.")
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
                print("Invalid input. Please enter a non-negative integer for the number of tank sensors.")
        except ValueError:
            print("Invalid input. Please enter a non-negative integer for the number of tank sensors.")
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
                print("Invalid input. Please enter a non-negative integer for the number of virtual batteries.")
        except ValueError:
            print("Invalid input. Please enter a non-negative integer for the number of virtual batteries.")
    config.set('Global', 'numberofvirtualbatteries', str(num_virtual_batteries))

    # Initialize clients_to_configure
    clients_to_configure = []

    # --- MQTT Broker Info (Always Prompted with defaults) ---
    broker_address, port, username, password = get_mqtt_broker_info(
        current_broker_address=existing_mqtt_broker, 
        current_port=existing_mqtt_port, 
        current_username=existing_mqtt_username, 
        current_password=existing_mqtt_password
    )

    # --- Determine if Dingtian module discovery should be attempted ---
    should_attempt_discovery = False
    if not file_exists: # New configuration
        if new_num_relay_modules > 0:
            should_attempt_discovery = True
    else: # Editing existing configuration
        # Only attempt discovery if the number of modules has *increased*
        if new_num_relay_modules > original_num_relay_modules:
            should_attempt_discovery = True

    # --- Conditional Dingtian Discovery Block ---
    if should_attempt_discovery:
        # Only ask about discovery if there are modules to configure
        if new_num_relay_modules > 0: # Ensure we don't try to discover if user set modules to 0
            while True:
                discovery_choice = input("\nDo you want to try to discover Dingtian modules via MQTT, or proceed to manual configuration? (discover/manual): ").lower()
                if discovery_choice in ['discover', 'manual']:
                    break
                else:
                    print("Invalid choice. Please enter 'discover' or 'manual'.")

            if discovery_choice == 'discover':
                mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
                if username:
                    mqtt_client.username_pw_set(username, password)

                try:
                    print(f"Connecting to MQTT broker at {broker_address}:{port}...")
                    mqtt_client.connect(broker_address, port, 60)
                    print("Connected to MQTT broker.")
                    
                    mqtt_client.loop_start() 
                    found_dingtian_clients = discover_dingtian_devices_via_mqtt(mqtt_client)
                    mqtt_client.loop_stop()
                    mqtt_client.disconnect()
                    print("Disconnected from MQTT broker.")

                    if found_dingtian_clients:
                        print("\n--- Discovered Dingtian Clients ---")
                        client_options = list(found_dingtian_clients.keys())
                        for i, client_id in enumerate(client_options):
                            print(f"{i+1}) {found_dingtian_clients[client_id]} (Client ID: {client_id})")
                        
                        selected_indices = input("Enter the numbers of the clients you want to configure (e.g., 1,3,4 or 'all'): ")
                        
                        if selected_indices.lower() == 'all':
                            clients_to_configure = client_options
                        else:
                            try:
                                indices = [int(x.strip()) - 1 for x in selected_indices.split(',')]
                                for idx in indices:
                                    if 0 <= idx < len(client_options):
                                        clients_to_configure.append(client_options[idx])
                                    else:
                                        print(f"Warning: Invalid selection number {idx+1} ignored.")
                            except ValueError:
                                print("Invalid input for selection. No specific clients selected for auto-configuration.")

                        if clients_to_configure:
                            print(f"\nProceeding to configure selected clients: {', '.join(clients_to_configure)}")
                        else:
                            print("\nNo specific Dingtian clients selected for auto-configuration. Proceeding with manual configuration.")

                    else:
                        print("\nNo Dingtian clients found via MQTT discovery. You will need to manually configure device details.")

                except Exception as e:
                    print(f"\nCould not connect to MQTT broker or perform discovery: {e}")
                    print("Proceeding with manual configuration without MQTT discovery.")
            else:
                print("\nSkipping MQTT discovery. Proceeding with manual configuration.")
        else:
            print("\nNumber of relay modules is 0. Skipping Dingtian module discovery (no modules to configure).")
    else:
        # This message will be shown if should_attempt_discovery is False
        print("\nSkipping Dingtian module discovery (number of relay modules not increased from previous setting or is 0 for new config).")


    device_instance_counter = 100

    # Relay module settings
    for i in range(1, new_num_relay_modules + 1):
        relay_module_section = f'Relay_Module_{i}'
        switch_prefix = f'switch_{i}_'

        if not config.has_section(relay_module_section):
            config.add_section(relay_module_section)

        current_device_instance = config.getint(relay_module_section, 'deviceinstance', fallback=device_instance_counter)
        device_instance_input = input(f"Enter device instance for Relay Module {i} (current: {current_device_instance}): ")
        config.set(relay_module_section, 'deviceinstance', device_instance_input if device_instance_input else str(current_device_instance))
        device_instance_counter = int(config.get(relay_module_section, 'deviceinstance')) + 1

        current_custom_name = config.get(relay_module_section, 'customname', fallback=f'Relay Module {i}')
        custom_name = input(f"Enter custom name for Relay Module {i} (current: {current_custom_name}): ")
        config.set(relay_module_section, 'customname', custom_name if custom_name else current_custom_name)

        default_num_switches_initial = 2 if not file_exists else 0
        current_num_switches = config.getint(relay_module_section, 'numberofswitches', fallback=default_num_switches_initial)
        while True:
            try:
                num_switches_input = input(f"Enter the number of switches for Relay Module {i} (current: {current_num_switches if current_num_switches > 0 else 'not set'}): ")
                if num_switches_input:
                    num_switches = int(num_switches_input)
                    if num_switches <= 0:
                        raise ValueError
                    break
                elif current_num_switches > 0:
                    num_switches = current_num_switches
                    break
                elif not file_exists:
                    num_switches = default_num_switches_initial
                    break
                else:
                    print("Invalid input. Please enter a positive integer for the number of switches.")
            except ValueError:
                print("Invalid input. Please enter a positive integer for the number of switches.")
        config.set(relay_module_section, 'numberofswitches', str(num_switches))

        current_mqtt_on_state_payload = config.get(relay_module_section, 'mqtt_on_state_payload', fallback='ON')
        mqtt_on_state_payload = input(f"Enter MQTT ON state payload for Relay Module {i} (current: {current_mqtt_on_state_payload}): ")
        config.set(relay_module_section, 'mqtt_on_state_payload', mqtt_on_state_payload if mqtt_on_state_payload else current_mqtt_on_state_payload)

        current_mqtt_off_state_payload = config.get(relay_module_section, 'mqtt_off_state_payload', fallback='OFF')
        mqtt_off_state_payload = input(f"Enter MQTT OFF state payload for Relay Module {i} (current: {current_mqtt_off_state_payload}): ")
        config.set(relay_module_section, 'mqtt_off_state_payload', mqtt_off_state_payload if mqtt_off_state_payload else current_mqtt_off_state_payload)

        current_mqtt_on_command_payload = config.get(relay_module_section, 'mqtt_on_command_payload', fallback='ON')
        mqtt_on_command_payload = input(f"Enter MQTT ON command payload for Relay Module {i} (current: {current_mqtt_on_command_payload}): ")
        config.set(relay_module_section, 'mqtt_on_command_payload', mqtt_on_command_payload if mqtt_on_command_payload else current_mqtt_on_command_payload)

        current_mqtt_off_command_payload = config.get(relay_module_section, 'mqtt_off_command_payload', fallback='OFF')
        mqtt_off_command_payload = input(f"Enter MQTT OFF command payload for Relay Module {i} (current: {current_mqtt_off_command_payload}): ")
        config.set(relay_module_section, 'mqtt_off_command_payload', mqtt_off_command_payload if mqtt_off_command_payload else current_mqtt_off_command_payload)

        config.set(relay_module_section, 'deviceindex', str(i))

        current_serial = config.get(relay_module_section, 'serial', fallback='')
        if not current_serial:
            new_serial = generate_serial()
            print(f"No existing serial for Relay Module {i}. Generating new serial: {new_serial}")
            config.set(relay_module_section, 'serial', new_serial)
        else:
            print(f"Using existing serial for Relay Module {i}: {current_serial}")
            config.set(relay_module_section, 'serial', current_serial)


        for j in range(1, num_switches + 1):
            switch_section = f'{switch_prefix}{j}'
            if not config.has_section(switch_section):
                config.add_section(switch_section)

            current_switch_custom_name = config.get(switch_section, 'customname', fallback=f'switch {j}')
            switch_custom_name = input(f"Enter custom name for Relay Module {i}, switch {j} (current: {current_switch_custom_name}): ")
            config.set(switch_section, 'customname', switch_custom_name if switch_custom_name else current_switch_custom_name)

            current_switch_group = config.get(switch_section, 'group', fallback=f'Group{i}')
            switch_group = input(f"Enter group for Relay Module {i}, switch {j} (current: {current_switch_group}): ")
            config.set(switch_section, 'group', switch_group if switch_group else current_switch_group)

            current_mqtt_state_topic = config.get(switch_section, 'mqttstatetopic', fallback='path/to/mqtt/topic')
            mqtt_state_topic = input(f"Enter MQTT state topic for Relay Module {i}, switch {j} (current: {current_mqtt_state_topic}): ")
            config.set(switch_section, 'mqttstatetopic', mqtt_state_topic if mqtt_state_topic else current_mqtt_state_topic)

            current_mqtt_command_topic = config.get(switch_section, 'mqttcommandtopic', fallback='path/to/mqtt/topic')
            mqtt_command_topic = input(f"Enter MQTT command topic for Relay Module {i}, switch {j} (current: {current_mqtt_command_topic}): ")
            config.set(switch_section, 'mqttcommandtopic', mqtt_command_topic if mqtt_command_topic else current_mqtt_command_topic)


    # Temperature Sensor settings
    for i in range(1, num_temp_sensors + 1):
        temp_sensor_section = f'Temp_Sensor_{i}'
        if not config.has_section(temp_sensor_section):
            config.add_section(temp_sensor_section)

        current_device_instance = config.getint(temp_sensor_section, 'deviceinstance', fallback=device_instance_counter)
        device_instance_input = input(f"Enter device instance for Temperature Sensor {i} (current: {current_device_instance}): ")
        config.set(temp_sensor_section, 'deviceinstance', device_instance_input if device_instance_input else str(current_device_instance))
        device_instance_counter = int(config.get(temp_sensor_section, 'deviceinstance')) + 1

        current_custom_name = config.get(temp_sensor_section, 'customname', fallback=f'Temperature Sensor {i}')
        custom_name = input(f"Enter custom name for Temperature Sensor {i} (current: {current_custom_name}): ")
        config.set(temp_sensor_section, 'customname', custom_name if custom_name else current_custom_name)

        current_serial = config.get(temp_sensor_section, 'serial', fallback='')
        if not current_serial:
            new_serial = generate_serial()
            print(f"No existing serial for Temperature Sensor {i}. Generating new serial: {new_serial}")
            config.set(temp_sensor_section, 'serial', new_serial)
        else:
            print(f"Using existing serial for Temperature Sensor {i}: {current_serial}")
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
                    print(f"Invalid type. Please choose from: {', '.join(temp_sensor_types)}")
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

        current_custom_name = config.get(tank_sensor_section, 'customname', fallback=f'Tank Sensor {i}')
        custom_name = input(f"Enter custom name for Tank Sensor {i} (current: {current_custom_name}): ")
        config.set(tank_sensor_section, 'customname', custom_name if custom_name else current_custom_name)

        current_serial = config.get(tank_sensor_section, 'serial', fallback='')
        if not current_serial:
            new_serial = generate_serial()
            print(f"No existing serial for Tank Sensor {i}. Generating new serial: {new_serial}")
            config.set(tank_sensor_section, 'serial', new_serial)
        else:
            print(f"Using existing serial for Tank Sensor {i}: {current_serial}")
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
                    print("Invalid fluid type. Please choose from the available options.")
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

        current_custom_name = config.get(virtual_battery_section, 'customname', fallback=f'Virtual Battery {i}')
        custom_name = input(f"Enter custom name for Virtual Battery {i} (current: {current_custom_name}): ")
        config.set(virtual_battery_section, 'customname', custom_name if custom_name else current_custom_name)

        current_serial = config.get(virtual_battery_section, 'serial', fallback='')
        if not current_serial:
            new_serial = generate_serial()
            print(f"No existing serial for Virtual Battery {i}. Generating new serial: {new_serial}")
            config.set(virtual_battery_section, 'serial', new_serial)
        else:
            print(f"Using existing serial for Virtual Battery {i}: {current_serial}")
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
    print(f"\nconfig successfully created/updated at {config_path}")

    # Post-configuration menu
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
                print(f"Error installing service or rebooting: {e}")
            except FileNotFoundError:
                print("Error: '/data/venus-os_virtual-devices/setup' command not found. Please ensure the setup script exists.")
            break
        elif choice == '2':
            print("Rebooting system...")
            try:
                subprocess.run(['reboot'], check=True)
            except subprocess.CalledProcessError as e:
                print(f"Error rebooting system: {e}")
            except FileNotFoundError:
                print("Error: 'sudo' command not found. Please ensure sudo is in your PATH.")
            break
        elif choice == '3':
            print("Exiting script.")
            break
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

if __name__ == "__main__":
    create_or_edit_config()
