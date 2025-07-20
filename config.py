#!/usr/bin/env python3
import configparser
import os
import random
import subprocess # Import the subprocess module

def generate_serial():
    """Generates a random 16-digit serial number."""
    return ''.join([str(random.randint(0, 9)) for _ in range(16)])

def create_or_edit_config():
    """
    Creates or edits a config file based on user input.
    The file will be located in /data/setupOptions/venus-os_virtual-devices and named optionsSet.
    """
    config_dir = '/data/setupOptions/venus-os_virtual-devices'
    config_path = os.path.join(config_dir, 'optionsSet') # Updated config file name

    # Ensure the directory exists
    os.makedirs(config_dir, exist_ok=True)

    config = configparser.ConfigParser()
    file_exists = os.path.exists(config_path)

    if file_exists:
        config.read(config_path)
        print(f"Existing config file found at {config_path}. It will be updated.")
    else:
        print(f"No existing config file found. A new one will be created at {config_path}.")

    # Global settings
    if not config.has_section('Global'):
        config.add_section('Global')
    
    # Prompt for loglevel, default to INFO if not present or empty
    current_loglevel = config.get('Global', 'loglevel', fallback='INFO')
    loglevel = input(f"Enter log level (options: DEBUG, INFO, WARNING, ERROR, CRITICAL; default: {current_loglevel}): ") or current_loglevel
    config.set('Global', 'loglevel', loglevel)

    # Prompt for number of relay modules
    # Default to 1 if file doesn't exist, otherwise use existing or 0
    default_num_relay_modules_initial = 1 if not file_exists else 0
    current_num_relay_modules = config.getint('Global', 'numberofmodules', fallback=default_num_relay_modules_initial)
    while True:
        try:
            num_relay_modules_input = input(f"Enter the number of relay modules (current: {current_num_relay_modules if current_num_relay_modules > 0 else 'not set'}): ")
            if num_relay_modules_input:
                num_relay_modules = int(num_relay_modules_input)
                if num_relay_modules <= 0:
                    raise ValueError
                break
            elif current_num_relay_modules > 0:
                num_relay_modules = current_num_relay_modules
                break
            elif not file_exists: # If new file and no input, use initial default
                num_relay_modules = default_num_relay_modules_initial
                break
            else:
                print("Invalid input. Please enter a positive integer for the number of relay modules.")
        except ValueError:
            print("Invalid input. Please enter a positive integer for the number of relay modules.")
    config.set('Global', 'numberofmodules', str(num_relay_modules))

    # Prompt for number of temperature sensors (NEW)
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

    # Prompt for number of tank sensors (NEW)
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

    # Relay module settings
    for i in range(1, num_relay_modules + 1):
        relay_module_section = f'Relay_Module_{i}'
        switch_prefix = f'switch_{i}_'

        if not config.has_section(relay_module_section):
            config.add_section(relay_module_section)

        # Device instance
        current_device_instance = config.getint(relay_module_section, 'deviceinstance', fallback=100 + (i - 1))
        device_instance_input = input(f"Enter device instance for Relay Module {i} (current: {current_device_instance}): ")
        config.set(relay_module_section, 'deviceinstance', device_instance_input if device_instance_input else str(current_device_instance))

        # Custom name
        current_custom_name = config.get(relay_module_section, 'customname', fallback=f'Relay Module {i}')
        custom_name = input(f"Enter custom name for Relay Module {i} (current: {current_custom_name}): ")
        config.set(relay_module_section, 'customname', custom_name if custom_name else current_custom_name)

        # Number of switches
        # Default to 2 if file doesn't exist, otherwise use existing or 0
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
                elif not file_exists: # If new file and no input, use initial default
                    num_switches = default_num_switches_initial
                    break
                else:
                    print("Invalid input. Please enter a positive integer for the number of switches.")
            except ValueError:
                print("Invalid input. Please enter a positive integer for the number of switches.")
        config.set(relay_module_section, 'numberofswitches', str(num_switches))

        # MQTT ON/OFF payload
        current_mqtt_on = config.get(relay_module_section, 'mqttonpayload', fallback='ON')
        mqtt_on = input(f"Enter MQTT ON payload for Relay Module {i} (current: {current_mqtt_on}): ")
        config.set(relay_module_section, 'mqttonpayload', mqtt_on if mqtt_on else current_mqtt_on)

        current_mqtt_off = config.get(relay_module_section, 'mqttoffpayload', fallback='OFF')
        mqtt_off = input(f"Enter MQTT OFF payload for Relay Module {i} (current: {current_mqtt_off}): ")
        config.set(relay_module_section, 'mqttoffpayload', mqtt_off if mqtt_off else current_mqtt_off)

        # Relay Module index - No longer prompting, just setting it based on loop variable 'i'
        config.set(relay_module_section, 'deviceindex', str(i))

        # Serial number - generate if not present
        current_serial = config.get(relay_module_section, 'serial', fallback='')
        if not current_serial:
            new_serial = generate_serial()
            print(f"No existing serial for Relay Module {i}. Generating new serial: {new_serial}")
            config.set(relay_module_section, 'serial', new_serial)
        else:
            print(f"Using existing serial for Relay Module {i}: {current_serial}")
            config.set(relay_module_section, 'serial', current_serial) # Ensure it's explicitly set even if not changed


        # switch settings for each switch
        for j in range(1, num_switches + 1):
            switch_section = f'{switch_prefix}{j}'
            if not config.has_section(switch_section):
                config.add_section(switch_section)

            # Custom name for switch
            current_switch_custom_name = config.get(switch_section, 'customname', fallback=f'switch {j}')
            switch_custom_name = input(f"Enter custom name for Relay Module {i}, switch {j} (current: {current_switch_custom_name}): ")
            config.set(switch_section, 'customname', switch_custom_name if switch_custom_name else current_switch_custom_name)

            # Group for switch - changed to use Relay Module number
            current_switch_group = config.get(switch_section, 'group', fallback=f'Group{i}')
            switch_group = input(f"Enter group for Relay Module {i}, switch {j} (current: {current_switch_group}): ")
            config.set(switch_section, 'group', switch_group if switch_group else current_switch_group)

            # MQTT state topic
            current_mqtt_state_topic = config.get(switch_section, 'mqttstatetopic', fallback='path/to/mqtt/topic')
            mqtt_state_topic = input(f"Enter MQTT state topic for Relay Module {i}, switch {j} (current: {current_mqtt_state_topic}): ")
            config.set(switch_section, 'mqttstatetopic', mqtt_state_topic if mqtt_state_topic else current_mqtt_state_topic)

            # MQTT command topic
            current_mqtt_command_topic = config.get(switch_section, 'mqttcommandtopic', fallback='path/to/mqtt/topic')
            mqtt_command_topic = input(f"Enter MQTT command topic for Relay Module {i}, switch {j} (current: {current_mqtt_command_topic}): ")
            config.set(switch_section, 'mqttcommandtopic', mqtt_command_topic if mqtt_command_topic else current_mqtt_command_topic)

    # Temperature Sensor settings (NEW SECTION)
    for i in range(1, num_temp_sensors + 1):
        temp_sensor_section = f'Temp_Sensor_{i}'
        if not config.has_section(temp_sensor_section):
            config.add_section(temp_sensor_section)

        # Device instance
        current_device_instance = config.getint(temp_sensor_section, 'deviceinstance', fallback=200 + (i - 1))
        device_instance_input = input(f"Enter device instance for Temperature Sensor {i} (current: {current_device_instance}): ")
        config.set(temp_sensor_section, 'deviceinstance', device_instance_input if device_instance_input else str(current_device_instance))

        # Custom name
        current_custom_name = config.get(temp_sensor_section, 'customname', fallback=f'Temperature Sensor {i}')
        custom_name = input(f"Enter custom name for Temperature Sensor {i} (current: {current_custom_name}): ")
        config.set(temp_sensor_section, 'customname', custom_name if custom_name else current_custom_name)

        # Serial number - generate if not present
        current_serial = config.get(temp_sensor_section, 'serial', fallback='')
        if not current_serial:
            new_serial = generate_serial()
            print(f"No existing serial for Temperature Sensor {i}. Generating new serial: {new_serial}")
            config.set(temp_sensor_section, 'serial', new_serial)
        else:
            print(f"Using existing serial for Temperature Sensor {i}: {current_serial}")
            config.set(temp_sensor_section, 'serial', current_serial)

        # Temperature state topic
        current_temp_state_topic = config.get(temp_sensor_section, 'temperaturestatetopic', fallback='path/to/mqtt/temperature')
        temp_state_topic = input(f"Enter MQTT temperature state topic for Temperature Sensor {i} (current: {current_temp_state_topic}): ")
        config.set(temp_sensor_section, 'temperaturestatetopic', temp_state_topic if temp_state_topic else current_temp_state_topic)

        # Humidity state topic
        current_humidity_state_topic = config.get(temp_sensor_section, 'humiditystatetopic', fallback='path/to/mqtt/humidity')
        humidity_state_topic = input(f"Enter MQTT humidity state topic for Temperature Sensor {i} (current: {current_humidity_state_topic}): ")
        config.set(temp_sensor_section, 'humiditystatetopic', humidity_state_topic if humidity_state_topic else current_humidity_state_topic)

        # Battery state topic
        current_battery_state_topic = config.get(temp_sensor_section, 'batterystatetopic', fallback='path/to/mqtt/battery')
        battery_state_topic = input(f"Enter MQTT battery state topic for Temperature Sensor {i} (current: {current_battery_state_topic}): ")
        config.set(temp_sensor_section, 'batterystatetopic', battery_state_topic if battery_state_topic else current_battery_state_topic)

    # Tank Sensor settings (NEW SECTION)
    for i in range(1, num_tank_sensors + 1):
        tank_sensor_section = f'Tank_Sensor_{i}'
        if not config.has_section(tank_sensor_section):
            config.add_section(tank_sensor_section)

        # Device instance
        current_device_instance = config.getint(tank_sensor_section, 'deviceinstance', fallback=300 + (i - 1))
        device_instance_input = input(f"Enter device instance for Tank Sensor {i} (current: {current_device_instance}): ")
        config.set(tank_sensor_section, 'deviceinstance', device_instance_input if device_instance_input else str(current_device_instance))

        # Custom name
        current_custom_name = config.get(tank_sensor_section, 'customname', fallback=f'Tank Sensor {i}')
        custom_name = input(f"Enter custom name for Tank Sensor {i} (current: {current_custom_name}): ")
        config.set(tank_sensor_section, 'customname', custom_name if custom_name else current_custom_name)

        # Serial number - generate if not present
        current_serial = config.get(tank_sensor_section, 'serial', fallback='')
        if not current_serial:
            new_serial = generate_serial()
            print(f"No existing serial for Tank Sensor {i}. Generating new serial: {new_serial}")
            config.set(tank_sensor_section, 'serial', new_serial)
        else:
            print(f"Using existing serial for Tank Sensor {i}: {current_serial}")
            config.set(tank_sensor_section, 'serial', current_serial)

        # Level state topic
        current_level_state_topic = config.get(tank_sensor_section, 'levelstatetopic', fallback='path/to/mqtt/level')
        level_state_topic = input(f"Enter MQTT level state topic for Tank Sensor {i} (current: {current_level_state_topic}): ")
        config.set(tank_sensor_section, 'levelstatetopic', level_state_topic if level_state_topic else current_level_state_topic)

        # Battery state topic
        current_battery_state_topic = config.get(tank_sensor_section, 'batterystatetopic', fallback='path/to/mqtt/battery')
        battery_state_topic = input(f"Enter MQTT battery state topic for Tank Sensor {i} (current: {current_battery_state_topic}): ")
        config.set(tank_sensor_section, 'batterystatetopic', battery_state_topic if battery_state_topic else current_battery_state_topic)

        # Temperature state topic
        current_temp_state_topic = config.get(tank_sensor_section, 'temperaturestatetopic', fallback='path/to/mqtt/temperature')
        temp_state_topic = input(f"Enter MQTT temperature state topic for Tank Sensor {i} (current: {current_temp_state_topic}): ")
        config.set(tank_sensor_section, 'temperaturestatetopic', temp_state_topic if temp_state_topic else current_temp_state_topic)

    # MQTT broker settings
    if not config.has_section('MQTT'):
        config.add_section('MQTT')
    
    current_broker_address = config.get('MQTT', 'brokeraddress', fallback='localhost')
    broker_address = input(f"Enter MQTT broker address (current: {current_broker_address}): ")
    config.set('MQTT', 'brokeraddress', broker_address if broker_address else current_broker_address)

    current_port = config.get('MQTT', 'port', fallback='1883')
    port = input(f"Enter MQTT port (current: {current_port}): ")
    config.set('MQTT', 'port', port if port else current_port)

    current_username = config.get('MQTT', 'username', fallback='your_username')
    username = input(f"Enter MQTT username (current: {current_username}): ")
    config.set('MQTT', 'username', username if username else current_username)

    current_password = config.get('MQTT', 'password', fallback='your_password')
    password = input(f"Enter MQTT password (current: {current_password}): ")
    config.set('MQTT', 'password', password if password else current_password)

    # Write the configuration to the file
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
                subprocess.run(['reboot'], check=True) # Changed to direct reboot
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
