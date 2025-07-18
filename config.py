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
    The file will be located in /data/setupOptions/MQTT-switches and named optionsSet.
    """
    config_dir = '/data/setupOptions/MQTT-switches'
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

    # Prompt for number of devices
    # Default to 1 if file doesn't exist, otherwise use existing or 0
    default_num_devices_initial = 1 if not file_exists else 0
    current_num_devices = config.getint('Global', 'numberofdevices', fallback=default_num_devices_initial)
    while True:
        try:
            num_devices_input = input(f"Enter the number of devices (current: {current_num_devices if current_num_devices > 0 else 'not set'}): ")
            if num_devices_input:
                num_devices = int(num_devices_input)
                if num_devices <= 0:
                    raise ValueError
                break
            elif current_num_devices > 0:
                num_devices = current_num_devices
                break
            elif not file_exists: # If new file and no input, use initial default
                num_devices = default_num_devices_initial
                break
            else:
                print("Invalid input. Please enter a positive integer for the number of devices.")
        except ValueError:
            print("Invalid input. Please enter a positive integer for the number of devices.")
    config.set('Global', 'numberofdevices', str(num_devices))

    # Device settings
    for i in range(1, num_devices + 1):
        device_section = f'Device_{i}'
        output_prefix = f'Output_{i}_'

        if not config.has_section(device_section):
            config.add_section(device_section)

        # Device instance
        current_device_instance = config.getint(device_section, 'deviceinstance', fallback=100 + (i - 1))
        device_instance_input = input(f"Enter device instance for Device {i} (current: {current_device_instance}): ")
        config.set(device_section, 'deviceinstance', device_instance_input if device_instance_input else str(current_device_instance))

        # Custom name
        current_custom_name = config.get(device_section, 'customname', fallback=f'Relay Module {i}')
        custom_name = input(f"Enter custom name for Device {i} (current: {current_custom_name}): ")
        config.set(device_section, 'customname', custom_name if custom_name else current_custom_name)

        # Number of switches
        # Default to 2 if file doesn't exist, otherwise use existing or 0
        default_num_switches_initial = 2 if not file_exists else 0
        current_num_switches = config.getint(device_section, 'numberofswitches', fallback=default_num_switches_initial)
        while True:
            try:
                num_switches_input = input(f"Enter the number of switches for Device {i} (current: {current_num_switches if current_num_switches > 0 else 'not set'}): ")
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
        config.set(device_section, 'numberofswitches', str(num_switches))

        # MQTT ON/OFF payload
        current_mqtt_on = config.get(device_section, 'mqttonpayload', fallback='ON')
        mqtt_on = input(f"Enter MQTT ON payload for Device {i} (current: {current_mqtt_on}): ")
        config.set(device_section, 'mqttonpayload', mqtt_on if mqtt_on else current_mqtt_on)

        current_mqtt_off = config.get(device_section, 'mqttoffpayload', fallback='OFF')
        mqtt_off = input(f"Enter MQTT OFF payload for Device {i} (current: {current_mqtt_off}): ")
        config.set(device_section, 'mqttoffpayload', mqtt_off if mqtt_off else current_mqtt_off)

        # Device index - No longer prompting, just setting it based on loop variable 'i'
        config.set(device_section, 'deviceindex', str(i))

        # Serial number - generate if not present
        current_serial = config.get(device_section, 'serial', fallback='')
        if not current_serial:
            new_serial = generate_serial()
            print(f"No existing serial for Device {i}. Generating new serial: {new_serial}")
            config.set(device_section, 'serial', new_serial)
        else:
            print(f"Using existing serial for Device {i}: {current_serial}")
            config.set(device_section, 'serial', current_serial) # Ensure it's explicitly set even if not changed


        # Output settings for each switch
        for j in range(1, num_switches + 1):
            output_section = f'{output_prefix}{j}'
            if not config.has_section(output_section):
                config.add_section(output_section)

            # Custom name for output
            current_output_custom_name = config.get(output_section, 'customname', fallback=f'Output {j}')
            output_custom_name = input(f"Enter custom name for Device {i}, Output {j} (current: {current_output_custom_name}): ")
            config.set(output_section, 'customname', output_custom_name if output_custom_name else current_output_custom_name)

            # Group for output - changed to use device number
            current_output_group = config.get(output_section, 'group', fallback=f'Group{i}')
            output_group = input(f"Enter group for Device {i}, Output {j} (current: {current_output_group}): ")
            config.set(output_section, 'group', output_group if output_group else current_output_group)

            # MQTT state topic
            current_mqtt_state_topic = config.get(output_section, 'mqttstatetopic', fallback='path/to/mqtt/topic')
            mqtt_state_topic = input(f"Enter MQTT state topic for Device {i}, Output {j} (current: {current_mqtt_state_topic}): ")
            config.set(output_section, 'mqttstatetopic', mqtt_state_topic if mqtt_state_topic else current_mqtt_state_topic)

            # MQTT command topic
            current_mqtt_command_topic = config.get(output_section, 'mqttcommandtopic', fallback='path/to/mqtt/topic')
            mqtt_command_topic = input(f"Enter MQTT command topic for Device {i}, Output {j} (current: {current_mqtt_command_topic}): ")
            config.set(output_section, 'mqttcommandtopic', mqtt_command_topic if mqtt_command_topic else current_mqtt_command_topic)

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
            print("Running: /data/MQTT-switches/setup install")
            try:
                subprocess.run(['/data/MQTT-switches/setup', 'install'], check=True)
                print("Service installed and activated successfully. Rebooting system...")
                subprocess.run(['reboot'], check=True)
            except subprocess.CalledProcessError as e:
                print(f"Error installing service or rebooting: {e}")
            except FileNotFoundError:
                print("Error: '/data/MQTT-switches/setup' command not found. Please ensure the setup script exists.")
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
