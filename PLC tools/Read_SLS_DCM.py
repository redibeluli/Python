from pycomm3 import LogixDriver
import csv
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Gather sorter IPs from environment variables
sorters = {
    'SLS15': os.getenv('SLS15_IP'),
    'SLS14': os.getenv('SLS14_IP'),
    'SLS16': os.getenv('SLS16_IP'),
    'SLS3': os.getenv('SLS3_IP'),
    'SLS10': os.getenv('SLS10_IP'),
    'SLS4': os.getenv('SLS4_IP'),
}

# Verify all IPs are present
for sorter, ip in sorters.items():
    if ip is None:
        raise ValueError(f"Missing IP address for sorter {sorter} in .env file.")

print('Starting DCM data collection...')

# Dictionary to store DCM statistics per sorter
DCM_Info = {sorter: [] for sorter in sorters.keys()}

# Iterate over each sorter and read DCM stats
for sorter, ip in sorters.items():
    print(f'Collecting data from {sorter} at {ip}...')
    try:
        with LogixDriver(ip, init_tags=True, init_program_tags=False) as plc:
            # DCM indexes start at 11, go to 243 stepping by 8
            for i in range(11, 244, 8):
                # Read the stStatistics tag for each DCM index
                _, stat, _, err = plc.read(f'astDCM[{i}].stStatistics')
                if err:
                    print(f"Error reading astDCM[{i}] on {sorter}: {err}")
                else:
                    DCM_Info[sorter].append(stat)
    except Exception as e:
        print(f"Failed to connect to {sorter} ({ip}): {e}")

# Write collected data to CSV
output_file = 'DCM_Info.csv'
with open(output_file, 'w', encoding='UTF8', newline='') as csvfile:
    writer = csv.writer(csvfile)

    # Write headers for each sorter section
    for sorter, stats_list in DCM_Info.items():
        writer.writerow([
            sorter, 'DCM #', 'Disabled', 'Pin_Flt', 'Prox_Flt', 'Connection', 'HeartBeatTx', 'HeartbeatRx',
            'VaneTravelTime', 'VaneChargingTime', 'IncorrectSpeed', 'DidNotSwitch', "SolenoidFailure",
            "GeneralFault", "PotentialMissingPin"
        ])

        DCM_num = 11
        for stat in stats_list:
            writer.writerow([
                '', DCM_num,
                stat.get('diDisabled', ''),
                stat.get('diPin', ''),
                stat.get('diPX', ''),
                stat.get('diConnection', ''),
                stat.get('diHeartbeatTx', ''),
                stat.get('diHeartbeatRx', ''),
                stat.get('diVaneTravelTime', ''),
                stat.get('diVaneChargingTime', ''),
                stat.get('diIncorrectSpeed', ''),
                stat.get('diVaneDidNotSwitch', ''),
                stat.get('diSolenoidFailure', ''),
                stat.get('diGeneralFault', ''),
                stat.get('diPotentialMissingPin', '')
            ])
            DCM_num += 8

print(f"Data collection complete. CSV saved as {output_file}.")

# Optionally copy CSV to network share from env variable
network_share = os.getenv('NETWORK_SHARE')
if network_share:
    try:
        import shutil
        shutil.copy2(output_file, os.path.join(network_share, os.path.basename(output_file)))
        print(f"CSV file copied to network share: {network_share}")
    except Exception as e:
        print(f"Failed to copy CSV to network share: {e}")
else:
    print("No NETWORK_SHARE path defined in environment variables; skipping copy.")
