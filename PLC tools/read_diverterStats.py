from pycomm3 import LogixDriver
from collections import defaultdict
import csv
import math
import time
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import shutil
import os
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read IP address dictionaries and network share path from environment
sorters = json.loads(os.getenv("SORTERS_IPS", "{}"))
sorters2 = json.loads(os.getenv("SORTERS2_IPS", "{}"))
network_share = os.getenv("NETWORK_SHARE", "")

# Data structure for holding diverter info
Diverter_Info = defaultdict(list)

for module in list(sorters) + list(sorters2):
    Diverter_Info[module]  # Initializes keys with empty lists

# Generate header row for CSV output
mods = [''] + [item for key in Diverter_Info for item in [key, '', '', '', '']]
metrics = ['']

print('Starting diverter data collection...')

def get_diverter_info(ip, sorter):
    """Reads metrics from a PLC diverter and appends to the global Diverter_Info."""
    print(f'Collecting data for {sorter}...')
    try:
        with LogixDriver(ip, init_tags=True, init_program_tags=False) as plc:
            metrics.extend(['Cycle Time', 'Latency', 'Fired', 'Confirmed', 'DC_Percent'])
            for x in range(1, 22):
                _, cycle_time, _, _ = plc.read(f'Diverter_diags[{x}].Cycle_Time_Ave')
                _, latency, _, _ = plc.read(f'Diverter_diags[{x}].Latency_Ave')
                _, fired, _, _ = plc.read(f'Diverter_diags[{x}].Fired')
                _, confirmed, _, _ = plc.read(f'Diverter_diags[{x}].Confirmed_Total')

                # Calculate values and append to diverter info
                truncated_cycle = math.trunc(cycle_time)
                truncated_latency = math.trunc(latency)
                DC_percent = (confirmed / fired) * 100 if fired > 0 else 0

                Diverter_Info[sorter].extend([
                    truncated_cycle,
                    truncated_latency,
                    fired,
                    confirmed,
                    DC_percent
                ])
    except Exception as e:
        print(f"Error collecting data from {sorter} at {ip}: {e}")

def multi_get_div_info(sorter_dict, workers=20):
    """Collects data in parallel from all sorters."""
    start_time = datetime.now()
    print("Fetching PLC data...")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for module, ip_address in sorter_dict.items():
            executor.submit(get_diverter_info, ip=ip_address, sorter=module)
    print(f'Time taken: {datetime.now() - start_time}')

def yes_no_prompt(question):
    while True:
        answer = input(f"{question} (yes/no): ").lower()
        if answer in ["yes", "y"]:
            return True
        elif answer in ["no", "n"]:
            return False
        print("Invalid input. Please enter yes or no.")

sum_data = yes_no_prompt("Do you want to sum the totals?")
print("Data will{} be added to totals...".format("" if sum_data else " not"))

multi_get_div_info(sorters)
multi_get_div_info(sorters2)

# Format diverter data for CSV

diverters = [[f'Div{x+1}'] for x in range(21)]
for x in range(21):
    for module in Diverter_Info:
        base_index = x * 5
        diverters[x].extend(Diverter_Info[module][base_index:base_index + 5])

# Save to timestamped CSV
filename = f"Diverter_Stats_{date.today().strftime('%Y-%m-%d')}.csv"
with open(filename, 'w', encoding='UTF8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(mods)
    writer.writerow(metrics)
    writer.writerows(diverters)

# Optionally sum data into running totals file
if sum_data:
    df = pd.read_csv('Diverter_Stat_Totals.csv')
    mod_index = 3
    for mod in range(1, 109, 3):
        for div in range(1, 22):
            df.iloc[1:, mod] = pd.to_numeric(df.iloc[1:, mod], errors='coerce')
            df.iloc[1:, mod + 1] = pd.to_numeric(df.iloc[1:, mod + 1], errors='coerce')
            df.iloc[div, mod] += diverters[div - 1][mod_index]
            df.iloc[div, mod + 1] += diverters[div - 1][mod_index + 1]
        mod_index += 5
    df.iloc[23, 1] += 1  # Increment run counter
    df.to_csv('Diverter_Stat_Totals.csv', index=False)

# Copy to shared network directory
if network_share:
    try:
        shutil.copy2('Diverter_Stat_Totals.csv', os.path.join(network_share, 'Diverter_Stat_Totals.csv'))
    except Exception as e:
        print(f"Failed to copy file to network location: {e}")
else:
    print("Network share path not defined in environment variables.")
