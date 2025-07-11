import os
import csv
import math
import time
import json
import shutil
import pandas as pd
from collections import defaultdict
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor
from pycomm3 import LogixDriver
from dotenv import load_dotenv

# Class to manage the collection, formatting, and storage of diverter statistics
class DiverterStatsLogger:
        # Initialize the logger by loading environment variables and setting up data structures
    def __init__(self):
        load_dotenv()
        self.sorters = json.loads(os.getenv("SORTERS_IPS", "{}"))
        self.sorters2 = json.loads(os.getenv("SORTERS2_IPS", "{}"))
        self.network_share = os.getenv("NETWORK_SHARE", "")
        self.diverter_info = defaultdict(list)
        self.metrics = ['Cycle Time', 'Latency', 'Fired', 'Confirmed', 'DC_Percent']
        self._initialize_diverter_info()

        # Pre-fill the diverter_info dict with keys from both sorter dictionaries
    def _initialize_diverter_info(self):
        for module in list(self.sorters) + list(self.sorters2):
            self.diverter_info[module]  # initializes empty list

        # Connect to a single PLC and collect 21 rows of diverter data metrics
    def get_diverter_info(self, ip, sorter):
        print(f'Collecting data for {sorter}...')
        try:
            with LogixDriver(ip, init_tags=True, init_program_tags=False) as plc:
                for x in range(1, 22):
                    _, cycle_time, _, _ = plc.read(f'Diverter_diags[{x}].Cycle_Time_Ave')
                    _, latency, _, _ = plc.read(f'Diverter_diags[{x}].Latency_Ave')
                    _, fired, _, _ = plc.read(f'Diverter_diags[{x}].Fired')
                    _, confirmed, _, _ = plc.read(f'Diverter_diags[{x}].Confirmed_Total')

                    truncated_cycle = math.trunc(cycle_time)
                    truncated_latency = math.trunc(latency)
                    dc_percent = (confirmed / fired) * 100 if fired > 0 else 0

                    self.diverter_info[sorter].extend([
                        truncated_cycle,
                        truncated_latency,
                        fired,
                        confirmed,
                        dc_percent
                    ])
        except Exception as e:
            print(f"Error collecting data from {sorter} at {ip}: {e}")

        # Launch parallel data collection for all sorters
    def collect_all_data(self):
        print("Starting data collection...")
        with ThreadPoolExecutor(max_workers=20) as executor:
            for module, ip in {**self.sorters, **self.sorters2}.items():
                executor.submit(self.get_diverter_info, ip, module)
        print("Data collection complete.")

        # Transform the diverter_info dictionary into a 2D list format suitable for CSV output
    def format_for_csv(self):
        diverters = [[f'Div{x+1}'] for x in range(21)]
        for x in range(21):
            for module in self.diverter_info:
                base_index = x * 5
                diverters[x].extend(self.diverter_info[module][base_index:base_index + 5])
        return diverters

        # Write the formatted diverter data and headers to a CSV file
    def save_csv(self, diverters, filename):
        mods = [''] + [item for key in self.diverter_info for item in [key, '', '', '', '']]
        metrics = [''] + self.metrics * len(self.diverter_info)

        with open(filename, 'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(mods)
            writer.writerow(metrics)
            writer.writerows(diverters)

        # Add the new diverter values to an existing running totals file
    def sum_totals(self, diverters):
        df = pd.read_csv('Diverter_Stat_Totals.csv')
        mod_index = 3
        for mod in range(1, 109, 3):
            for div in range(1, 22):
                df.iloc[1:, mod] = pd.to_numeric(df.iloc[1:, mod], errors='coerce')
                df.iloc[1:, mod + 1] = pd.to_numeric(df.iloc[1:, mod + 1], errors='coerce')
                df.iloc[div, mod] += diverters[div - 1][mod_index]
                df.iloc[div, mod + 1] += diverters[div - 1][mod_index + 1]
            mod_index += 5
        df.iloc[23, 1] += 1
        df.to_csv('Diverter_Stat_Totals.csv', index=False)

        # Copy the updated totals file to a predefined network share location
    def copy_to_network(self, filename):
        if self.network_share:
            try:
                shutil.copy2(filename, os.path.join(self.network_share, os.path.basename(filename)))
                print("File copied to network share successfully.")
            except Exception as e:
                print(f"Failed to copy file to network location: {e}")
        else:
            print("Network share path not defined.")

# Prompt the user with a yes/no question and return a boolean result
def yes_no_prompt(question):
    while True:
        answer = input(f"{question} (yes/no): ").lower()
        if answer in ("yes", "y"):
            return True
        if answer in ("no", "n"):
            return False
        print("Invalid input. Please enter yes or no.")

# Main entry point to execute the diverter data collection process
def main():
    logger = DiverterStatsLogger()

    sum_data = yes_no_prompt("Do you want to sum the totals?")
    logger.collect_all_data()

    diverters = logger.format_for_csv()
    filename = f"Diverter_Stats_{date.today().strftime('%Y-%m-%d')}.csv"
    logger.save_csv(diverters, filename)

    if sum_data:
        logger.sum_totals(diverters)

    logger.copy_to_network('Diverter_Stat_Totals.csv')

if __name__ == "__main__":
    main()
