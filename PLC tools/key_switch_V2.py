from pycomm3 import LogixDriver
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dotenv import load_dotenv
import shutil
import os
import plc

# Load environment variables from .env file
load_dotenv()

def get_plc_info(module, ip, firmware_list, processor_list, naughty_list, total, not_run):
    """
    Connects to a PLC and collects keyswitch, firmware, and processor information.
    Adds module to naughty_list if it's not in RUN mode.
    """
    try:
        with LogixDriver(ip, init_tags=False, init_program_tags=False) as plc:
            total[0] += 1
            mode = plc.info['keyswitch']
            if mode != 'RUN':
                naughty_list.append(module)
                not_run[0] += 1

            revision = plc.info['revision']
            firmware = f"{revision['major']}.{revision['minor']}"
            firmware_list.append(firmware)

            processor = plc.info['product_name'].split()[0]
            processor_list.append(processor)

            print(f"{module} -- v{firmware} {processor} KeySwitch: {mode}")

    except Exception as e:
        print(f"Could not connect to {module} ({ip}): {e}")


def collect_all_plc_data(modules, max_workers=58):
    """
    Launches multithreaded data collection for all PLC modules.
    Returns firmware list, processor list, naughty list, and counts.
    """
    firmware_list = []
    processor_list = []
    naughty_list = []
    total = [0]
    not_run = [0]

    start_time = datetime.now()
    print("Fetching PLC info...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for module, ip in modules.items():
            executor.submit(get_plc_info, module, ip, firmware_list, processor_list, naughty_list, total, not_run)

    print(f"Time taken: {datetime.now() - start_time}")
    return firmware_list, processor_list, naughty_list, not_run[0], total[0]


def write_summary_report(filename, naughty_list, not_run, total, processor_count, firmware_count):
    """
    Writes the results to a summary text file.
    """
    with open(filename, "w") as file:
        file.write(f"PLCs not in Run: {not_run}/{total}\n")
        for module in naughty_list:
            file.write(f'"{module}", ')
        file.write('\n\nProcessor breakdown:\n')
        for processor, cnt in processor_count.items():
            file.write(f"{processor}: ({cnt}/{total}) {round(cnt / total * 100, 2)}%\n")
        file.write('\nFirmware breakdown:\n')
        for firmware, cnt in firmware_count.items():
            file.write(f"{firmware}: ({cnt}/{total}) {round(cnt / total * 100, 2)}%\n")


def main():
    firmware_list, processor_list, naughty_list, not_run, total = collect_all_plc_data(plc.modules)

    # Aggregate results
    processor_count = Counter(processor_list)
    firmware_count = Counter(firmware_list)

    # Print summary to console
    print('\nPLCs not in Run:', f"{not_run}/{total}")
    print(naughty_list)
    print('\nProcessor breakdown:')
    for p, c in processor_count.items():
        print(f"{p}: ({c}/{total}) {round(c / total * 100, 2)}%")
    print('\nFirmware breakdown:')
    for f, c in firmware_count.items():
        print(f"{f}: ({c}/{total}) {round(c / total * 100, 2)}%")

    # Save to file
    report_path = "PLC_Keyswitch.txt"
    write_summary_report(report_path, naughty_list, not_run, total, processor_count, firmware_count)

    # Move to network share from .env variable
    network_path = os.getenv("NETWORK_SHARE")
    try:
        shutil.move(report_path, os.path.join(network_path, os.path.basename(report_path)))
        print(f"Report moved to network share: {network_path}")
    except Exception as e:
        print(f"Failed to move report to network share: {e}")


if __name__ == "__main__":
    main()
