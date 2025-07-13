from pycomm3 import CIPDriver, Services, ClassCode, UINT, UDINT, INT, REAL
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import csv
import traceback
import re

# Constants for parameter names and mappings
PARAM_LIST_525 = {
    29: 'Firmware',
    1: 'Output Freq',
    2: 'CMD Freq',
    3: 'Output Current',
    4: 'Output Voltage',
    5: 'DC Bus Voltage',
    41: 'Accel',
    42: 'Deccel',
    46: 'Start Source1',
    47: 'Speed Ref'
}

PARAM_LIST_755 = {
    29: 'Firmware',
    1: 'Output Freq',
    2: 'CMD Freq',
    7: 'Output Current',
    8: 'Output Voltage',
    11: 'DC Bus Voltage',
    535: 'Accel',
    537: 'Deccel',
    3: 'Mtr Vel Fdbk',
    4: 'Commanded Trq'
}

START_SOURCE_MAP = {
    1: 'Keypad',
    2: 'DigIn TrmBlk',
    4: 'Network Opt',
    5: 'Ethernet/IP'
}

SPEED_REF_MAP = {
    1: 'Drive Pot',
    2: 'Keypad',
    3: 'SerialDSI',
    4: 'Network Opt',
    5: '0-10V Input',
    6: '4-20mA Input',
    7: 'Preset Freq',
    15: 'Ethernet/IP'
}


def read_pf525_parameter(ip, module):
    """
    Connect to VFD and read parameters based on product code.
    Returns tuple: (module, local_info list)
    """
    local_info = [ip]
    try:
        with CIPDriver(ip, socket_timeout=3) as drive:
            # Read product code
            _, product_code, _, _ = drive.generic_message(
                service=Services.get_attribute_single,
                class_code=ClassCode.identity_object,
                instance=1,
                attribute=3,
                data_type=UINT,
                name="productCode"
            )

            # Read serial number
            _, serial, _, _ = drive.generic_message(
                service=Services.get_attribute_single,
                class_code=ClassCode.identity_object,
                instance=1,
                attribute=6,
                data_type=UDINT,
                name="serial"
            )
            serial_hex = hex(serial)[2:]
            local_info.append(serial_hex)

            # Read MAC address (6 parts)
            mac_address_num = 687
            mac_parts = []
            for _ in range(6):
                _, part, _, _ = drive.generic_message(
                    service=Services.get_attribute_single,
                    class_code=b'\x93',
                    instance=mac_address_num,
                    attribute=b'\x09',
                    data_type=INT,
                    connected=False,
                    unconnected_send=True,
                    route_path=True,
                    name="HW ADDR"
                )
                mac_parts.append(f'{part:x}')
                mac_address_num += 1
            mac_address = "-".join(mac_parts)
            local_info.append(mac_address)

            # Read parameters depending on product code
            if product_code == 9:
                # PF525 style
                for num, param_name in PARAM_LIST_525.items():
                    _, value, _, _ = drive.generic_message(
                        service=Services.get_attribute_single,
                        class_code=b'\x93',
                        instance=num,
                        attribute=b'\x09',
                        data_type=INT,
                        connected=False,
                        unconnected_send=True,
                        route_path=True,
                        name=param_name
                    )
                    # Map specific params
                    if param_name == 'Start Source1':
                        value = START_SOURCE_MAP.get(value, value)
                    elif param_name == 'Speed Ref':
                        value = SPEED_REF_MAP.get(value, value)
                    local_info.append(value)

            elif product_code == 2192:
                # PF755 style
                for num, param_name in PARAM_LIST_755.items():
                    _, value, _, _ = drive.generic_message(
                        service=Services.get_attribute_single,
                        class_code=b'\x93',
                        instance=num,
                        attribute=b'\x09',
                        data_type=REAL,
                        connected=False,
                        unconnected_send=True,
                        route_path=True,
                        name=param_name
                    )
                    local_info.append(value)

            else:
                print(f"Unknown product code {product_code} on module {module} at {ip}")

            return (module, local_info)

    except Exception:
        print(f"Cannot connect to drive: {module} ({ip})")
        traceback.print_exc()
        return (module, None)


def multi_get_VFD_info(plc_list, max_workers=10):
    """
    Concurrently reads VFD info for a list of (module, ip) tuples.
    Returns a dict of module -> list of local_info lists.
    """
    vfd_info = defaultdict(list)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(read_pf525_parameter, ip, module) for module, ip in plc_list]
        for future in futures:
            module, info = future.result()
            if info:
                vfd_info[module].append(info)
    return vfd_info


def build_drive_path(ip, slot, drive_ip):
    return f"{ip}/bp/{slot}/enet/{drive_ip}"


def main():
    # example plc lists and drives from your plc module
    # You would replace these with actual data from your 'plc' module
    area = int(input("Enter area: 1=Primary, 2=Secondary, 3=Outbound, 4=Smalls: "))

    plc_list = []
    if area == 1:
        # build primary list
        pass  # your original logic here to populate plc_list
    elif area == 2:
        # secondary
        pass
    elif area == 3:
        # outbound
        pass
    elif area == 4:
        # smalls
        pass
    else:
        print("Invalid area selected.")
        return

    # collect data
    vfd_info = multi_get_VFD_info(plc_list)

    # sort and output
    sorted_vfd_info = dict(sorted(vfd_info.items(), key=lambda x: tuple(map(int, re.findall(r'\d+', x[0])))))

    with open('VFD_Info.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            'Module', 'IP Address', 'Serial#', 'MacAddress', 'Firmware', 'Output Freq',
            'CMD Freq', 'Output Current', 'Output Voltage', 'DC Bus Voltage',
            'Accel', 'Deccel', 'Start Source', 'Speed Ref'
        ])

        for module, infos in sorted_vfd_info.items():
            for info in infos:
                writer.writerow([module] + info)

    print("VFD info collection complete and saved to VFD_Info.csv")


if __name__ == "__main__":
    main()
