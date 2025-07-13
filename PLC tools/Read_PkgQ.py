from pycomm3 import LogixDriver
import collections
import time
import csv
import plc


# This script pulls all package info from style sorters and populates it into a CSV file.

def flip_status_bits(sts):
    return ~(-sts) + 1


def get_module_type_and_size(module):
    idx = 2
    module = module.lower()
    if module in ["p41", "p43", "rc4"]:
        return 4, 1000, idx
    elif module in ["sls11", "s1", "s2"]:
        if module == 'sls11':
            return 6, 2000, 4
        elif module == 's2':
            return 6, 351, 3
        else:
            return 6, 351, idx
    elif module in ["p45", "p46"]:
        return 5, 1000, idx
    elif 's' in module and module not in ['sls11', 'sls12', 'sls13', 's1', 's2']:
        if 'a' in module:
            return 1, None, idx, 'Divert_Confirm_Msgs_A'
        elif 'b' in module:
            return 2, None, idx, 'Divert_Confirm_Msgs_B'
        return 3, None, idx, 'Divert_Confirm_Msgs'
    return 0, None, idx, 'Divert_Confirm_Msgs'


def main():
    module = input("Enter Module: ").lower()
    print(f"Selected module: {module}")

    result = get_module_type_and_size(module)
    if len(result) == 3:
        type_id, q_size, idx = result
        dc_msgs_str = 'Divert_Confirm_Msgs'
    else:
        type_id, q_size, idx, dc_msgs_str = result

    try:
        ip = plc.modules[module]
    except KeyError:
        print("Invalid module name... exiting")
        return

    pkg_str, pntr_str, vol_str = plc.getStruct(type_id, idx)
    pkg, pkg2 = [], []

    with LogixDriver(ip, init_program_tags=True) as driver:
        print(f"Connected to {module} PLC at {ip}")
        print(f"Sorter type: {type_id}")

        if type_id in [0, 1, 2, 3]:
            q_size = driver.tags[pkg_str]['dimensions'][0]

        _, pntr, _, _ = driver.read(pntr_str)
        _, volume, _, _ = driver.read(vol_str)
        volume = volume or 1000

        print(f"Array size: {q_size}")
        print(f"Current Volume: {volume}")
        print(f"Pointer: {pntr}")

        if volume < q_size:
            if pntr - volume > 0:
                start = pntr - volume
                _, pkg, _, _ = driver.read(f"{pkg_str}[{start}]{{{volume}}}")
            else:
                start = 0
                start2 = q_size + (pntr - volume)
                end = -1 * (pntr - volume)
                _, pkg, _, _ = driver.read(f"{pkg_str}[{start}]{{{pntr}}}")
                _, pkg2, _, _ = driver.read(f"{pkg_str}[{start2}]{{{end}}}")
        else:
            volume = q_size
            _, pkg, _, _ = driver.read(f"{pkg_str}[0]{{{q_size}}}")
            if type_id in [1, 2, 3]:
                _, dc_msgs, _, _ = driver.read(f"{dc_msgs_str}[0]{{{q_size}}}")

    # Write to CSV depending on type
    if type_id in [0, 1, 2, 3]:
        write_legacy_csv(pkg, pkg2, dc_msgs if 'dc_msgs' in locals() else [])
    elif type_id == 4:
        write_intelligrated_csv(pkg, pkg2)
    elif type_id == 5:
        write_dematic_csv(pkg, pkg2)
    else:
        write_sls_csv(pkg, pkg2)


def write_legacy_csv(pkg, pkg2, dc_msgs):
    with open('PackageData.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['PkgID', 'CamID', 'RT_Total', 'Induct_Length', 'PLCID_Length',
                         'Dest[0]', 'NumDest', 'DetectedOTE', 'Status', 'ReasonCode',
                         'DivTime', 'DC MSG', 'DestConfirmed'])

        for i, p in enumerate(pkg):
            status_str = format_status(p['Status'])
            writer.writerow([p['PkgID'], p['CamID'], (int(p['RT_In']) - int(p['RT_Out'])) / 1000,
                             p['Induct_Length'], int(p['TE_PC']) - int(p['LE_PC']),
                             p['Dest'][0], p['Dest'][1], status_str[4] == '1',
                             status_str, p['ReasonCode'], p['DivTime'],
                             dc_msgs[i], dc_msgs[i][57:62]])

        for i, p in enumerate(pkg2):
            status_str = format_status(p['Status'])
            writer.writerow([p['PkgID'], p['CamID'], (int(p['RT_In']) - int(p['RT_Out'])) / 1000,
                             p['Induct_Length'], int(p['TE_PC']) - int(p['LE_PC']),
                             p['Dest'][0], p['Dest'][1], status_str[4] == '1',
                             status_str, p['ReasonCode'], p['DivTime'],
                             dc_msgs[i], dc_msgs[i][57:62]])


def write_intelligrated_csv(pkg, pkg2):
    status_map = collections.defaultdict(lambda: "NewCode", {
        0: "Good", 1: "Good", 4: "????", 5: "Good", 16: "Jam",
        32: "Jam&Full", 64: "Disabled", 256: "Failed", 260: "Unexpected&Overlap",
        1024: "GapErr", 1025: "GapErr&Divert", 1028: "Unexpected&Gap", 2048: "Lost",
        2052: "NoDest&Lost", 3072: "MultipleIssue", 3076: "NoDest&Lost",
        4096: "NotUpToSpeed", 6144: "IDK", 8012: "Overlength"
    })

    with open('Book2.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['PkgID', 'Barcode', 'RT_Total', 'Induct_Length', 'NumDest', 'StatusCode', 'Status'])

        for p in pkg + pkg2:
            writer.writerow([p['PKGUID'], p['BARCODE'], p['DA_RESPONSE_TIME'],
                             p['LENGTH'], p['INT_DEST'], p['STATUS_CODE'],
                             status_map[p['STATUS_CODE']]])


def write_dematic_csv(pkg, pkg2):
    dispo = ["UNTRANS", "NO_CODE", "FULL", "CHOKE", "GAP_ERROR", "LENGTH_CHANGE",
             "FAILED", "MUZ", "INH", "AMBIG", "LOST", "TOO_LATE",
             "LATE_HOST", "NO_HOST", "INVALID", "NOT_AT_SPEED"]

    with open('Book2.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['PkgID', 'Barcode', 'Leading Slat', 'Length', 'NumDest', 'Status', 'Count'])

        for p in pkg + pkg2:
            reason = 'Good Divert'
            cnt = sum(p['DISPO'][d] == 1 for d in dispo if d in p['DISPO'])
            for d in dispo:
                if p['DISPO'].get(d) == 1:
                    reason = d
            writer.writerow([p['PKGUID'], p['TRKNUM'], p['LE_SLAT'],
                             p['LENGTH'], p['ACT_DEST'], reason, cnt])


def write_sls_csv(pkg, pkg2):
    errors = ["Gap_Lead_Error", "Gap_Trail_Error", "Chute_Full", "Chute_Jam", "SCU_Fault",
              "Destination_Disabled", "Lost_in_Track", "Parcel_Too_Long", "No_Sort_MSG_from_Chat",
              "Sorter_NOT_At_Speed", "SORT_Command_Late", "SORT_Command_Invalid_Dest",
              "SORT_Command_Null_Dest", "SORT_Command_Unknown_ID", "Head_Error", "Tail_Error",
              "Covered_Error", "Double_Error"]

    with open('Book2.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['PkgID', 'Barcode', 'Length', 'Dest', 'SortedDest',
                         'CameraData_Rcvd', 'SortData_Rcvd', 'Diverted?', 'Status', 'Count', 'Full Status'])

        for p in pkg + pkg2:
            status_word = ','.join([e for e in p['Status'] if p['Status'][e] == 1])
            reason = next((e for e in errors if p['Status'].get(e) == 1), '')
            cnt = sum(p['Status'].get(e) == 1 for e in errors)

            writer.writerow([p['PkgID'], p['SortData']['TRKNUM'], p['PLength'],
                             p['SortData'].get('Dest', [None])[0], p['SortedDest'],
                             p['Status']['CameraData_Rcvd'], p['Status']['SortData_Rcvd'],
                             p['Status']['Diverted'], reason, cnt, status_word])


def format_status(val):
    return '0b' + '{:032b}'.format(val if val >= 0 else val + (1 << 32))


if __name__ == "__main__":
    main()
