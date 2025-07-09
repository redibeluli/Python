from pycomm3 import (CIPDriver, Services, ClassCode,  FileObjectServices, FileObjectInstances,
                     FileObjectInstanceAttributes, IdentityObjectInstanceAttributes, Struct, UDINT,UINT, USINT,INT,REAL, n_bytes)
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from collections import defaultdict
import time
import plc
import inspect
import traceback
import csv
import re

test_plc_list = {'s3d1':'10.67.42.27'}
module_VFD_info = defaultdict(list)
module_VFD_paths = []

def read_pf525_parameter(ip,module, vfd_info):
    param_list_525 = { 29: 'Firmware:', 1:'Output Freg:',2: 'CMD Freq:',
                   3: 'Output Current:',4 : 'Output_Voltage:',5: 'DC Bus Voltage:',
                  41: 'Accel:',42: 'Deccel:',46 :'Start Source1:',47 : 'Speed Ref:'}
    param_list_755 = { 29: 'Firmware:', 1:'Output Freg:',2: 'CMD Freq:',
                    7: 'Output Current:',8 : 'Output_Voltage:',11: 'DC Bus Voltage:',
                    535:'Accel', 537: 'Deccel', 3 :'Mtr Vel Fdbk', 4: 'Commanded Trq'}

    start_source = {1:'Keypad',2:'DigIn TrmBlk',4:'Network Opt',5:'Ethernet/IP'}
    speed_ref = {1:'Drive Pot',2:'Keypad',3:'SerialDSI',4:'Network Opt',5:'0-10V Input',6:'4-20mA Input',7: 'Preset Freq',15: 'Ethernet/IP'}

    local_info = [ip]
    print(module)
    try:
        with CIPDriver(ip, socket_timeout=3) as drive:


            a,b,c,d = drive.generic_message(
                service=Services.get_attribute_single,
                class_code=ClassCode.identity_object,
                instance=1,
                attribute=3,
                data_type=UINT,
                name="productCode:",
            )           
            product_code = b
            a,b,c,d = drive.generic_message(
                service=Services.get_attribute_single,
                class_code=ClassCode.identity_object,
                instance=1,
                attribute=6,
                data_type=UDINT,
                name="serial:",
            )
            #print (a,hex(b)[2:])
            serial = hex(b)[2:]
            local_info.append(serial)
            #vfd_info[module].append(hex(b)[2:])

            mac_address_num = 687
            mac_address = []
            for y in range (6):
                a,b,c,d = drive.generic_message(
                    service=Services.get_attribute_single,
                    class_code=b'\x93',
                    instance= mac_address_num,  # Parameter 41 = Accel Time
                    attribute=b'\x09',
                    data_type=INT,
                    connected=False,
                    unconnected_send=True,
                    route_path=True,
                    name= "HW ADDR"
                )
                mac_address.append(f'{b:x}')
                mac_address_num += 1
                #print (a,b)
            #print (f'MAC Address: {"-".join(mac_address)}')
            mac_address = "-".join(mac_address)
            local_info.append(mac_address)
            #vfd_info[module].append(f'MAC Address: {"-".join(mac_address)}')
            if product_code == 9:
                for num, nameStr in param_list_525.items():
                
                    a,b,c,d = drive.generic_message(
                        service=Services.get_attribute_single,
                        class_code=b'\x93',
                        instance= num,  # Parameter 41 = Accel Time
                        attribute=b'\x09',
                        data_type=INT,
                        connected=False,
                        unconnected_send=True,
                        route_path=True,
                        name= nameStr
                    )
                    
                    if a == 'Start Source1:':
                        b = start_source[b]
                    elif a == 'Speed Ref:':
                        b = speed_ref[b]
                    #print (a,b)
                    local_info.append(b)
            elif product_code == 2192:
                for num, param in param_list_755.items():
                    a,b,c,d = drive.generic_message(
                        service=Services.get_attribute_single,
                        class_code=b'\x93',
                        instance= num,  # Parameter 41 = Accel Time
                        attribute=b'\x09',
                        data_type=REAL,
                        connected=False,
                        unconnected_send=True,
                        route_path=True,
                        name= param
                    )
                    #print (a,b)
                    local_info.append(b)

            vfd_info[module].append(local_info)
            vfd_count[0] += 1
      
    except Exception as e:
        print (f'{module} Cannot connect to drive: {ip}')
        #traceback.print_exc()

def multi_get_VFD_info(plc_list,vfd_info_list, workers=408):
    start_time = datetime.now()
    print ("fetching PLC info...")
    with ThreadPoolExecutor(max_workers=workers) as executor:
    	[executor.submit(read_pf525_parameter, ip = ip_address, module = module, vfd_info = vfd_info_list) for module,ip_address in plc_list]
    print (f'| Time taken {datetime.now() - start_time} for {vfd_count[0]} VFDs')
    #return data


area = int(input("Enter area: 1 = Primary, 2 = Secondary, 3 = Outbound, 4 = Smalls: "))

if area == 1:
    #pri
    vfd_slot = 9
elif area == 2:
    #sec
    vfd_slot = 11
elif area == 3:
    #outbound 
    vfd_slot = 8

vfd_count = [0]
test = 0
if test:
    for module, ip in test_plc_list.items():
        print (module)
        for drive_ip in plc.outbound_drive_list:
            print (f'Drive IP: {drive_ip}')
            full_path = f'{ip}/bp/8/enet/{drive_ip}'
            read_pf525_parameter(full_path)
            print ('\n')
else:
    if area == 1:
        for module,ip in plc.primaries.items():
            #default to primary
            list_index = 0
            slot_num = 9
            if module == 'p41_MCP':
                list_index = 1
                slot_num = 5
            elif module == 'p43_MCP':
                list_index = 2
                slot_num = 5
            else:
                with CIPDriver(ip) as Pmod:
                    try:
                        print(Pmod.get_module_info(9))
                    except:
                        print('No ENET module in slot 9')
                        continue     
            for drive_ip in plc.primary_drive_list[list_index]:
                #print (f'Drive IP: {drive_ip}')
                full_path = f'{ip}/bp/{slot_num}/enet/{drive_ip}'
                #print(full_path)
                module_VFD_paths.append([module,full_path])
        multi_get_VFD_info(module_VFD_paths, module_VFD_info)
        #print(module_VFD_info)
    elif area == 2:
        #do secondary
        for module,ip in plc.secondaries.items():
            for drive_ip in plc.secondary_drive_list:
                #print (f'Drive IP: {drive_ip}')
                full_path = f'{ip}/bp/11/enet/{drive_ip}'
                module_VFD_paths.append([module,full_path])
        multi_get_VFD_info(module_VFD_paths, module_VFD_info)
        #print(module_VFD_info)
    elif area == 3:
        #do outbound
        for module,ip in plc.outbounds.items():
            if module == 's2_Soli':
                print("here")
                for drive_ip in plc.outbound_drive_list[1]:
                    #print (f'Drive IP: {drive_ip}')
                    full_path = f'{ip}/bp/3/enet/{drive_ip}'
                    module_VFD_paths.append([module,full_path])
            else:
                for drive_ip in plc.outbound_drive_list[0]:
                    #print (f'Drive IP: {drive_ip}')
                    full_path = f'{ip}/bp/8/enet/{drive_ip}'
                    module_VFD_paths.append([module,full_path])
        multi_get_VFD_info(module_VFD_paths, module_VFD_info)
        #print(module_VFD_info)
    elif area == 4:
        # sms, sas (two network cards), smls1, smls2
        for module,ip in plc.powerflex_smalls.items():
            if 'SMS' in module:
                #do this
                slot = 3
                for drive_ip in plc.smalls_drive_list[0]:
                    full_path = f'{ip}/bp/{slot}/enet/{drive_ip}'
                    module_VFD_paths.append([module,full_path])
            elif 'SAS' in module:
                slots = [2,3]
                for slot in slots:
                    if slot == 2:
                        #do this
                        for drive_ip in plc.smalls_drive_list[1]:
                            full_path = f'{ip}/bp/{slot}/enet/{drive_ip}'
                            module_VFD_paths.append([module,full_path])
                    else:
                        for drive_ip in plc.smalls_drive_list[2]:
                            full_path = f'{ip}/bp/{slot}/enet/{drive_ip}'
                            module_VFD_paths.append([module,full_path])

            elif 'SMLS' in module:
                slot = 5
                for drive_ip in plc.smalls_drive_list[3]:
                #print (f'Drive IP: {drive_ip}')
                    full_path = f'{ip}/bp/{slot}/enet/{drive_ip}'
                    module_VFD_paths.append([module,full_path])
                #do this
        multi_get_VFD_info(module_VFD_paths, module_VFD_info)
    else:
        print("invalid entry")

print("Processing data...")
#sort the information 
sorted_VFD_info = dict(sorted(module_VFD_info.items(), key=lambda x: tuple(map(int, re.findall(r'\d+', x[0])))))

#time.sleep(1)
with open('VFD_Info.csv', 'w', encoding='UTF8', newline='') as csvfile:

    spamwriter = csv.writer(csvfile,)
    spamwriter.writerow(['Module','IP Address','Serial#','MacAddress', 'Firmware','Output Freq','CMD Freq','Output Current','Output Voltage', 'DC Bus Voltage','Accel', 'Deccel', 'Start Source','Speed Ref'])
    for module, VFD_Info_List in sorted_VFD_info.items():
            spamwriter.writerow([module])
            for VFD_info in VFD_Info_List:
                spamwriter.writerow([module,VFD_info[0][-12:], VFD_info[1], VFD_info[2], VFD_info[3], VFD_info[4], VFD_info[5], 
                                     VFD_info[6], VFD_info[7], VFD_info[8], VFD_info[9], VFD_info[10], VFD_info[11], VFD_info[12]])




