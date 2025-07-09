from pycomm3 import LogixDriver
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
import plc
import shutil
import os


def get_plc_info(module,ip, f_list, p_list):

    #print (f'module: {module}, ip: {ip}')
    try:
        with LogixDriver(ip, init_tags= False, init_program_tags = False) as plc:
            total[0] += 1
            mode = plc.info['keyswitch']
            if mode != 'RUN':
                naughty_list.append(module)
                count[0] += 1
            major = plc.info['revision']['major']
            minor = plc.info['revision']['minor']
            f_list.append(str(major) + '.' + str(minor))
            processor = plc.info['product_name'].split()[0]
            p_list.append(processor)
            print (f'{module} --v{major}.{minor} {processor} KeySwitch: {mode}')
    except:
        print (f'Could not connect to {module}, check IP address')

        
def multi_get_data(data, data2,workers=58):
    start_time = datetime.now()
    print ("fetching PLC info...")
    with ThreadPoolExecutor(max_workers=workers) as executor:
    	[executor.submit(get_plc_info, module = module, ip = ip_address, f_list = data, p_list = data2) for module,ip_address in plc.modules.items()]
    
    print(f'| Time taken {datetime.now() - start_time}')
    return data, data2

firmware_list = []
processor_list = []
naughty_list = []
total = [0]
count = [0]

firmware_list , processor_list = multi_get_data(firmware_list, processor_list)
firmware_count = Counter(firmware_list)
processor_count = Counter(processor_list)

count = count[0]
total = total[0]

with open("PLC_Keyswitch.txt", "w") as file:
    file.write(f"PLCs not in Run: {count}/{total} \n")
    for module in naughty_list:
        file.write(f'"{module}", ')
    file.write('\n')
    file.write('\n')
    file.write('Processor breakdown: \n')
    for processor, cnt in processor_count.items():
        file.write(f'{processor}: ({cnt}/{total}) {round(cnt/total*100,2)}% \n')
    file.write('Firmware Breakdown \n')
    for firmware, cnt in firmware_count.items():
        file.write(f'{firmware}: ({cnt}/{total}) {round(cnt/total*100,2)}% \n')

print ('\n')
print (f"PLCs not in Run: {count}/{total}")
print (naughty_list)
print ('\n')
print('Processor breakdown:')
for processor, count in processor_count.items():
    print (f'{processor}: ({count}/{total}) {round(count/total*100,2)}%')
print ('\n')
print ('Firmware breakdown:')
for firmware, count in firmware_count.items():
    print (f'{firmware}: ({count}/{total}) {round(count/total*100,2)}%')

#dst_dirname = "Z:\\PLC_Files"
dst_dirname = "\\\\svrp000a22cd\\CACH\\UPSApps\\PE\\PSG\\Apps\\PKED\\PLC Security"
src_filename = "PLC_Keyswitch.txt"
dst_filename = os.path.join(dst_dirname, os.path.basename(src_filename))

#print(dst_dirname)
shutil.move(src_filename,dst_filename)



