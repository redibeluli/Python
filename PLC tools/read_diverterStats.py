from pycomm3 import LogixDriver
from collections import Counter, defaultdict
import plc
import csv
import math
import time
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import shutil
import os



#tag = 'Diverter_diags[23]'
#sorters = {'s3d1':'10.67.42.27','s3d2':'10.67.42.37'}


sorters = {'s3d1':'10.67.42.27','s3d2':'10.67.42.37',
        's4d1':'10.67.44.27','s4d2':'10.67.44.37',
        's5d1':'10.67.47.27','s5d2':'10.67.47.37',
        's6d1':'10.67.49.27','s6d2':'10.67.49.37',
        's7d1':'10.67.52.27','s7d2':'10.67.52.37',
        's8d1':'10.67.54.27','s8d2':'10.67.54.37',
        's9d1':'10.67.57.27','s9d2':'10.67.57.37',
        's10d1':'10.67.59.27','s10d2':'10.67.59.37',
        's11d1':'10.67.9.27','s11d2':'10.67.9.37'}

sorters2 = {'s12d1':'10.67.10.27','s12d2':'10.67.10.37',
        's13d1':'10.67.14.27','s13d2':'10.67.14.37',
        's14d1':'10.67.15.27','s14d2':'10.67.15.37',
        's15d1':'10.67.19.27','s15d2':'10.67.19.37',
        's16d1':'10.67.21.27','s16d2':'10.67.21.37',
        's17d1':'10.67.24.27','s17d2':'10.67.24.37',
        's18d1':'10.67.26.27','s18d2':'10.67.26.37',
        's19d1':'10.67.32.27','s19d2':'10.67.32.37',
        's20d1':'10.67.34.27','s20d2':'10.67.34.37'}

Diverter_Info = defaultdict(list)

for module in sorters.keys():
    Diverter_Info[module]
for module in sorters2.keys():
    Diverter_Info[module]

#mods = [item for pair in zip(Diverter_Info.keys(), ['']*len(Diverter_Info)) for item in pair]
mods = []
for item in Diverter_Info.keys():
    mods.append(item)
    mods.extend(['','','',''])

mods.insert(0, '')
print(mods)

metrics = ['']

print ('Here we goooooo')
def get_diverter_info(ip, sorter):

    print(f'***{sorter}***')
    with LogixDriver(ip, init_tags= True, init_program_tags = False) as plc:
        overall_cycle_ave = 0
        overall_latency_ave = 0
        metrics.append('Cycle Time')
        metrics.append('Latency')
        metrics.append('Fired')
        metrics.append('Confirmed')
        metrics.append('DC_Percent')
        for x in range(1,22):
            a,cycle_time,c,errMsg= plc.read(f'Diverter_diags[{x}].Cycle_Time_Ave')
            a,latency,c, errMsg = plc.read(f'Diverter_diags[{x}].Latency_Ave')
            a,fired,c,errMsg= plc.read(f'Diverter_diags[{x}].Fired')
            a,confirmed,c, errMsg = plc.read(f'Diverter_diags[{x}].Confirmed_Total')
            truncated_cycle = math.trunc(cycle_time)
            truncated_latency = math.trunc(latency)
            overall_cycle_ave += truncated_cycle
            overall_latency_ave += truncated_latency

            if fired > 0:
                DC_percent = (confirmed/fired) * 100
            else:
                DC_percent = 0

            Diverter_Info[sorter].append(truncated_cycle)
            Diverter_Info[sorter].append(truncated_latency)
            Diverter_Info[sorter].append(fired)
            Diverter_Info[sorter].append(confirmed)
            Diverter_Info[sorter].append(DC_percent)
            #print (f'Diverter {x} -- cycle time average: {truncated_cycle}ms, latency: {truncated_latency}ms')
        #print('')
        #print(f'Overall cycle time average: {overall_cycle_ave/21}')
        #print(f'Overall latency average: {overall_latency_ave/21}')
        #print(f' error message: {errMsg}')
        #Diverter_Info[sorter].append(b)
    #print (Diverter_Info)

def multi_get_div_info(sorters, workers=20):
    start_time = datetime.now()
    print ("fetching PLC info...")
    with ThreadPoolExecutor(max_workers=workers) as executor:
    	[executor.submit(get_diverter_info, ip = ip_address, sorter = module) for module,ip_address in sorters.items()]
    print (f'| Time taken {datetime.now() - start_time}')


def yes_no_prompt(question):
    while True:
        answer = input(f"{question} (yes/no): ").lower()
        if answer in ["yes", "y"]:
            return True
        elif answer in ["no", "n"]:
            return False
        else:
            print("Invalid input. Please enter yes or no.")

if yes_no_prompt("Do you want to sum the totals?"):
    sum_data = 1
    print("Data will be added to totals...")
else:
    sum_data = 0
    print("Data will not be added to totals...")





multi_get_div_info(sorters)
multi_get_div_info(sorters2)
#print (Diverter_Info)

diverters =[['Div1',],['Div2'],['Div3'],['Div4'],
            ['Div5'],['Div6'],['Div7'],['Div8'],
            ['Div9'],['Div10'],['Div11'],['Div12'],
            ['Div13'],['Div14'],['Div15'],['Div16'],
            ['Div17'],['Div18'],['Div19'],['Div20'],
            ['Div21']]

#refine list for CSV 
for x in range(21):
     for module, Div_info in Diverter_Info.items():
          div_index = x * 5
          diverters[x].append(Div_info[div_index])
          diverters[x].append(Div_info[div_index+1])
          diverters[x].append(Div_info[div_index+2])
          diverters[x].append(Div_info[div_index+3])
          diverters[x].append(Div_info[div_index+4])


_date = date.today()
filename = 'Diverter_Stats_' + _date.strftime("%Y-%m-%d") + '.csv'
with open(filename, 'w', encoding='UTF8', newline='') as csvfile:

    spamwriter = csv.writer(csvfile,)
    spamwriter.writerow(mods)
    spamwriter.writerow(metrics)
    for diverter in diverters:
        #print (diverter)
        spamwriter.writerow(diverter)

if sum_data:
    df = pd.read_csv('Diverter_Stat_Totals.csv')
    #109 cells in full script
    mod_index = 3
    for mod in range(1,109,3):
        for div in range(1,22):
            #print(f'mod {mod} div {div} diverter value {diverter[3]} datatype {type(diverter[3])}')
            df.iloc[1:, mod] = pd.to_numeric(df.iloc[1:, mod], errors='coerce')
            df.iloc[1:, mod+1] = pd.to_numeric(df.iloc[1:, mod+1], errors='coerce')
            #print(f'mod {mod} mod index: {mod_index}')
            df.iloc[div, mod] += diverters[div - 1][mod_index]
            df.iloc[div, mod+1] += diverters[div - 1][mod_index+1]
        mod_index += 5
    
    # Save the updated DataFrame back to the CSV file
    df.iloc[23,1] += 1
    df.to_csv('Diverter_Stat_Totals.csv', index=False)


time.sleep(1)

dst_dirname = "\\\\svrp000a22cd\\CACH\\UPSApps\\PE\\PSG\\Apps\\Process_Engineering\\PMI_Project_Group\\PLC_Programs\\PLC_FILES_12_08_2017 Newest!!\\DiverterStats"
src_filename = "Diverter_Stat_Totals.csv"
dst_filename = os.path.join(dst_dirname, os.path.basename(src_filename))

#print(dst_dirname)
shutil.copy2(src_filename,dst_filename)
