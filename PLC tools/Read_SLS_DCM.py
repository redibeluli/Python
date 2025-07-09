from pycomm3 import LogixDriver
from collections import Counter
import plc
import csv

#ip = "10.67.97.86/1"
#tag = 'astDCM[19].stStatistics'

sorters = {'SLS15':'10.67.97.86/1','SLS14' : '10.67.96.87/1','SLS16' : '10.67.93.57/1','SLS3' : '10.67.104.108/1','SLS10' : '10.67.102.84/1','SLS4' : '10.67.91.87/1'}
print ('Here we goooooo')

DCM_Info = {'SLS15':[],'SLS14':[],'SLS16':[],'SLS3':[],'SLS10':[],'SLS4':[]}

for sorter, ip in sorters.items():
    print(f'***{sorter}***')
    with LogixDriver(ip, init_tags= True, init_program_tags = False) as plc:

        for i in range(11,244,8):
            print (i)
            a,b,c,d = plc.read(f'astDCM[{i}].stStatistics')
            #print (b)
            DCM_Info[sorter].append(b)


with open('DCM_Info.csv', 'w', encoding='UTF8', newline='') as csvfile:

    spamwriter = csv.writer(csvfile,)
    for sorter, sorter_DCM_Stats in DCM_Info.items():

        spamwriter.writerow([sorter,'DCM #','Disabled', 'Pin_Flt','Prox_Flt','Connection','HeartBeatTx','HeartbeatRx','VaneTravelTime','VaneChargingTime','IncorrectSpeed', 'DidNotSwitch',"SolenoidFailure","GeneralFault","PotentialMissingPin"])
        DCM_num = 11

        for DCM in sorter_DCM_Stats:
            spamwriter.writerow(['',DCM_num,DCM['diDisabled'], DCM['diPin'],DCM['diPX'],
                                    DCM['diConnection'],DCM['diHeartbeatTx'],DCM['diHeartbeatRx'],DCM['diVaneTravelTime'],DCM['diVaneChargingTime'], DCM['diIncorrectSpeed'],
                                    DCM['diVaneDidNotSwitch'],DCM['diSolenoidFailure'], DCM['diGeneralFault'],DCM['diPotentialMissingPin']])
            DCM_num += 8