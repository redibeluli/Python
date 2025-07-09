from pycomm3 import LogixDriver
import collections
import time
#import requests
import csv
import plc

#this program pulls all package info from all style sorters and populates them into CSV table
# Redi Beluli 04/21/22

# *************************package queues for different style sorters:*********************************

# legacy        - Q_Pkgs, pointer -> Pntr_Induct
# intelligrated - eSORT_DATABASE.CARTON{1000}, pointer -> 
#		        - Secondary -> Q_Pkgs_A,Q_Pkgs_B
# dematic       - P45 & P46 - SC.SORT.CID{1000}, pointer -> .....
#			    - SLS 11, S1 & S2 - TrkID{3500}, pointer -> SORT_TRKID


def flip_status_bits(Sts):
	return NOT(-sts) + 1


module = input("Enter Module: ").lower()

print (module)

idx = 2
if module in ["p41","p43","rc4"]:
	Q_size = 1000
	type = 4
elif module in ["sls11","s1","s2"]:
	if module == 'sls11':
		Q_size = 2000
		idx = 4
	elif module == 's2':
		Q_size = 351
		idx = 3
	else:
		Q_size = 351
	type = 6
elif module in ["p45","p46"]:
	type = 5
	Q_size = 1000
elif 's' in module and module not in ['sls11', 'sls12', 'sls13','s1','s2']:
	if 'a' in module:
		module = module.replace('a','')
		type = 1
		DC_Msgs_Str = 'Divert_Confirm_Msgs_A'
	elif 'b' in module:
		module = module.replace('b','')
		type = 2
		DC_Msgs_Str = 'Divert_Confirm_Msgs_B'
	else:
		type = 3
		DC_Msgs_Str = 'Divert_Confirm_Msgs'
else:
	type = 0
	DC_Msgs_Str = 'Divert_Confirm_Msgs'


try:
	ip = plc.modules[module]
except:
	print ("Invalid module name...exiting")
	exit()

pkg_Str, Pntr_Str, vol_str  = plc.getStruct(type,idx)

#print (pkg_Str, Pntr_Str, vol_str)

#test IP address
#ip = '10.67.45.27'

Pkg = []
Pkg2 = []


#print (pkg_Str, Pntr_Str, vol_str)

with LogixDriver(ip,init_program_tags = True) as plc:
	
	print ("Connected to " + module + " PLC (at " + ip + ')' )
	print ("type:", type)
	if type in [0,1,2,3]:
		Q_size = plc.tags[pkg_Str]['dimensions'][0]
	#plc.get_tag_list('Main')
	#print ("Connected to: " + ip2)
	#print(plc.info)
	#print(plc.tags)

	name,Pntr,datatype,errMsg = plc.read(Pntr_Str)
	name,Volume,datatype,errMsg = plc.read(vol_str)

	if Volume == 0:
		Volume = 1000

	print ('Array size:', Q_size)
	print ("Current Volume: ", Volume)
	print ("Pntr: ", Pntr)
	if Volume < Q_size:
		if Pntr - Volume > 0:
			Strt = Pntr - Volume
		else:
			Strt = 0
			Strt2 = Q_size + (Pntr - Volume)
			End = -1*(Pntr - Volume)

		if Strt != 0:
			print ('Reading Q_Pkgs' + '[' + str(Strt) + ']' + '{' + str(Volume) + '}...')
			name,Pkg,datatype,errMsg = plc.read(pkg_Str + '[' + str(Strt) + ']' + '{' + str(Volume) + '}')
			#name,DC_Msgs,datatype,errMsg = plc.read(DC_Msgs_Str + '[' + str(Strt) + ']' + '{' + str(Volume) + '}') { commented out to run on S1 9/3/24}
			print (errMsg)
			
		else:
			print ('Reading Q_Pkgs' + '[' + str(Strt) + ']' + '{' + str(Pntr) + '} and ' + 
				  'Q_Pkgs' + '[' + str(Strt2) + ']' + '{' + str(End) + '}...')
			name,Pkg,datatype,errMsg = plc.read(pkg_Str + '[' + str(Strt) + ']' + '{' + str(Pntr) + '}')
			#name,DC_Msgs,datatype,errMsg = plc.read(DC_Msgs_Str + '[' + str(Strt) + ']' + '{' + str(Pntr) + '}')		
			name,Pkg2,datatype,errMsg = plc.read(pkg_Str + '[' + str(Strt2) + ']' + '{' + str(End) + '}')
			#name,DC_Msgs2,datatype,errMsg = plc.read(DC_Msgs_Str + '[' + str(Strt2) + ']' + '{' + str(End) + '}')
			#print ('Q_Pkgs2' + '[' + str(Strt2) + ']' + '{' + str(End) + '}')
	else:
		Strt = -999
		Volume = Q_size
		S_Q_size = '{' + str(Q_size) + '}'
		print ("getting all pkgs, setting volume to size of package que...")
		name,Pkg,datatype,errMsg = plc.read(pkg_Str + '[0]'+ S_Q_size)
		if type in [1,2,3]:
			name,DC_Msgs,datatype,errMsg = plc.read(DC_Msgs_Str + '[0]'+ S_Q_size)
		#print (errMsg)	
#legacy
if type in [0,1,2,3]:
	with open('PackageData.csv', 'w', encoding='UTF8', newline='') as csvfile:
		spamwriter = csv.writer(csvfile,)
		spamwriter.writerow(['PkgID', 'CamID', 'RT_Total'
							,'Induct_Length','PLCID_Length','Dest[0]','NumDest','DetectedOTE'
							,'Status','ReasonCode','DivTime','DC MSG', 'DestConfirmed'])

		for i,parcel in enumerate(Pkg):

			#special case for negative reason code
			val = parcel['Status']
			if val > 0:
				str_Status1 = '0b' + '{:032b}'.format(val)  
			else:
				str_Status1 = bin(val+(1<<32))
			#print (parcel['PkgID'])
			#print (len(DC_Msgs))
			#print (DC_Msgs[parcel['PkgID']])
			spamwriter.writerow([parcel['PkgID'], parcel['CamID'], float(int(parcel['RT_In']) - int(parcel['RT_Out']))/1000
								,parcel['Induct_Length'], int(parcel['TE_PC']) - int(parcel['LE_PC']),parcel['Dest'][0], parcel['Dest'][1]
								,str_Status1[4] == '1', str_Status1, parcel['ReasonCode'], parcel['DivTime'], DC_Msgs[i],DC_Msgs[i][57:62]])

		if Pkg2:
			for i, parcel in enumerate(Pkg2):

				val = parcel['Status']
				if val > 0:
					str_Status2 = '0b' + '{:032b}'.format(val)   
				else:
					str_Status2 = bin(val+(1<<32))
				#print (DC_Msgs2[i])
				#print (DC_Msgs2[i][57:62])
				spamwriter.writerow([parcel['PkgID'], parcel['CamID'], float(int(parcel['RT_In']) - int(parcel['RT_Out']))/1000
								,parcel['Induct_Length'], int(parcel['TE_PC']) - int(parcel['LE_PC']),parcel['Dest'][0], parcel['Dest'][1]
								,str_Status2[4] == '1', str_Status2, parcel['ReasonCode'], parcel['DivTime'],DC_Msgs2[i],DC_Msgs2[i][57:62]])
#inteligrated
elif type == 4:
	Status_Str = collections.defaultdict(lambda: "NewCode")
	Status_Str[0]    = "Good"
	Status_Str[1]    = "Good"
	Status_Str[4]    = "????"
	Status_Str[5]    = "Good"
	Status_Str[16]   = "Jam"
	Status_Str[32]   = "Jam&Full"
	Status_Str[64]   = "Disabled"
	Status_Str[256]  = "Failed"
	Status_Str[260]  = "Unexpected&Overlap"
	Status_Str[1024] = "GapErr"
	Status_Str[1025] = "GapErr&Divert"
	Status_Str[1028] = "Unexpected&Gap"
	Status_Str[2048] = "Lost"
	Status_Str[2052] = "NoDest&Lost"
	Status_Str[3072] = "MultipleIssue"
	Status_Str[3076] = "NoDest&Lost"
	Status_Str[4096] = "NotUpToSpeed"
	Status_Str[6144] = "IDK"
	Status_Str[8012] = "Overlength"

	with open('Book2.csv', 'w', encoding='UTF8', newline='') as csvfile:
		spamwriter = csv.writer(csvfile,)
		spamwriter.writerow(['PkgID', 'Barcode', 'RT_Total'
							,'Induct_Length','NumDest','StatusCode','Status'])

		for parcel in Pkg:
			spamwriter.writerow([parcel['PKGUID'],parcel['BARCODE'], parcel['DA_RESPONSE_TIME']
								,parcel['LENGTH'], parcel['INT_DEST'], parcel['STATUS_CODE'], Status_Str[parcel['STATUS_CODE']]])

		if Pkg2:
			for parcel in Pkg2:
				spamwriter.writerow([parcel['PKGUID'], parcel['BARCODE'], parcel['DA_RESPONSE_TIME']
									,parcel['LENGTH'], parcel['INT_DEST'], parcel['STATUS_CODE'], Status_Str[parcel['STATUS_CODE']]])
#dematic
elif type == 5: 
	dispo = ["UNTRANS","NO_CODE","FULL","CHOKE","GAP_ERROR","LENGTH_CHANGE",
			 "FAILED","MUZ","INH","AMBIG","LOST","TOO_LATE",
			 "LATE_HOST","NO_HOST","INVALID","NOT_AT_SPEED"]

	with open('Book2.csv', 'w', encoding='UTF8', newline='') as csvfile:
		spamwriter = csv.writer(csvfile,)
		spamwriter.writerow(['PkgID', 'Barcode', 'Leading Slat'
							,'Length','NumDest','Status', 'Count'])

		for parcel in Pkg:
			cnt = 0
			for status in dispo:
				if parcel['DISPO'][status] == 1:
					reason = status
					cnt += 1
			if cnt == 0:
				reason = 'Good Divert'

			spamwriter.writerow([parcel['PKGUID'], parcel['TRKNUM'], parcel['LE_SLAT']
								,parcel['LENGTH'], parcel['ACT_DEST'], reason, cnt])

		if Pkg2:
			for parcel in Pkg2:
				cnt = 0
				for status in dispo:
					if parcel['DISPO'][status] == 1:
						reason = status
						cnt += 1
				if cnt == 0:
					reason = 'Good Divert'
				spamwriter.writerow([parcel['PKGUID'], parcel['TRKNUM'], parcel['LE_SLAT']
									,parcel['LENGTH'], parcel['ACT_DEST'], reason, cnt])
#SLS11, S1, S2
else:
	errors = ["Gap_Lead_Error","Gap_Trail_Error","Chute_Full","Chute_Jam","SCU_Fault","Destination_Disabled",
			 "Lost_in_Track","Parcel_Too_Long","No_Sort_MSG_from_Chat","Sorter_NOT_At_Speed","SORT_Command_Late","SORT_Command_Invalid_Dest",
			 "SORT_Command_Null_Dest", "No_Sort_MSG_from_Chat", "SORT_Command_Unknown_ID", "Head_Error", "Tail_Error",
			 "Covered_Error","Double_Error"]
	reason = "????"
	with open('Book2.csv', 'w', encoding='UTF8', newline='') as csvfile:
		spamwriter = csv.writer(csvfile,)
		spamwriter.writerow(['PkgID', 'Barcode','Length','Dest','SortedDest','CameraData_Rcvd','SortData_Rcvd','Diverted?','Status', 'Count',"Full Status"])

		for parcel in Pkg:
			cnt = 0
			status_word = ''

			for status in parcel['Status']:
				if parcel['Status'][status] == 1:
					status_word = status_word + ',' + status

			for error in errors:
				if parcel['Status'][error] == 1:
					reason = error
					cnt += 1
			'''
			if cnt == 0 and parcel['Status']['Diverted'] == 1:
				reason = 'Good Divert'
			'''
			if cnt == 0:
				reason = ''

			spamwriter.writerow([parcel['PkgID'], parcel['SortData']['TRKNUM'],
								 parcel['PLength'],parcel['SortData']['Dest'][0],parcel['SortedDest'],parcel['Status']['CameraData_Rcvd'],parcel['Status']['SortData_Rcvd'],
								 parcel['Status']['Diverted'], reason, cnt, status_word])

		if Pkg2:
			for parcel in Pkg2:
				cnt = 0
				for error in errors:
					if parcel['Status'][error] == 1:
						reason = error
						cnt += 1
				if cnt == 0 and parcel['Status']['Diverted'] == 1:
					reason = 'Good Divert'
			spamwriter.writerow([parcel['PkgID'], parcel['SortData']['TRKNUM'],
								 parcel['PLength'],parcel['SortedDest'],parcel['Status']['CameraData_Rcvd'],parcel['Status']['SortData_Rcvd'],
								 parcel['Status']['Diverted'], reason, cnt, status_word])




     


