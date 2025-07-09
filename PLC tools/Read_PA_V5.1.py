import time
import requests
import pandas as pd
import openpyxl
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# Copyright © Redi Beluli 11/23/2022 - Do not modify this code or you will go straight to jail.

headers = {'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/106.0.1370.72'}

sorts = { 1: ["T09:30:00.000Z&endTime=","T15:29:59.999Z&locale=en-US", "Sunrise"]
		, 2: ["T15:30:00.000Z&endTime=","T22:29:59.999Z&locale=en-US", "Day"]
		, 3: ["T22:30:00.000Z&endTime=","T03:29:59.999Z&locale=en-US", "Twilight"]
		, 4: ["T03:30:00.000Z&endTime=","T09:29:59.999Z&locale=en-US","Night"]
		, 5: ["T06:00:00.000Z&endTime=","T15:29:59.999Z&locale=en-US", "Mon-Sunrise"]}


pmods = ["399","398","146","152", "82", "151", "84", "153", "397"
		,"172","409","97","428","95","402","96","401","94","85","110"
		,"90","109","106","143","168","139","108","140","105","144"
		,"123","141","88","142","121","124","403","430","400","176"
		,"207","390", "177","212","214", "173", "183","185","184"
		,"186","411","412","421","420","217","419","213","215",]

day = int(input ("Enter day 1-6 (0 = today, 1 = yesterday...etc.) "))
#day = 1

df_date_str = str((datetime.today() - timedelta(days=day)).date())
day_num = ((datetime.today() - timedelta(days=day)).date()).weekday()
#print (day_num)

sort_arr = [1,2,3,4]
if day_num == 0: #special case for monday sunrise
	sort_arr = [5,2,3,4]
elif day_num == 5 or day_num == 6:
	print ("No data for Saturday or Sunday...Exiting")
	quit()

print ("Date:", df_date_str)

def get_dates(day):

	if sort == 4:
		date_ = str((datetime.today() - timedelta(days=day-1)).date())
		date2_ = str((datetime.today() - timedelta(days=day-1)).date())
	elif sort == 3:		
		date_ = str((datetime.today() - timedelta(days=day)).date())
		date2_ = str((datetime.today() - timedelta(days=day-1)).date())
	else:
		date_ = str((datetime.today() - timedelta(days=day)).date())
		date2_ = str((datetime.today() - timedelta(days=day)).date())
	return date_ , date2_

def get_data(data,data2, headers, sort, page=1):
	# Get start time
	start_time = datetime.now()
	url = f'http://10.66.225.88:8080/fa/api/v1/facility/1/system/{pmods[page]}/statistics/aggregatedreadrate/shift/device?startTime={date_}{sorts[sort][0]}{date2_}{sorts[sort][1]}'
	#URL for getting totes
	url2 = f'http://10.66.225.88:8080/fa/api/v1/facility/1/system/{pmods[page]}/statistics/aggregatedreadrate/shift?startTime={date_}{sorts[sort][0]}{date2_}{sorts[sort][1]}'
	#print (url)
	r = requests.get(url)
	r2 = requests.get(url2)
	# If the requests is fine, proceed
	if r.ok and r2.ok:
		#print("webpage accessed")
		data.append(r.json())
		data2.append(r2.json())
	#else:
		#print('connection issues or incorrect URL formats...check that URLs return JSON result')
	return data, data2
    
def multi_get_data(data, data2, headers,sort,workers=58):
    start_time = datetime.now()
    # Execute our get_data in multiple threads each having a different page number
    print ("fetching data...")
    with ThreadPoolExecutor(max_workers=workers) as executor:
    	[executor.submit(get_data, data=data, data2= data2, sort=sort,headers=headers,page=x) for x in range(58)]
    
    print(f'| Time taken {datetime.now() - start_time}')
    return data, data2

def sort_list(dl):

	sorted_list = [None] * 58
	sorted_list2 = [None] * 58
	for item in dl:
		idx = pmods.index(item[0]["systemId"])
		sorted_list[idx] = item[0]
		sorted_list2[idx] = item[1] 
	return sorted_list, sorted_list2

def mapped_read_rates(size, reads, rates):

	if size == 17:

		tote_idx = 12
		m_reads = ['', '',reads[6],reads[7],reads[8],reads[9],reads[10],reads[11],
						reads[12],reads[13],reads[14],reads[15],reads[16],reads[0],
						reads[1],reads[2], reads[3],reads[4], reads[5]]

		m_rates = [rates[0],rates [1],rates[8],rates[9],rates[10],rates[11],rates[12],rates[13],
						rates[14],rates[15],rates[16],rates[17],rates[18],rates[2],
						rates[3],rates[4], rates[5],rates[6], rates[7]]

	elif size == 6:
		# 6 devices
		tote_idx = 6
		m_reads = ['', '',reads[0],reads[1],reads[2],reads[3],reads[4],reads[5]]
		m_rates = [rates[0],rates[1],rates[2],rates[3],rates[4],rates[5],rates[6],rates[7]]

	else:
		tote_idx = 1		
		m_reads = ['', '',reads[1],reads[2],reads[3],reads[4],reads[0]]
		m_rates = [rates[0],rates[1],rates[3],rates[4],rates[5],rates[6],rates[2]]

	return m_reads, m_rates, tote_idx 


df_cntr = 0 
dfs = [[],[],[],[]]

for sort in sort_arr:
	
	print ("\n")
	date_, date2_ = get_dates(day)
	print ("Sort:", sorts[sort][2])

	df_modules = []
	df_scanners = []
	Totes = []
	data = []
	data2 = []
	data_list1, data_list2 = multi_get_data(data,data2,headers,sort)
	sorted_list, sorted_list2 = sort_list(zip(data_list1, data_list2))

	if not any(sorted_list):
		print("No data for sort, check URL times are okay or connection issues")
		continue
	print ("Errors:")
	for i, pmod in enumerate(sorted_list):

		if pmod is None:
			print(f'No result from module P{i} URL fetch...check module device #.')
			continue

		if not pmod["deviceStatistics"] or (len(pmod["deviceStatistics"]) == 6 and len(pmod["deviceStatistics"][1]["statistics"]) == 1):
			print (f'No PA data for {pmod["systemName"]}, Module URL fetch good')
			df_modules.append(pmod["systemName"])
			df_modules.append("")
			df_modules.append("Totes")
			df_scanners.append(["","","N/A"])
			df_scanners.append(["","","N/A"])
			df_scanners.append(["","","N/A"])
			continue	

		#print (pmod["systemName"], "Volume:",pmod["validObjectCount"], "--Scanners/Cameras:", len(pmod["deviceStatistics"]))	
		reads_x = []
		rates_x = [pmod["validObjectCount"],""]
		device_num = len(pmod["deviceStatistics"])

		if device_num in [17,6,5]:

			if device_num == 17:
				cnt_idx = 8
			else:
				cnt_idx = 1

			for scanner in range(device_num):				
				reads_x.append(pmod["deviceStatistics"][scanner]["statistics"][cnt_idx]["conditionCount"])
				rates_x.append(pmod["deviceStatistics"][scanner]["statistics"][cnt_idx]["readRate"])

			mapped_reads, mapped_rates, tote_index = mapped_read_rates(device_num,reads_x,rates_x)

			df_scanners.append(mapped_reads)
			df_scanners.append(mapped_rates)
			df_scanners.append(['',sorted_list2[i]["statistics"][tote_index]["conditionCount"],'', '', '',"","","","","","","","","","","","","", ""])
			df_modules.append(pmod["systemName"])
			df_modules.append("")
			df_modules.append("Totes")			
		else:
			print ("Warning # of scanners in PA not 17,6, or 5...skipping")


	#print (df_scanners)
	#print (df_modules)
	if df_scanners and df_modules and len(df_scanners[0]) != 3:
		dfs[df_cntr] = pd.DataFrame(df_scanners,
		              index=df_modules, columns=[' Volume ',sorts[sort][2] + "-" + df_date_str ,'1', '2', '3',"4","5","6","7","8","9","10","11","12","13","14","15","16", "17"])
	df_cntr += 1

#change to different value if you don't want to write to local excel file
local = 1

if local == 1:
	path = 'ScanData.xlsx'
else:	
	path = 'P:\\UPSApps\\PE\\PSG\\Apps\\PKED\\Scanner Performance\\Data2\\ScanData.xlsx'

#check for at least one dataframe
xyz = [isinstance(dfs[x],pd.DataFrame) for x in range(4)]
if any(xyz):
	#with pd.ExcelWriter('P:\\UPSApps\\PE\\PSG\\Apps\\PKED\\Scanner Performance\\Data2\\ScanData.xlsx') as writer:
	with pd.ExcelWriter(path) as writer:
		for x in range(4):
			if isinstance(dfs[x],pd.DataFrame):
				dfs[x].to_excel(writer, sheet_name=sorts[x+1][2])
else:
	print("no dataframes!...nothing written to excel")




