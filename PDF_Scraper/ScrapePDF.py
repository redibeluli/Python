from pdfminer.pdfparser import PDFParser, PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTTextBox, LTTextLine
import re
import io
import csv
import openpyxl
import time
import datetime
import os
import pyodbc
import shutil
import sys
import email
import imaplib
import traceback


'''
   Script created by Redi Beluli 11/30/2017 
   
   Retrieves new primary module data from "ATC Operations Report" PDF files and inputs the data into SQL DB(dbRedi) and XLSX worksheet.
   Handles multiple files at a time and moves them to "processed" folder after data has been extracted 
   Requires "PDF_Data2.xlsx" and "Out_PDF.txt" to be on the desktop of the machine running the script
'''


debug = False
excel_out = True

connprod = pyodbc.connect('DRIVER={SQL Server};SERVER=PSGSQL;DATABASE=dbRedi;UID=wwUser;pwd=wwUser')
cursor = connprod.cursor()

if excel_out:
   wb = openpyxl.load_workbook('PDF_Data2.xlsx')
   Data = wb.get_sheet_by_name("Data")

#go through each PDF document in the directory
files = os.listdir('\\\\Svrp00054056\\cach\\UPSApps\\PE\\PSG\\Apps\\Process_Engineering\\Reports\\ATC_REPORTS')

for file in files:
     
   #get date and sort from filename
   filename = file

   if filename == 'Processed':
      print("All files processed")
      break

   if filename[6:9] == 'ARB':
      shutil.move('\\\\Svrp00054056\\cach\\UPSApps\\PE\\PSG\\Apps\\Process_Engineering\\Reports\\ATC_REPORTS\\' + file, '\\\\Svrp00054056\\cach\\UPSApps\\PE\\PSG\\Apps\\Process_Engineering\\Reports\\ATC_REPORTS\\Processed\\' + file)
      continue
      
   print(filename)
   
   date_idx = filename.find('_sort')
   #print(date_idx)

   sub_date = filename[date_idx - 10 : date_idx]
   #print(sub_date)

   sub_sort = filename[10:13]
   #print(sub_sort)

   sort = ''

   if sub_sort == 'Sun':
      sort = 'Sunrise'
   elif sub_sort == 'Day':
      sort = 'Day'
   elif sub_sort == 'Twi':
      sort = 'Twilight'
   elif sub_sort == 'Nig':
      sort = 'Night'
    
   fp = open('\\\\Svrp00054056\\cach\\UPSApps\\PE\\PSG\\Apps\\Process_Engineering\\Reports\\ATC_REPORTS\\' + file, 'rb')
   
   parser = PDFParser(fp)
   doc = PDFDocument()
   parser.set_document(doc)
   doc.set_parser(parser)
   doc.initialize('')
   rsrcmgr = PDFResourceManager()
   laparams = LAParams()
   device = PDFPageAggregator(rsrcmgr, laparams=laparams)
   interpreter = PDFPageInterpreter(rsrcmgr, device)
   fp.close()
   # Process each page contained in the document.

   F = open("Out_PDF.txt","w")
   
   #write data from PDF into txt document to be parsed later 
   for pageNumber, page in enumerate(doc.get_pages()):
      if pageNumber == 8:
          break
      interpreter.process_page(page)
      layout = device.get_result()
      for lt_obj in layout:
          if isinstance(lt_obj, LTTextBox) or isinstance(lt_obj, LTTextLine):
               #print(lt_obj.get_text())
               F.write(lt_obj.get_text())
   
   
   Scan_Volume = []
   In_Volume = []
   Net_Srt_Volume = []
   Sort_Duration = []
   Off_End = []
   Package_Rejects = []
   ASX_Rejects = []
   Rejects = []
   No_ASX = []
   No_Tracking = []
   Label_Conflicts = []
   Oversized_Pkgs = []
   No_Decode = []
   Max_Rev_Rejects = []

   line_num = 0
   F.close()
   f = open('Out_PDF.txt', 'r+')
   
   scanned = 0
   rej = 0
   ASX_rej = 0
   label = 0
   pkgs = 0

   #parse txt file(line by line) for data we are interested in
   line = f.readline()
   
   while line:

       if line == "4 of 19":
          break
      
       line_num += 1
       if line.startswith('CACH'):
          if scanned < 4:
             row = f.readline()
             if re.search('[a-zA-Z]', row):
                row = f.readline()
                if re.search('[a-zA-Z]', row):
                   Scan_Volume.append(f.readline().rsplit(' ',1)[0].rstrip())
                else:
                   Scan_Volume.append(row.rsplit(' ',1)[0].rstrip())
             else:
                Scan_Volume.append(row.rsplit(' ',1)[0].rstrip())
             scanned += 1
       elif line.startswith('Inducted Vol'):
          row = f.readline()
          if row.startswith('Net Sorted'):
             Net_Srt_Volume.append(row.rsplit()[3].rstrip())
             In_Volume.append('0')
          else:
             In_Volume.append(row.rsplit()[0].rstrip())
       elif line.startswith('Net Sorted Vol'):
           Net_Srt_Volume.append(line.split()[3].rstrip())
       elif line.startswith('Sort Duration'):
           Sort_Duration.append(line.split()[3].rstrip())
       elif line.startswith('Package Rejects'):
          if line[15] == '\n':
             #do nothing
             pass
          elif rej % 2 == 0:
             Package_Rejects.append(line.rsplit()[2].rstrip())
             rej += 1
          else:
              rej += 1
       elif line.startswith('ASX Rejects'):
          if len(line) > 13:
             ASX_Rejects.append(line.rsplit()[2].rstrip())
       elif line.startswith('Oversized Pkgs'):
          if pkgs % 2 == 0:
             row = f.readline()
             if '%' not in row:
                Oversized_Pkgs.append(row.split()[0].rstrip())
          pkgs += 1
       elif line.startswith('Max Rev Rejects'): #special case for no tracking
           row = line
           No_Tracking.append(f.readline().rsplit(' ',1)[0].rstrip())
           label = f.readline()
           if '%' not in label.rsplit(' ',1)[0].rstrip():
               Label_Conflicts.append(label.rsplit(' ',1)[0].rstrip())
           else:
              Label_Conflicts.append(f.readline().rsplit(' ',1)[0].rstrip())

           decode = f.readline()
           if '%' not in decode.rsplit(' ',1)[0].rstrip():
              No_Decode.append(decode.rsplit(' ',1)[0].rstrip())
           else:
              No_Decode.append(f.readline().rsplit(' ',1)[0].rstrip())           
       elif line.startswith('Off the end'):
           Off_End.append(f.readline().rsplit(' ',1)[0].rstrip())
       elif line.startswith('Rejects'):
           Rejects.append(f.readline().rsplit(' ',1)[0].rstrip())
           
       line = f.readline()

   sort1 = [sort,sort,sort,sort]
   date1 = [sub_date,sub_date,sub_date,sub_date]
   Module = ['Primary','P41','P43','RC4']

   data = []

   data.append(Module)
   data.append(sort1)
   data.append(date1)
   data.append(Scan_Volume)
   data.append(In_Volume)
   data.append(Net_Srt_Volume)
   data.append(Sort_Duration)
   data.append(Off_End)
   data.append(Package_Rejects)
   data.append(ASX_Rejects)
   data.append(No_Tracking)
   data.append(Label_Conflicts)
   data.append(No_Decode)
   data.append(Oversized_Pkgs)
   data.append(Rejects)

   #print(data)
   #print(In_Volume)
   #print(Oversized_Pkgs)
   #print(Scan_Volume)
   #print(In_Volume)
   #print(Net_Srt_Volume)

   for item in data[3:]:  
      for i in range(0,4):
         if len(item) < 4:
            for x in range(len(item),4):
               item.append('0')
               
   #print(data)
               
   for item in data[3:]:  
      for i in range(0,4):
         if re.search('[a-zA-Z]', item[i]):
            item[i] = '0'
         
         if '.' not in item[i]:
            item[i] = int(item[i].replace(',', ''))
         else:
            pass
   #print(data)

   data = list(map(list, zip(*data))) #transpose list (crucial step)

   #Write data to TXT doc-------------------------------------------------------------------------------------
   '''
   for x in zip(*data):
     for y in x:
       Result.write(y+'\t')
     Result.write('\n')
   '''
   #Write data to CSV file -----------------------------------------------------------------------------------
   '''
   csvfile = "book1.csv"

   with open(csvfile, "w") as output:
       writer = csv.writer(output, lineterminator='\n')
       writer.writerows(data)
   '''
   #Write data into a an XLSX file----------------------------------------------------------------------------
   
   #"Data" is the tab in the XLSX worksheet,  lowercase data is the list containing the lists of different sort stats (2D array)

   if excel_out:
      #Get last empty(available) row
      x = Data['D1']

      Current_row = 0
           
      while x.value is not None:
         x = Data.cell(row=1+Current_row, column=4)

         if x.value is None:
            if Data.cell(row=2+Current_row, column=4).value is None:
               Current_row += 1
               break
            else:
               x = Data.cell(row=2+Current_row, column=4)
         Current_row += 1

      if debug:
         print("current row: " + str(Current_row))                    
     
      #enter data into xlsx sheet   
      for item in data:   
         for i in range(1,16):
            Data.cell(row=Current_row,column=i).value = item[-1+i]
         Current_row = Current_row + 1
      
   #Insert data into SQLServer DB
   for item in data:
      result = [item]
      try:
         cursor.executemany("INSERT INTO tbl_New_Mod_Data (Module,Sort,SortDate,[Scan Volume],[Induct Volume],[Net Sorted Volume],[Sort Duration],[Off The End],[Package Rejects],[ASX Rejects],[No Tracking], [Label Conflicts], [No Decode], [Oversized Pkgs],[Total Rejects]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", result)
         cursor.commit()
      except:
         print("exception reached")
         connprod = pyodbc.connect('DRIVER={SQL Server};SERVER=PSGSQL;DATABASE=dbInfoXPIIBE;UID=wwUser;pwd=wwUser')
         cursor2 = connprod.cursor()
         cursor2.executemany("INSERT INTO tbl_New_Mod_Data_Duplicate (Module,Sort,SortDate,[Scan Volume],[Induct Volume],[Net Sorted Volume],[Sort Duration],[Off The End],[Package Rejects],[ASX Rejects],[No Tracking], [Label Conflicts], [No Decode], [Oversized Pkgs],[Total Rejects]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", result)
         cursor2.commit()
         cursor2.close()

   f.close()
   #Result.close()
   '''
   try:
      os.remove('\\\\Svrp00054056\\cach\\UPSApps\\PE\\PSG\\Apps\\Process_Engineering\\Reports\\ATC_REPORTS\\' + file)
      print('deleted')
   except OSError:
      #print("File moved: Could not delete")

   print("here")
   
   if os.path.isfile('\\\\Svrp00054056\\cach\\UPSApps\\PE\\PSG\\Apps\\Process_Engineering\\Reports\\ATC_REPORTS\\' + file):
      print("attempting to move")
      try:
         print("trying to move file")
         #shutil.move('\\\\Svrp00054056\\cach\\UPSApps\\PE\\PSG\\Apps\\Process_Engineering\\Reports\\ATC_REPORTS\\' + file, '\\\\Svrp00054056\\cach\\UPSApps\\PE\\PSG\\Apps\\Process_Engineering\\Reports\\ATC_REPORTS\\Processed\\' + file)   
      except Exception as e:
         print(e)
    '''  
   if debug:
      print("Scanned Volume:")
      print(Scan_Volume)
      print("Inducted Volume:")
      print(In_Volume)
      print("Net Sorted Volume:")
      print(Net_Srt_Volume)
      print("No Tracking")
      print(No_Tracking)
      print("Sort Duration")
      print(Sort_Duration)
      print("OFF the end:")
      print(Off_End)
      print("Package Rejects")
      print(Package_Rejects)
      print("ASX Rejects")
      print(ASX_Rejects)
      print("Label Conflicts")
      print(Label_Conflicts)
      print("No Decode")
      print(No_Decode)
      print("Oversized Packages")
      print(Oversized_Pkgs)
      print("Total Rejects")
      print(Rejects)
      print()

#close cursor object
cursor.close()

if excel_out:
   wb.save('PDF_Data2.xlsx')


