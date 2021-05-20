import xlrd
from xlrd import XL_CELL_EMPTY
import xlwt
import datetime
import glob
import re
import sftpcred
import pysftp
import mysqllogin
import pypyodbc
import os
import csv


#### Connect to SFTP location #####

myUsername = sftpcred.user
myHostname = sftpcred.host
myPassword = sftpcred.passWord

opts = pysftp.CnOpts()
opts.hostkeys = None


# with pysftp.Connection(host=myHostname, port=22, username=myUsername, password=myPassword, cnopts=opts) as sftp:
	# print("connection succesfully established ...")
	
	# # switch to remote directory
	# sftp.cwd('./Pioneer/ready/outgoing')
	# # print(sftp.pwd)
	# filetoget = sftp.listdir()
	# print(filetoget)
	# # sftp.cwd('./pioneer/ready/incoming')
	
	# # obtain structure of the remote directory
	# directory_structure = sftp.listdir_attr()
	# for attr in directory_structure:
		# print(attr)
	
	# # print data
	# for file in filetoget:
		# print(file)
		# sftp.get_d( sftp.pwd, 'C:\\PB1\\3rdPartyFiles\\', preserve_mtime=True)
	
		
		
# input()


####  Get Last Day of the Previous Month ######

currentday = datetime.date.today()		## Gets the current Days date
#print(currentday.strftime('%Y%m%d'))
firstday = currentday.replace(day=1)		## Uses the current date to determine the first day of the month
# print(firstday.strftime('%Y%m%d'))
lastmonth = firstday - datetime.timedelta(days=1)		## Uses the first day of the month and finds the last day of the previous month
datesearch = lastmonth.strftime('%Y%m%d')		## Assigns the yyyymmdd format for using in finding the file with the correct date
uploaddate = currentday.strftime('%Y%m%d')		## Assigns the yyyymmdd format to the current date for noting the day of upload
yestday = currentday - datetime.timedelta(days=1)		## sets variable to yesterday's date
oseyestdate = currentday - datetime.timedelta(days=1)
yestdaydate = yestday.strftime('%Y%m%d')
osedate = oseyestdate.strftime('%m%d%y')		## Assigns the mmddyy format specific to finding the date for OSE file load
# print(osedate)
# print(yestdaydate)
# input()
# print(lastmonth.strftime('%Y%m%d'))

# findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\Amkor*{0}*'.format(yestdaydate))
# print(findfile)
# input()

### Lists to populate for read file information to populate in custom table ###

amkorList = []
asemList = []
chmEWSList = []
chmEWSList2 = []
chmASEMList = []
gtcList = []
oseList = []
promList = []
udgList =[]
utcASEMList = []
utcList = []
cmosList = []
iseList = []
evsEWSList = []
utlList=[]



### Amkor File processing function ###
def amkorFile():
	print("Start Amkor File upload...")
	
	cleanuplist = []

	
	with pysftp.Connection(host=myHostname, port=22, username=myUsername, password=myPassword, cnopts=opts) as sftp:
		print("connection succesfully established ...")
		
		# switch to remote directory
		sftp.cwd('./Pioneer/ready/outgoing/Amkor_WIP')
		# print(sftp.pwd)
		filetoget = sftp.listdir()
		# print(filetoget)
		# sftp.cwd('./pioneer/ready/incoming')
		
		# obtain structure of the remote directory
		directory_structure = sftp.listdir_attr()
		# for attr in directory_structure:
			# print(attr)
		
		# print data
		for file in filetoget:
			print(file)
			filetoclean = re.match(r".*{0}.*".format(yestdaydate), file)
			# print(filetoclean)
			sftp.get_d( sftp.pwd, 'C:\\PB1\\3rdPartyFiles\\', preserve_mtime=True)
			if filetoclean == None:
				sftp.remove(sftp.pwd+'/'+file)
			else:
				pass
		
	# input()
		
		
	
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=VALIDATION; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))		## Create Connection to SQL Server 
	cursor = sapsqlcon.cursor()		## Create Cursor Object
	
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\Amkor*WIP_Report_*{0}*'.format(yestdaydate))		## Find the Amkor File with the correct date
	# findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*WIP_Report_*')
	
	
	if findfile != []:
		filetoload = (findfile[0])		## Assign the file name to the location path variable
		wb = xlrd.open_workbook(filetoload)		## Open the workbook at the file path location
		sheet = wb.sheet_by_index(0)		## Assign which sheet to read the data from.  0 is Sheet1, 1 is Sheet2, etc.

		sheet.cell_value(0,0)		## Assign what portion of the Cells to start in.  0,0 starts in A1 postion

		### Calculation for which Row to start on and creating a row count to ensure all rows are captured in the output ###
		count = 3
		xclrows = (sheet.nrows) - count
		xclstart = 2

		# print(xclrows)

		### While loop to go through all rows in file and append to list which column/row data should be transfered to table in database custom table ###
		while count <= xclrows:
			results =(sheet.row_values(xclstart))
			verify = (results[3], results[13], results[14], results[15], str(results[20]).split('.')[0], results[24].replace("'",""), str(results[38]).split('.')[0], str(results[56]).split('.')[0], str(results[62]).split('.')[0])
			amkorList.append(verify)	
			count = count + 1
			xclstart = xclstart + 1
	else:
		filetoload = 'NULL'
	
	### For loop to iterate through appended list and insert values to custom table ###
	if amkorList != None:
		for x in amkorList:
			factname, itemcode, itemcode2, lot, ponum, status, recvdate, recvqty, curqty = x
			if factname == 'ATC':
				pass
			else:
				query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode = '{1}' and LotNum = '{2}'".format('Amkor', itemcode, lot))
				cursor.execute(query)
				result = cursor.fetchone()
				# print(result)
				if result != None:
					# print(x)
					query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET VendorStatus = '{0}', UploadDate = '{1}', CurrentQty = '{2}', FileDate = '{3}', VendorFileName = '{4}' WHERE TransID = '{5}'".format(status, uploaddate, curqty, yestdaydate, str(filetoload).split('\\')[3], result[0])) 
					cursor.execute(query2)
				else:
					itemcheck = re.match(r'^E[S,X].*',itemcode)
					if itemcheck == None: 
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, PONum, Stage, VendorStatus, RecvDate, RecvQty, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, OrigUploadDate,FABWIPProcessed) values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', '{14}','N')".format(itemcode, itemcode2, lot, ponum, 'ASSEM', status, recvdate, recvqty, curqty, 'Amkor', uploaddate, yestdaydate, str(filetoload).split('\\')[3], 'Y', yestdaydate))
						cursor.execute(query1)
					else:
						itemcheck2 = re.match(r'^[A,I,P,S,L,M].*', itemcode2)
						if itemcheck2 == None:
							query3 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, PONum, Stage, VendorStatus, RecvDate, RecvQty, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, UpdateDate, UpdateReason, OrigUploadDate, FABWIPProcessed) values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','N')".format(itemcode, itemcode2, lot, ponum, 'ASSEM', status, recvdate, recvqty, curqty, 'Amkor', uploaddate, yestdaydate, str(filetoload).split('\\')[3], 'N', uploaddate, 'Row refers to Shield part and not assembly part.  False Positive', yestdaydate))
							cursor.execute(query3)
						else:
							query4 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, PONum, Stage, VendorStatus, RecvDate, RecvQty, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, OrigUploadDate,FABWIPProcessed) values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', '{14}','N')".format(itemcode, itemcode2, lot, ponum, 'ASSEM', status, recvdate, recvqty, curqty, 'Amkor', uploaddate, yestdaydate, str(filetoload).split('\\')[3], 'Y', yestdaydate))
							cursor.execute(query4)

	## ----  Update Third Party Recevied File (TPRF) table in Validation database for 3P file received ---- ##
	filequery = ("Select isnull(FileName,LastRecvFile) 'FileName', isnull(FileDate,LastRecvDate) 'FileDate' FROM VALIDATION.dbo.TPRF WHERE AsOfDate = '{0}' and Vendor = 'Amkor'".format(yestdaydate))
	cursor.execute(filequery)
	filecheck = cursor.fetchone()
	fileCheckList=[]
	fileCheckList.append(filecheck)
	
	if filecheck == None:
		nullstoinsert = [('NULL', 'NULL')]
		checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'Amkor' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
		cursor.execute(checktoupdate)
		result = cursor.fetchone()
		if result != None:
			for x in nullstoinsert:
				filename, filedate = x
				tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'Amkor' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(filetoload).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(tableupdatequery)
		else:
			for x in nullstoinsert:
				# print(x)
				filename, filedate = x
				fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('Amkor','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(filetoload).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(fileupdatequery)

	else:
		if filetoload == 'NULL':
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'Amkor' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result3 = cursor.fetchone()
			if result3 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = 'NULL', FileDate = '{0}', LastRecvFile = '{1}', LastRecvDate = '{2}' Where Vendor = 'Amkor' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(yestdaydate, filename, filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('Amkor','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format('NULL',yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
		else:
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'Amkor' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result2 = cursor.fetchone()
			if result2 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'Amkor' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(filetoload).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('Amkor','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(filetoload).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
		
	
	
	cursor.commit()
	cursor.close()
	sapsqlcon.close()

	print("Finished Amkor File upload...\n")		
		
### ASEM File processing function ###
def asemFile():

	print("Start ASEM File upload...")
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=VALIDATION; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))		## Create Connection to SQL Server 
	cursor = sapsqlcon.cursor()		## Create Cursor Object
	
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*REP_WIP*{0}*'.format(yestdaydate))		## Find the ASEM File with the correct date
	
	if findfile != []:
		loccheck = (findfile[0])		## Set file name to check
		if loccheck.endswith('.csv'):
			book = xlwt.Workbook()		## Create New Workbook
			sheet1 = book.add_sheet("Sheet1")		## Create New sheet in workbook
			with open(loccheck, 'rt', encoding='utf8') as f:		## Read .csv file
				reader = csv.reader(f)
				for r, row in enumerate(reader):		## iterate through .csv file rows
					for c, col in enumerate(row):		## iterate through .csv file columns
						sheet1.write(r,c,col)		## write data to new file
			book.save(loccheck + '.xls')		## Save new File as .xls
			os.remove(loccheck)		## Remove old .csv file
			findfile2 = glob.glob('C:\\PB1\\3rdPartyFiles\\*REP_WIP*{0}*.xls'.format(yestdaydate))		## Find updated extension file name
			loc = (findfile2[0])		## Assign the file name to the location path variable
		else:
			loc = loccheck		## Assign the file name to the location path variable

		# loc = (findfile[0])		## Assign the file name to the location path variable
		# print(loc)
		# input()

		wb = xlrd.open_workbook(loc)		## Open the workbook at the file path location
		sheet = wb.sheet_by_index(0)		## Assign which sheet to read the data from.  0 is Sheet1, 1 is Sheet2, etc.

		sheet.cell_value(0,0)		## Assign what portion of the Cells to start in.  0,0 starts in A1 postion

		### Calculation for which Row to start on and creating a row count to ensure all rows are captured in the output ###
		count = 4
		xclrows = (sheet.nrows) - count
		xclstart = 3

		### While loop to go through all rows in file and append to list which column/row data should be transfered to table in database custom table ###
		while count <= xclrows:
			results =(sheet.row_values(xclstart))
			verify = (results[0], results[1], results[2], str(results[7]).split('.')[0], str(results[9]).split('.')[0], str(results[11]).strip('P').split('.')[0], str(results[21]).split('.')[0])
			asemList.append(verify)	
			count = count + 1
			xclstart = xclstart + 1
			
		# print(asemList)
		# input()
	else:
		loc = 'NULL'
	### For loop to iterate through appended list and insert values to custom table ###
	if asemList != None:
		for x in asemList:
			status, item, lot, recvdate, recvqty, ponum, curqty = x
			query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode = '{1}' and LotNum = '{2}'".format('ASEM',item,lot))
			cursor.execute(query)
			result = cursor.fetchone()
			if result != None:
				query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET VendorStatus = '{0}', UploadDate = '{1}', CurrentQty = '{2}', FileDate = '{3}', VendorFileName = '{4}' WHERE TransID = '{5}'".format(status, uploaddate, curqty, yestdaydate, str(loc).split('\\')[3], result[0])) 
				cursor.execute(query2)		## This query will update an existing record
			else:
				itemcheck = re.match(r'^ES.*',item)
				if itemcheck == None:
					query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, VendorStatus, RecvDate, RecvQty, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, OrigUploadDate, FABWIPProcessed) values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','N')".format(item, lot, ponum, 'ASSEM', status, recvdate, recvqty, curqty, 'ASEM', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', yestdaydate))
					cursor.execute(query1)		## this query will insert a new record
				else:
					query3 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, VendorStatus, RecvDate, RecvQty, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, UpdateDate, UpdateReason, OrigUploadDate,FABWIPProcessed) values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','N')".format(item, lot, ponum, 'ASSEM', status, recvdate, recvqty, curqty, 'ASEM', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N',uploaddate,'Row refers to Shield part and not assembly part.  False Positive', yestdaydate))		## This query will flag the result as not valid
					cursor.execute(query3)
					
	## ----  Update Third Party Recevied File (TPRF) table in Validation database for 3P file received ---- ##
	filequery = ("Select isnull(FileName,LastRecvFile) 'FileName', isnull(FileDate,LastRecvDate) 'FileDate' FROM VALIDATION.dbo.TPRF WHERE AsOfDate = '{0}' and Vendor = 'ASEM'".format(yestdaydate))
	cursor.execute(filequery)
	filecheck = cursor.fetchone()
	fileCheckList=[]
	fileCheckList.append(filecheck)
	
	if filecheck == None:
		nullstoinsert = [('NULL', 'NULL')]
		checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'ASEM' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
		cursor.execute(checktoupdate)
		result = cursor.fetchone()
		if result != None:
			for x in nullstoinsert:
				filename, filedate = x
				tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'ASEM' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(filetoload).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(tableupdatequery)
		else:
			for x in nullstoinsert:
				# print(x)
				filename, filedate = x
				fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('ASEM','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(filetoload).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(fileupdatequery)
	else:
		if loc == 'NULL':
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'Amkor' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result3 = cursor.fetchone()
			if result3 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = 'NULL', FileDate = '{0}', LastRecvFile = '{1}', LastRecvDate = '{2}' Where Vendor = 'Amkor' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(yestdaydate, filename, filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('Amkor','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format('NULL',yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
		else:
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'ASEM' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result2 = cursor.fetchone()
			if result2 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'ASEM' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(filetoload).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('ASEM','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(filetoload).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
				
			
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	
	print("Finished ASEM File Upload....\n")

### CHM VE WIP File processing function ###
def chmVeWipFile():

	print("Start CHM WIP File upload...")
	
	# with pysftp.Connection(host=myHostname, port=22, username=myUsername, password=myPassword, cnopts=opts) as sftp:
		# print("connection succesfully established ...")
		
		# # switch to remote directory
		# sftp.cwd('./Pioneer/ready/outgoing')
		# # print(sftp.pwd)
		# filetoget = sftp.listdir()
		# print(filetoget)
		# # sftp.cwd('./pioneer/ready/incoming')
		
		# # obtain structure of the remote directory
		# directory_structure = sftp.listdir_attr()
		# for attr in directory_structure:
			# print(attr)
		
		# # print data
		# for file in filetoget:
			# print(file)
			# sftp.get_d( sftp.pwd, 'C:\\PB1\\3rdPartyFiles\\', preserve_mtime=True)
		
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=VALIDATION; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))		## Create Connection to SQL Server 
	cursor = sapsqlcon.cursor()		## Create Cursor Object
	
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*VE_WIP_*{0}*'.format(yestdaydate))		## Find the CHM VE WIP File with the correct date
	# findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*VE_WIP_*')

	if findfile != []:
		loc = (findfile[0])		## Assign the file name to the location path variable


		wb = xlrd.open_workbook(loc)		## Open the workbook at the file path location
		sheet = wb.sheet_by_index(0)		## Assign which sheet to read the data from.  0 is Sheet1, 1 is Sheet2, etc.

		sheet.cell_value(0,0)		## Assign what portion of the Cells to start in.  0,0 starts in A1 postion

		### Calculation for which Row to start on and creating a row count to ensure all rows are captured in the output ###
		count = 1
		xclrows = (sheet.nrows) - count
		# print(xclrows)
		xclstart = 1

		### While loop to go through all rows in file and append to list which column/row data should be transfered to table in database custom table ###
		while count <= xclrows:
			results =(sheet.row_values(xclstart))
			verify = (results[2], results[3], str(results[5]).split('.')[0], str(results[9]).split('.')[0], str(results[16]).split('.')[0])
			chmEWSList.append(verify)	
			# print(count)
			count = count + 1
			# print(xclstart)
			xclstart = xclstart + 1

			
		# print(asemList)
		# input()
		
		# print(str(loc).split('\\')[3])
		# input()
		
	### For loop to iterate through appended list and insert values to custom table ###
	if chmEWSList != None:
		for x in chmEWSList:
			item, lot, id, qty, ponum = x
			if item == 'Product':
				pass
			else:
				query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode = '{1}' and LotNum = '{2}' and RecvQty = '{3}'".format('CHM',item,lot, qty))
				cursor.execute(query)
				result = cursor.fetchone()
				if result != None:
					query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', CurrentQty = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}' WHERE TransID = '{5}'".format(uploaddate, qty, yestdaydate, str(loc).split('\\')[3], id, result[0])) 
					cursor.execute(query2)		## This query will update an existing record
				else:
					if id == '190':
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, RecvQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, OrigUploadDate, VendorStatus, FABWIPProcessed) values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}', '{10}','{11}','N')".format(item, lot, ponum, 'EWS',qty, 'CHM', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', yestdaydate, id))		## This query will insert new valid record
						cursor.execute(query1)
					else:
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, RecvQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, UpdateDate, UpdateReason,OrigUploadDate, VendorStatus,FABWIPProcessed) values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}', '{10}', '{11}','{12}','{13}','N')".format(item, lot, ponum, 'EWS',qty, 'CHM', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N',uploaddate, 'Engineering product.  False Positive', yestdaydate, id))		## this query will insert new invalid record and reason
						cursor.execute(query1)
						
	## ----  Update Third Party Recevied File (TPRF) table in Validation database for 3P file received ---- ##
	filequery = ("Select isnull(FileName,LastRecvFile) 'FileName', isnull(FileDate,LastRecvDate) 'FileDate' FROM VALIDATION.dbo.TPRF WHERE AsOfDate = '{0}' and Vendor = 'CHM'".format(yestdaydate))
	cursor.execute(filequery)
	filecheck = cursor.fetchone()
	fileCheckList=[]
	fileCheckList.append(filecheck)
	
	if filecheck == None:
		nullstoinsert = [('NULL', 'NULL')]
		checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'CHM' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
		cursor.execute(checktoupdate)
		result = cursor.fetchone()
		if result != None:
			for x in nullstoinsert:
				filename, filedate = x
				tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'CHM' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(filetoload).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(tableupdatequery)
		else:
			for x in nullstoinsert:
				# print(x)
				filename, filedate = x
				fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('CHM','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(filetoload).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(fileupdatequery)
	else:
		checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'CHM' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
		cursor.execute(checktoupdate)
		result2 = cursor.fetchone()
		if result2 != None:
			for x in fileCheckList:
				filename, filedate = x
				tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'CHM' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(filetoload).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(tableupdatequery)
		else:
			for x in fileCheckList:
				filename, filedate = x
				fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('CHM','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(filetoload).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(fileupdatequery)
				
				
			
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	
	print("Finished CHM WIP File Upload....\n")	

### CHM ASSEM File Processing function ###
def chmAsyFile():

	print("Start CHM ASSEM File upload...")
	
	with pysftp.Connection(host=myHostname, port=22, username=myUsername, password=myPassword, cnopts=opts) as sftp:
		print("connection succesfully established ...")
		
		# switch to remote directory
		sftp.cwd('./Pioneer/ready/outgoing/CHM_ASSY_WIP')
		# print(sftp.pwd)
		filetoget = sftp.listdir()
		# print(filetoget)
		# sftp.cwd('./pioneer/ready/incoming')
		
		# obtain structure of the remote directory
		directory_structure = sftp.listdir_attr()
		# for attr in directory_structure:
			# print(attr)
		
		# print data
		for file in filetoget:
			print(file)
			filetoclean = re.match(r".*{0}.*".format(yestdaydate), file)
			sftp.get_d( sftp.pwd, 'C:\\PB1\\3rdPartyFiles\\', preserve_mtime=True)
			if filetoclean == None:
				sftp.remove(sftp.pwd+'/'+file)
			else:
				pass
		
	# input()
		
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=VALIDATION; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))		## Create Connection to SQL Server 
	cursor = sapsqlcon.cursor()		## Create Cursor Object
	
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*wpsp006a*ASSY*{0}*'.format(yestdaydate))		## Find the ASEM File with the correct date
	
	if findfile != []:
		loccheck = (findfile[0])		## Set file name to check
		if loccheck.endswith('.csv'):
			book = xlwt.Workbook()		## Create New Workbook
			sheet1 = book.add_sheet("Sheet1")		## Create New sheet in workbook
			with open(loccheck, 'rt', encoding='utf8') as f:		## Read .csv file
				reader = csv.reader(f)
				for r, row in enumerate(reader):		## iterate through .csv file rows
					for c, col in enumerate(row):		## iterate through .csv file columns
						sheet1.write(r,c,col)		## write data to new file
			book.save(loccheck + '.xls')		## Save new File as .xls
			os.remove(loccheck)		## Remove old .csv file
			findfile2 = glob.glob('C:\\PB1\\3rdPartyFiles\\*wpsp006a_Everspin_ASSY*{0}*.xls'.format(yestdaydate))		## Find updated extension file name
			loc = (findfile2[0])		## Assign the file name to the location path variable
			# os.remove(findfile[0])
		else:
			loc = loccheck		## Assign the file name to the location path variable


		print("Starting workbookb parse...")
		wb = xlrd.open_workbook(loc)		## Open the workbook at the file path location
		sheet = wb.sheet_by_index(0)		## Assign which sheet to read the data from.  0 is Sheet1, 1 is Sheet2, etc.

		sheet.cell_value(0,0)		## Assign what portion of the Cells to start in.  0,0 starts in A1 postion

		### Calculation for which Row to start on and creating a row count to ensure all rows are captured in the output ###
		count = 4
		xclrows = (sheet.nrows) - count
		xclstart = 4
		

		
		# print(xclrows)
		# print(sheet.row_values(xclstart))
		# input()

		### While loop to go through all rows in file and append to list which column/row data should be transfered to table in database custom table ###
		while count <= xclrows:
			results =(sheet.row_values(xclstart))
			verify = (results[0], results[1], str(results[3]).split('.')[0], results[4], results[5], results[6], str(results[7]).split('.')[0], str(results[24]).split('.')[0],results[25])
			chmASEMList.append(verify)	
			count = count + 1
			xclstart = xclstart + 1
			
		# print(chmASEMList)
		# input()
	else:
		loc = 'NULL'
		
	### For loop to iterate through appended list and insert values to custom table ###
	if chmASEMList != None:
		for x in chmASEMList:
			check, check2, ponum, targetdev, item, parentlot, recvdate, curqty, lot = x
			if check.lower() == 'everspin':
				query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and TargetDevice = '{1}' and LotNum = '{2}' and Stage = 'ASSEM'".format('CHM', targetdev,lot))
				cursor.execute(query)
				result = cursor.fetchone()
				if result != None:
					query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', CurrentQty = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}' WHERE TransID = '{5}'".format(uploaddate, curqty, yestdaydate, str(loc).split('\\')[3], check2, result[0])) 
					cursor.execute(query2)		## This query will update an existing record
				else:
					if check2.lower() == 'mat':
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, PONum, Stage, VendorStatus, RecvDate, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, UpdateDate, UpdateReason, ParentLot, OrigUploadDate, FABWIPProcessed) values ('{0}','{1}','{2}','{3}','{4}', '{5}',Replace('{6}','/',''),'{7}','{8}','{9}','{10}','{11}','{12}', convert(date, getdate(),112), 'Item has Status of MAT.  False Positive', '{13}', '{14}','N')".format(item, targetdev, lot, ponum, 'ASSEM', check2, recvdate, curqty, 'CHM', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', parentlot, yestdaydate, check2)) 		## Query will insert new invalid record and reason
						cursor.execute(query1)
					else:
						query2 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, PONum, Stage, RecvDate, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, OrigUploadDate, VendorStatus,FABWIPProcessed) values ('{0}','{1}','{2}','{3}','{4}',replace('{5}','/',''),'{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', '{14}','N')".format(item, targetdev, lot, ponum, 'ASSEM', recvdate, curqty, 'CHM', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot, yestdaydate, check2))		## Query will insert new valid record
						cursor.execute(query2)
			else:
				pass
				
	## ----  Update Third Party Recevied File (TPRF) table in Validation database for 3P file received ---- ##
	filequery = ("Select isnull(FileName,LastRecvFile) 'FileName', isnull(FileDate,LastRecvDate) 'FileDate' FROM VALIDATION.dbo.TPRF WHERE AsOfDate = '{0}' and Vendor = 'CHMASY'".format(yestdaydate))
	cursor.execute(filequery)
	filecheck = cursor.fetchone()
	fileCheckList=[]
	fileCheckList.append(filecheck)
	
	if filecheck == None:
		nullstoinsert = [('NULL', 'NULL')]
		checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'CHMASY' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
		cursor.execute(checktoupdate)
		result = cursor.fetchone()
		if result != None:
			for x in nullstoinsert:
				filename, filedate = x
				tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'CHMASY' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(tableupdatequery)
		else:
			for x in nullstoinsert:
				# print(x)
				filename, filedate = x
				fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('CHMASY','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(fileupdatequery)
	else:
		if loc == 'NULL':
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'CHMASY' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result3 = cursor.fetchone()
			if result3 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = 'NULL', FileDate = '{0}', LastRecvFile = '{1}', LastRecvDate = '{2}' Where Vendor = 'CHMASY' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(yestdaydate, filename, filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('CHMASY','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format('NULL',yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
		else:
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'CHMASY' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result2 = cursor.fetchone()
			if result2 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'CHMASY' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('CHMASY','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
				
				
			
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	
	print("Finished CHM ASSEM File Upload....\n")	
	
### OSE ASSEM File Processing function ###	
def oseAsyFile():

	print("Start OSE File upload...")
	
	with pysftp.Connection(host=myHostname, port=22, username=myUsername, password=myPassword, cnopts=opts) as sftp:
		print("connection succesfully established ...")
		
		# switch to remote directory
		sftp.cwd('./Pioneer/ready/outgoing/OSE_WIP')
		# print(sftp.pwd)
		filetoget = sftp.listdir()
		# print(filetoget)
		# sftp.cwd('./pioneer/ready/incoming')
		
		# obtain structure of the remote directory
		directory_structure = sftp.listdir_attr()
		# for attr in directory_structure:
			# print(attr)
		
		# print data
		for file in filetoget:
			print(file)
			filetoclean = re.match(r".*{0}.*".format(osedate), file)
			sftp.get_d( sftp.pwd, 'C:\\PB1\\3rdPartyFiles\\', preserve_mtime=True)
			if filetoclean == None:
				sftp.remove(sftp.pwd+'/'+file)
			else:
				pass
	
	# input()	
		
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=VALIDATION; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))		## Create Connection to SQL Server 
	cursor = sapsqlcon.cursor()		## Create Cursor Object
	
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*OSE*WIP*Report*{0}*'.format(osedate))		## Find the ASEM File with the correct date

	if findfile != []:
		loc = (findfile[0])		## Assign the file name to the location path variable


		wb = xlrd.open_workbook(loc)		## Open the workbook at the file path location
		sheet = wb.sheet_by_index(0)		## Assign which sheet to read the data from.  0 is Sheet1, 1 is Sheet2, etc.

		sheet.cell_value(0,0)		## Assign what portion of the Cells to start in.  0,0 starts in A1 postion

		### Calculation for which Row to start on and creating a row count to ensure all rows are captured in the output ###
		count = 4
		xclrows = (sheet.nrows) - count
		xclstart = 7
		

		
		# print(xclrows)
		# print(sheet.row_values(xclstart))
		# input()

		### While loop to go through all rows in file and append to list which column/row data should be transfered to table in database custom table ###
		while count <= xclrows:
			results =(sheet.row_values(xclstart))
			verify = (results[0], results[1], results[3], str(results[4]).split('.')[0], results[6], str(results[8]).split('.')[0], str(results[9]).split('.')[0], str(results[31]).split('.')[0])
			oseList.append(verify)	
			count = count + 1
			xclstart = xclstart + 1
			
		# print(chmASEMList)
		# input()
	else:
		loc = 'NULL'
		
	### For loop to iterate through appended list and insert values to custom table ###
	if oseList != None:
		for x in oseList:
			status, item, lot1, ponum, lot2, qty1, qty2, recvdate = x
			if status.lower() == 'die bank':
				itemcheck = re.match(r'^ES.*',item)
				if itemcheck == None:
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}' and Stage = 'ASSEM'".format('OSE', item,lot2))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', CurrentQty = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}' WHERE TransID = '{5}'".format(uploaddate, qty1, yestdaydate, str(loc).split('\\')[3], status, result[0])) 
						cursor.execute(query2)		## This query will update an existing record
					else:
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, RecvDate, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, FABWIPProcessed,VendorStatus,OrigUploadDate) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','N','{11}','{12}')".format(item, lot2, ponum, 'ASSEM', recvdate, qty1, 'OSE', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y',status,yestdaydate))
						cursor.execute(query1)
				else:
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}' and Stage = 'ASSEM'".format('OSE', item,lot2))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', CurrentQty = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}' WHERE TransID = '{5}'".format(uploaddate, qty1, yestdaydate, str(loc).split('\\')[3], status, result[0])) 
						cursor.execute(query2)		## This query will update an existing record
					else:
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, RecvDate, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, UpdateDate, UpdateReason, FABWIPProcessed,VendorStatus,OrigUploadDate) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}', '{11}','{12}','N','{13}','{14}')".format(item, lot2, ponum, 'ASSEM', recvdate, qty1, 'OSE', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N',uploaddate, 'Shield Part.  Record is not valid',status,yestdaydate))
						cursor.execute(query1)
			elif status.lower() == 'running':
				itemcheck = re.match(r'^ES.*',item)
				if itemcheck == None:
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}' and Stage = 'ASSEM'".format('OSE', item,lot1))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', CurrentQty = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}' WHERE TransID = '{5}'".format(uploaddate, qty2, yestdaydate, str(loc).split('\\')[3], status, result[0])) 
						cursor.execute(query2)		## This query will update an existing record
					else:
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, RecvDate, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot,FABWIPProcessed,VendorStatus,OrigUploadDate) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','N','{12}','{13}')".format(item, lot1, ponum, 'ASSEM', recvdate, qty2, 'OSE', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', lot2,status,yestdaydate))
						cursor.execute(query1)
				else:
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}' and Stage = 'ASSEM'".format('OSE', item,lot1))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', CurrentQty = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}' WHERE TransID = '{5}'".format(uploaddate, qty2, yestdaydate, str(loc).split('\\')[3], status, result[0])) 
						cursor.execute(query2)		## This query will update an existing record
					else:
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, RecvDate, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, UpdateDate, UpdateReason, ParentLot,FABWIPProcessed,VendorStatus,OrigUploadDate) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','N','{14}','{15}')".format(item, lot1, ponum, 'ASSEM', recvdate, qty2, 'OSE', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', uploaddate, 'Shield Part, Record is not valid', lot2,status,yestdaydate))
						cursor.execute(query1)
			elif status.lower() == 'wait':
				itemcheck = re.match(r'^ES.*',item)
				if itemcheck == None:
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}' and Stage = 'ASSEM'".format('OSE', item,lot1))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', CurrentQty = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}' WHERE TransID = '{5}'".format(uploaddate, qty2, yestdaydate, str(loc).split('\\')[3], status, result[0])) 
						cursor.execute(query2)		## This query will update an existing record
					else:
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, RecvDate, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot,FABWIPProcessed,VendorStatus,OrigUploadDate) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','N','{12}','{13}')".format(item, lot1, ponum, 'ASSEM', recvdate, qty2, 'OSE', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', lot2,status,yestdaydate))
						cursor.execute(query1)
				else:
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}'  and Stage = 'ASSEM'".format('OSE', item,lot1))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', CurrentQty = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}' WHERE TransID = '{5}'".format(uploaddate, qty2, yestdaydate, str(loc).split('\\')[3], status, result[0])) 
						cursor.execute(query2)		## This query will update an existing record
					else:
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, RecvDate, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, UpdateDate, UpdateReason ParentLot,FABWIPProcessed,VendorStatus,OrigUploadDate) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','N','{14}','{15}')".format(item, lot1, ponum, 'ASSEM', recvdate, qty2, 'OSE', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N',uploaddate, 'Shield Part. Record is not valid', lot2,status,yestdaydate))
						cursor.execute(query1)
			else:
				pass
				
	## ----  Update Third Party Recevied File (TPRF) table in Validation database for 3P file received ---- ##
	filequery = ("Select isnull(FileName,LastRecvFile) 'FileName', isnull(FileDate,LastRecvDate) 'FileDate' FROM VALIDATION.dbo.TPRF WHERE AsOfDate = '{0}' and Vendor = 'OSE'".format(yestdaydate))
	cursor.execute(filequery)
	filecheck = cursor.fetchone()
	fileCheckList=[]
	fileCheckList.append(filecheck)
	
	if filecheck == None:
		nullstoinsert = [('NULL', 'NULL')]
		checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'OSE' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
		cursor.execute(checktoupdate)
		result = cursor.fetchone()
		if result != None:
			for x in nullstoinsert:
				filename, filedate = x
				tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'OSE' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(tableupdatequery)
		else:
			for x in nullstoinsert:
				# print(x)
				filename, filedate = x
				fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('OSE','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(fileupdatequery)
	else:
		if loc == 'NULL':
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'OSE' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result3 = cursor.fetchone()
			if result3 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = 'NULL', FileDate = '{0}', LastRecvFile = '{1}', LastRecvDate = '{2}' Where Vendor = 'OSE' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(yestdaydate, filename, filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('OSE','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format('NULL',yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
		else:
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'OSE' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result2 = cursor.fetchone()
			if result2 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'OSE' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('OSE','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
	
	
			
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	
	print("Finished OSE File Upload....\n")	
		
### Promise File Processing function ###	
def promiseFile():

	print("Start Promise WIP File upload...")
	
	with pysftp.Connection(host=myHostname, port=22, username=myUsername, password=myPassword, cnopts=opts) as sftp:
		print("connection succesfully established ...")
		
		# switch to remote directory
		sftp.cwd('./Pioneer/ready/outgoing/PROMIS_WIP')
		# print(sftp.pwd)
		filetoget = sftp.listdir()
		print(filetoget)
		# sftp.cwd('./pioneer/ready/incoming')
		
		# obtain structure of the remote directory
		directory_structure = sftp.listdir_attr()
		# for attr in directory_structure:
			# print(attr)
		
		# print data
		for file in filetoget:
			print(file)
			filetoclean = re.match(r".*{0}.*".format(yestdaydate), file)
			sftp.get_d( sftp.pwd, 'C:\\PB1\\3rdPartyFiles\\', preserve_mtime=True)
			if filetoclean == None:
				sftp.remove(sftp.pwd+'/'+file)
			else:
				pass
		
	# input()
		
		
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=VALIDATION; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))		## Create Connection to SQL Server 
	cursor = sapsqlcon.cursor()		## Create Cursor Object
	
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\Promis*EWS*WIP*{0}*'.format(yestdaydate))		## Find the ASEM File with the correct date
	

	if findfile != []:
		loccheck = (findfile[0])		## Set file name to check
		if loccheck.endswith('.csv'):
			book = xlwt.Workbook()		## Create New Workbook
			sheet1 = book.add_sheet("Sheet1")		## Create New sheet in workbook
			with open(loccheck, 'rt', encoding='utf8') as f:		## Read .csv file
				reader = csv.reader(f)
				for r, row in enumerate(reader):		## iterate through .csv file rows
					for c, col in enumerate(row):		## iterate through .csv file columns
						sheet1.write(r,c,col)		## write data to new file
			book.save(loccheck + '.xls')		## Save new File as .xls
			os.remove(loccheck)		## Remove old .csv file
			findfile2 = glob.glob('C:\\PB1\\3rdPartyFiles\\Promis*EWS*WIP*{0}*'.format(yestdaydate))		## Find updated extension file name
			loc = (findfile2[0])		## Assign the file name to the location path variable
			# os.remove(findfile[0])
		else:
			loc = loccheck		## Assign the file name to the location path variable
		
			
		print("Starting workbook parse")
		wb = xlrd.open_workbook(loc)		## Open the workbook at the file path location
		sheet = wb.sheet_by_index(0)		## Assign which sheet to read the data from.  0 is Sheet1, 1 is Sheet2, etc.

		sheet.cell_value(0,0)		## Assign what portion of the Cells to start in.  0,0 starts in A1 postion

		### Calculation for which Row to start on and creating a row count to ensure all rows are captured in the output ###
		count = 1
		xclrows = (sheet.nrows) - count
		xclstart = 1
		

		
		# print(xclrows)
		# print(sheet.row_values(xclstart))
		# input()

		### While loop to go through all rows in file and append to list which column/row data should be transfered to table in database custom table ###
		while count <= xclrows:
			results =(sheet.row_values(xclstart))
			verify = (results[0], results[1], results[2], results[3], results[4], str(results[6]).split('.')[0], str(results[7]).split('.')[0], results[8], results[9], str(results[10]).split('.')[0], results[11])
			promList.append(verify)	
			count = count + 1
			xclstart = xclstart + 1
		
	else:
		loc = 'NULL'
		
	### For loop to iterate through appended list and insert values to custom table ###
	if promList != None:
		for x in promList:
			fam, item, parentlot, lot, lottype, recvdate, recvqty, fabstage, status2, curqty, status = x
			nxpcheck = re.match("94[2-9].*",fabstage)
			condorcheck = re.match("WB0[6,8]M35M",item)
			engcheck = re.match(".*-ENG",item)
			mkcheck = re.match("7XY.*",lot)
			# print(mkcheck, lottype)
			if fabstage == '':  ## -- Check to see if Fabstage is blank and assign values to EWS instead of FAB -- ##
				query4 = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and LotNum = '{1}' and ItemCode in ('{2}','{3}')".format('FCHD', lot, fam, item))
				cursor.execute(query4)
				result2 = cursor.fetchone()
				if result2 != None:
					if fam.lower() == 'condor':
						if lottype != None:
							if lottype.lower() == 'p':
								query11 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES Set ItemCode = '{0}', Stage = 'EWS', UploadDate = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}', FabStage = '{5}', VendorStatus2 = '{6}', TargetDevice = '{7}', IsValid = 'Y', UpdateDate = '', UpdateReason = '' Where TransID = '{8}' and TransID <> 388".format(fam, uploaddate, yestdaydate,str(loc).split('\\')[3],status, fabstage, status2, item, result2[0]))
								cursor.execute(query11)
							elif lottype.lower() == 'e':
								query11 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES Set ItemCode = '{0}', Stage = 'EWS', UploadDate = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}', FabStage = '{5}', VendorStatus2 = '{6}', TargetDevice = '{7}', IsValid = 'N', UpdateDate = '{8}', UpdateReason = '{9}' Where TransID = '{10}' and TransID <> 388".format(fam, uploaddate, yestdaydate,str(loc).split('\\')[3],status, fabstage, status2, item, uploaddate,'Part Marked for Engineering.  Not Valid', result2[0]))
								cursor.execute(query11)
							else:
								query11 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES Set ItemCode = '{0}', Stage = 'EWS', UploadDate = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}', FabStage = '{5}', VendorStatus2 = '{6}', TargetDevice = '{7}', IsValid = 'Y', UpdateDate = '', UpdateReason = '' Where TransID = '{8}' and TransID <> 388".format(fam, uploaddate, yestdaydate,str(loc).split('\\')[3],status, fabstage, status2, item, result2[0]))
								cursor.execute(query11)
						else:
							query11 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES Set ItemCode = '{0}', Stage = 'EWS', UploadDate = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}', FabStage = '{5}', VendorStatus2 = '{6}', TargetDevice = '{7}', IsValid = 'Y', UpdateDate = '', UpdateReason = '' Where TransID = '{8}' and TransID <> 388".format(fam, uploaddate, yestdaydate,str(loc).split('\\')[3],status, fabstage, status2, item, result2[0]))
							cursor.execute(query11)
					elif fam.lower() == 'condor_dl':
						if lottype != None:
							if lottype.lower() == 'p':
								query12 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES Set ItemCode = '{0}', Stage = 'EWS', UploadDate = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}', FabStage = '{5}', VendorStatus2 = '{6}', TargetDevice = '{7}', IsValid = 'Y', UpdateDate = '', UpdateReason = '' Where TransID = '{8}'".format(fam, uploaddate, yestdaydate,str(loc).split('\\')[3],status, fabstage, status2, item, result2[0]))
								cursor.execute(query12)
							elif lottype.lower() == 'e':
								query12 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES Set ItemCode = '{0}', Stage = 'EWS', UploadDate = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}', FabStage = '{5}', VendorStatus2 = '{6}', TargetDevice = '{7}', IsValid = 'N', UpdateDate = '{8}', UpdateReason = '{9}' Where TransID = '{10}'".format(fam, uploaddate, yestdaydate,str(loc).split('\\')[3],status, fabstage, status2, item, uploaddate,'Part Marked for Engineering.  Not Valid.', result2[0]))
								cursor.execute(query12)
							else:
								query12 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES Set ItemCode = '{0}', Stage = 'EWS', UploadDate = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}', FabStage = '{5}', VendorStatus2 = '{6}', TargetDevice = '{7}', IsValid = 'Y', UpdateDate = '', UpdateReason = '' Where TransID = '{8}'".format(fam, uploaddate, yestdaydate,str(loc).split('\\')[3],status, fabstage, status2, item, result2[0]))
								cursor.execute(query12)
						else:
							query12 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES Set ItemCode = '{0}', Stage = 'EWS', UploadDate = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}', FabStage = '{5}', VendorStatus2 = '{6}', TargetDevice = '{7}', IsValid = 'Y', UpdateDate = '', UpdateReason = '' Where TransID = '{8}'".format(fam, uploaddate, yestdaydate,str(loc).split('\\')[3],status, fabstage, status2, item, result2[0]))
							cursor.execute(query12)
					else:
						if lottype != None:
							if lottype.lower() == 'p':
								if mkcheck != None:
									if lot == mkcheck.group(0):
										# print(mkcheck.group(0))
										query5 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES Set ItemCode = '{0}', Stage = 'EWS', UploadDate = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}', FabStage = '{5}', VendorStatus2 = '{6}', TargetDevice = '{7}', IsValid = 'N', UpdateDate = '{8}', UpdateReason = '{9}' Where TransID = '{10}'".format(fam, uploaddate, yestdaydate,str(loc).split('\\')[3],status, fabstage, status2, item, uploaddate, 'Part is McKinley marked for Engineering.  Not Valid.', result2[0]))
										cursor.execute(query5)
								else:
									query5 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES Set ItemCode = '{0}', Stage = 'EWS', UploadDate = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}', FabStage = '{5}', VendorStatus2 = '{6}', TargetDevice = '{7}', IsValid = 'Y', UpdateDate = '', UpdateReason = '' Where TransID = '{8}'".format(fam, uploaddate, yestdaydate,str(loc).split('\\')[3],status, fabstage, status2, item, result2[0]))
									cursor.execute(query5)
							elif lottype.lower() == 'e':
								query5 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES Set ItemCode = '{0}', Stage = 'EWS', UploadDate = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}', FabStage = '{5}', VendorStatus2 = '{6}', TargetDevice = '{7}', IsValid = 'N', UpdateDate = '{8}', UpdateReason = '{9}' Where TransID = '{10}'".format(fam, uploaddate, yestdaydate,str(loc).split('\\')[3],status, fabstage, status2, item, uploaddate, 'Part is marked for Engineering.  Not Valid.', result2[0]))
								cursor.execute(query5)
							else:
								if mkcheck != None:
									if lot == mkcheck.group(0):
										# print(mkcheck.group(0))
										query5 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES Set ItemCode = '{0}', Stage = 'EWS', UploadDate = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}', FabStage = '{5}', VendorStatus2 = '{6}', TargetDevice = '{7}', IsValid = 'N', UpdateDate = '{8}', UpdateReason = '{9}' Where TransID = '{10}'".format(fam, uploaddate, yestdaydate,str(loc).split('\\')[3],status, fabstage, status2, item, uploaddate, 'Part is McKinley marked for Engineering.  Not Valid.', result2[0]))
										cursor.execute(query5)
								else:
									query5 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES Set ItemCode = '{0}', Stage = 'EWS', UploadDate = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}', FabStage = '{5}', VendorStatus2 = '{6}', TargetDevice = '{7}', IsValid = 'Y', UpdateDate = '', UpdateReason = '' Where TransID = '{8}'".format(fam, uploaddate, yestdaydate,str(loc).split('\\')[3],status, fabstage, status2, item, result2[0]))
									cursor.execute(query5)
						else:
							if mkcheck != None:
								if lot == mkcheck.group(0):
									# print(mkcheck.group(0))
									query5 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES Set ItemCode = '{0}', Stage = 'EWS', UploadDate = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}', FabStage = '{5}', VendorStatus2 = '{6}', TargetDevice = '{7}', IsValid = 'N', UpdateDate = '{8}', UpdateReason = '{9}' Where TransID = '{10}'".format(fam, uploaddate, yestdaydate,str(loc).split('\\')[3],status, fabstage, status2, item, uploaddate, 'Part is McKinley marked for Engineering.  Not Valid.', result2[0]))
									cursor.execute(query5)
							else:
								query5 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES Set ItemCode = '{0}', Stage = 'EWS', UploadDate = '{1}', FileDate = '{2}', VendorFileName = '{3}', VendorStatus = '{4}', FabStage = '{5}', VendorStatus2 = '{6}', TargetDevice = '{7}', IsValid = 'Y', UpdateDate = '', UpdateReason = '' Where TransID = '{8}'".format(fam, uploaddate, yestdaydate,str(loc).split('\\')[3],status, fabstage, status2, item, result2[0]))
								cursor.execute(query5)
				else:
					if mkcheck != None:
						if lot == mkcheck.group(0):
							# print('line 1038')
							query6 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, UpdateDate, UpdateReason, ParentLot, FabStage, OrigUploadDate, CurrentQty,FABWIPProcessed, TargetDevice, VendorStatus2) values ('{0}','{1}','{2}',replace('{3}','-',''),'{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','N','{17}','{18}')".format(fam, lot, 'EWS', recvdate, recvqty, status, 'FCHD', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', uploaddate, 'Part is McKinley marked for Engineering.  Not valid.', parentlot, fabstage, yestdaydate, curqty, item, status2))
							cursor.execute(query6)
					else:
						# print('line 1042')
						query6 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, FabStage, OrigUploadDate, CurrentQty,FABWIPProcessed, TargetDevice, VendorStatus2) values ('{0}','{1}','{2}',replace('{3}','-',''),'{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','N', '{15}','{16}')".format(fam, lot, 'EWS', recvdate, recvqty, status, 'FCHD', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot, fabstage, yestdaydate, curqty, item, status2))
						cursor.execute(query6)
			else:
				query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}' and Stage = 'FAB'".format('FCHD', item,lot))
				cursor.execute(query)
				result = cursor.fetchone()
				if result != None:
					if condorcheck != None:
						if item == condorcheck.group(0):
							query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', RecvQty = '{1}', CurrentQty = '{2}', FileDate = '{3}', VendorFileName = '{4}', VendorStatus = '{5}', FabStage = '{6}', FABWIPProcessed = 'N', IsValid = 'N', UpdateDate = '{7}', UpdateReason = 'Non-Finished Condor in FAB.  Not EVS Inventory.' WHERE TransID = '{8}'".format(uploaddate, recvqty, curqty, yestdaydate, str(loc).split('\\')[3], status, fabstage,uploaddate, result[0])) 
							cursor.execute(query2)		## This query will update an existing record ##
					elif engcheck != None:
						if item == engcheck.group(0):
							query7 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', RecvQty = '{1}', CurrentQty = '{2}', FileDate = '{3}', VendorFileName = '{4}', VendorStatus = '{5}', FabStage = '{6}', FABWIPProcessed = 'N', IsValid = 'N', UpdateDate = '{7}', UpdateReason = 'Engineering Item.  Not Valid.' WHERE TransID = '{8}'".format(uploaddate, recvqty, curqty, yestdaydate, str(loc).split('\\')[3], status, fabstage,uploaddate, result[0])) 
							cursor.execute(query7)		## This query will update an existing record ##
					else:
						if lottype != None:
							if lottype.lower() == 'p':
								if mkcheck != None:
									if lot == mkcheck.group(0):
										# print(mkcheck.group(0))
										query7 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', RecvQty = '{1}', CurrentQty = '{2}', FileDate = '{3}', VendorFileName = '{4}', VendorStatus = '{5}', FabStage = '{6}', FABWIPProcessed = 'N', IsValid = 'N', UpdateDate = '{7}', UpdateReason = '{8}' WHERE TransID = '{9}'".format(uploaddate, recvqty, curqty, yestdaydate, str(loc).split('\\')[3], status, fabstage, uploaddate, 'Part is McKinley marked for Engineering.  Not Valid.', result[0])) 
										cursor.execute(query7)		## This query will update an existing record ##
								else:
									query7 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', RecvQty = '{1}', CurrentQty = '{2}', FileDate = '{3}', VendorFileName = '{4}', VendorStatus = '{5}', FabStage = '{6}', FABWIPProcessed = 'N' WHERE TransID = '{7}'".format(uploaddate, recvqty, curqty, yestdaydate, str(loc).split('\\')[3], status, fabstage, result[0])) 
									cursor.execute(query7)		## This query will update an existing record ##
							elif lottype.lower() == 'e':
								query7 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', RecvQty = '{1}', CurrentQty = '{2}', FileDate = '{3}', VendorFileName = '{4}', VendorStatus = '{5}', FabStage = '{6}', FABWIPProcessed = 'N', IsValid = 'N', UpdateDate='{7}', UpdateReason = '{8}' WHERE TransID = '{9}'".format(uploaddate, recvqty, curqty, yestdaydate, str(loc).split('\\')[3], status, fabstage, uploaddate, 'Part is marked for Egineering.  Not Valid.', result[0])) 
								cursor.execute(query7)		## This query will update an existing record ##
							else:
								if mkcheck != None:
									if lot == mkcheck.group(0):
										query7 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', RecvQty = '{1}', CurrentQty = '{2}', FileDate = '{3}', VendorFileName = '{4}', VendorStatus = '{5}', FabStage = '{6}', FABWIPProcessed = 'N', IsValid = 'N', UpdateDate = '{7}', UpdateReason = '{8}' WHERE TransID = '{9}'".format(uploaddate, recvqty, curqty, yestdaydate, str(loc).split('\\')[3], status, fabstage, uploaddate, 'Part is McKinley marked for Engineering.  Not Valid.', result[0])) 
										cursor.execute(query7)		## This query will update an existing record ##
								else:
									query7 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', RecvQty = '{1}', CurrentQty = '{2}', FileDate = '{3}', VendorFileName = '{4}', VendorStatus = '{5}', FabStage = '{6}', FABWIPProcessed = 'N' WHERE TransID = '{7}'".format(uploaddate, recvqty, curqty, yestdaydate, str(loc).split('\\')[3], status, fabstage, result[0])) 
									cursor.execute(query7)		## This query will update an existing record ##
						else:
							if mkcheck != None:
								if lot == mkcheck.group(0):
									# print('line 1077')
									query7 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', RecvQty = '{1}', CurrentQty = '{2}', FileDate = '{3}', VendorFileName = '{4}', VendorStatus = '{5}', FabStage = '{6}', FABWIPProcessed = 'N', IsValid = 'N', UpdateDate = '{7}', UpdateReason = '{8}' WHERE TransID = '{9}'".format(uploaddate, recvqty, curqty, yestdaydate, str(loc).split('\\')[3], status, fabstage, uploaddate, 'Part is McKinley marked for Engineering.  Not Valid.', result[0])) 
									cursor.execute(query7)		## This query will update an existing record ##
							else:
								# print('line 1081')
								query7 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', RecvQty = '{1}', CurrentQty = '{2}', FileDate = '{3}', VendorFileName = '{4}', VendorStatus = '{5}', FabStage = '{6}', FABWIPProcessed = 'N' WHERE TransID = '{7}'".format(uploaddate, recvqty, curqty, yestdaydate, str(loc).split('\\')[3], status, fabstage, result[0])) 
								cursor.execute(query7)		## This query will update an existing record ##
				else:
					if nxpcheck != None:
						if fabstage == nxpcheck:
							query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, UpdateDate, UpdateReason, ParentLot, FabStage, OrigUploadDate, CurrentQty,FABWIPProcessed) values ('{0}','{1}','{2}',replace('{3}','-',''),'{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','N')".format(item, lot, 'FAB', recvdate, recvqty, status, 'FCHD', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', uploaddate,'NXP Inventory and not Everspin Inventory.', parentlot, fabstage, yestdaydate, curqty))
							cursor.execute(query1)
					else:
						if condorcheck != None:
							if item == condorcheck.group(0):
								query3 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, FabStage, OrigUploadDate, CurrentQty,FABWIPProcessed, UpdateDate, UpdateReason) values ('{0}','{1}','{2}',replace('{3}','-',''),'{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','N','{15}','{16}')".format(item, lot, 'FAB', recvdate, recvqty, status, 'FCHD', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', parentlot, fabstage, yestdaydate, curqty, uploaddate, 'Non-Finished Condor in FAB.  Not EVS Inventory.'))
								cursor.execute(query3)
						elif engcheck != None:
							if item == engcheck.group(0):
								query10 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, FabStage, OrigUploadDate, CurrentQty,FABWIPProcessed, UpdateDate, UpdateReason) values ('{0}','{1}','{2}',replace('{3}','-',''),'{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','N','{15}','{16}')".format(item, lot, 'FAB', recvdate, recvqty, status, 'FCHD', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', parentlot, fabstage, yestdaydate, curqty, uploaddate, 'Engineering Item.  Not Valid.'))
								cursor.execute(query10)
						else:
							if lottype != None:
								if lottype.lower() == 'p':
									if mkcheck != None:
										if lot == mkcheck.group(0):
											query9 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, UpdateDate, UpdateReason, ParentLot, FabStage, OrigUploadDate, CurrentQty,FABWIPProcessed) values ('{0}','{1}','{2}',replace('{3}','-',''),'{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','N')".format(item, lot, 'FAB', recvdate, recvqty, status, 'FCHD', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', uploaddate,'Part is Mckinley marked for Egineering. Not Valid', parentlot, fabstage, yestdaydate, curqty))
											cursor.execute(query9)
									else:
										query9 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, FabStage, OrigUploadDate, CurrentQty,FABWIPProcessed) values ('{0}','{1}','{2}',replace('{3}','-',''),'{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','N')".format(item, lot, 'FAB', recvdate, recvqty, status, 'FCHD', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot, fabstage, yestdaydate, curqty))
										cursor.execute(query9)
								elif lottype.lower() =='e':
									query9 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, UpdateDate, UpdateReason, ParentLot, FabStage, OrigUploadDate, CurrentQty,FABWIPProcessed) values ('{0}','{1}','{2}',replace('{3}','-',''),'{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','N')".format(item, lot, 'FAB', recvdate, recvqty, status, 'FCHD', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', uploaddate,'Part is marked for Egineering. Not Valid', parentlot, fabstage, yestdaydate, curqty))
									cursor.execute(query9)
								else:
									if mkcheck != None:
										if lot == mkcheck.group(0):
											# print('line 1109')
											query9 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, UpdateDate, UpdateReason, ParentLot, FabStage, OrigUploadDate, CurrentQty,FABWIPProcessed) values ('{0}','{1}','{2}',replace('{3}','-',''),'{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','N')".format(item, lot, 'FAB', recvdate, recvqty, status, 'FCHD', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', uploaddate,'Part is Mckinley marked for Egineering. Not Valid', parentlot, fabstage, yestdaydate, curqty))
											cursor.execute(query9)
									else:
										# print('line 1113')
										query9 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, FabStage, OrigUploadDate, CurrentQty,FABWIPProcessed) values ('{0}','{1}','{2}',replace('{3}','-',''),'{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','N')".format(item, lot, 'FAB', recvdate, recvqty, status, 'FCHD', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot, fabstage, yestdaydate, curqty))
										cursor.execute(query9)
							else:
								if mkcheck != None:
									if lot == mkcheck.group(0):
										# print('line 1119')
										query9 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, UpdateDate, UpdateReason, ParentLot, FabStage, OrigUploadDate, CurrentQty,FABWIPProcessed) values ('{0}','{1}','{2}',replace('{3}','-',''),'{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','N')".format(item, lot, 'FAB', recvdate, recvqty, status, 'FCHD', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', uploaddate,'Part is Mckinley marked for Egineering. Not Valid', parentlot, fabstage, yestdaydate, curqty))
										cursor.execute(query9)
								else:
									# print('line 1123')
									query9 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, FabStage, OrigUploadDate, CurrentQty,FABWIPProcessed) values ('{0}','{1}','{2}',replace('{3}','-',''),'{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','N')".format(item, lot, 'FAB', recvdate, recvqty, status, 'FCHD', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot, fabstage, yestdaydate, curqty))
									cursor.execute(query9)
							
	## ----  Update Third Party Recevied File (TPRF) table in Validation database for 3P file received ---- ##
	filequery = ("Select isnull(FileName,LastRecvFile) 'FileName', isnull(FileDate,LastRecvDate) 'FileDate' FROM VALIDATION.dbo.TPRF WHERE AsOfDate = '{0}' and Vendor = 'FCHD'".format(yestdaydate))
	cursor.execute(filequery)
	filecheck = cursor.fetchone()
	fileCheckList=[]
	fileCheckList.append(filecheck)
	
	if filecheck == None:
		nullstoinsert = [('NULL', 'NULL')]
		checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'FCHD' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
		cursor.execute(checktoupdate)
		result = cursor.fetchone()
		if result != None:
			for x in nullstoinsert:
				filename, filedate = x
				tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'FCHD' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(tableupdatequery)
		else:
			for x in nullstoinsert:
				# print(x)
				filename, filedate = x
				fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('FCHD','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(fileupdatequery)
	else:
		if loc == 'NULL':
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'FCHD' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result3 = cursor.fetchone()
			if result3 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = 'NULL', FileDate = '{0}', LastRecvFile = '{1}', LastRecvDate = '{2}' Where Vendor = 'FCHD' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(yestdaydate, filename, filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('FCHD','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format('NULL',yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
		else:
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'FCHD' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result2 = cursor.fetchone()
			if result2 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'FCHD' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('FCHD','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
							
		
			
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	
	print("Finished Promise WIP File Upload....\n")
	
### UTC ASSEM File Processing function ###	
def utcAsyFile():

	print("Start UTC ASSEM File upload...")
	
	with pysftp.Connection(host=myHostname, port=22, username=myUsername, password=myPassword, cnopts=opts) as sftp:
		print("connection succesfully established ...")
		
		# switch to remote directory
		sftp.cwd('./Pioneer/ready/outgoing/UTC_ASSY_WIP')
		# print(sftp.pwd)
		filetoget = sftp.listdir()
		# print(filetoget)
		# sftp.cwd('./pioneer/ready/incoming')
		
		# obtain structure of the remote directory
		directory_structure = sftp.listdir_attr()
		# for attr in directory_structure:
			# print(attr)
		
		# print data
		for file in filetoget:
			print(file)
			filetoclean = re.match(r".*{0}.*".format(yestdaydate), file)
			sftp.get_d( sftp.pwd, 'C:\\PB1\\3rdPartyFiles\\', preserve_mtime=True)
			if filetoclean == None:
				sftp.remove(sftp.pwd+'/'+file)
			else:
				pass
		
	# input()
		
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=VALIDATION; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))		## Create Connection to SQL Server 
	cursor = sapsqlcon.cursor()		## Create Cursor Object
	
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*Everspin*AssyWIP*{0}*'.format(yestdaydate))		## Find the UTC Assembly File with the correct date

	if findfile != []:
		loc = (findfile[0])		## Assign the file name to the location path variable


		wb = xlrd.open_workbook(loc)		## Open the workbook at the file path location
		sheet = wb.sheet_by_index(0)		## Assign which sheet to read the data from.  0 is Sheet1, 1 is Sheet2, etc.

		sheet.cell_value(0,0)		## Assign what portion of the Cells to start in.  0,0 starts in A1 postion

		### Calculation for which Row to start on and creating a row count to ensure all rows are captured in the output ###
		count = 1
		xclrows = (sheet.nrows) - count
		xclstart = 1
		

		
		# print(xclrows)
		# print(sheet.row_values(xclstart))
		# input()

		### While loop to go through all rows in file and append to list which column/row data should be transfered to table in database custom table ###
		while count <= xclrows:
			results =(sheet.row_values(xclstart))
			verify = (results[0], results[2], results[4], str(results[7]).split('.')[0], str(results[8]).split('.')[0], str(results[11]).split('.')[0], results[12])
			utcASEMList.append(verify)	
			count = count + 1
			xclstart = xclstart + 1
			
	else:
		loc = 'NULL'
		
	### For loop to iterate through appended list and insert values to custom table ###
	if utcASEMList != None:
		for x in utcASEMList:
			status, recvdate, item, recvqty, curqty, ponum, lot = x
			query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}' and Stage = 'ASSEM'".format('UTC', item,lot))
			cursor.execute(query)
			result = cursor.fetchone()
			if result != None:
				query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorFileName = '{2}', RecvQty = '{3}', CurrentQty = '{4}', VendorStatus = '{5}' where TransID = '{6}'".format(uploaddate, yestdaydate, str(loc).split('\\')[3], recvqty, curqty, status, result[0]))
				cursor.execute(query2)
			else:
				query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, RecvDate, RecvQty, CurrentQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, OrigUploadDate,FABWIPProcessed) values ('{0}','{1}','{2}','{3}',right(replace('{4}','/',''),4)+left(replace('{5}','/',''),4),'{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', '{14}','N')".format(item, lot, ponum, 'ASSEM', recvdate, recvdate, recvqty, curqty, status, 'UTC', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', uploaddate))
				cursor.execute(query1)
				
	
	## ----  Update Third Party Recevied File (TPRF) table in Validation database for 3P file received ---- ##
	filequery = ("Select isnull(FileName,LastRecvFile) 'FileName', isnull(FileDate,LastRecvDate) 'FileDate' FROM VALIDATION.dbo.TPRF WHERE AsOfDate = '{0}' and Vendor = 'UTCASY'".format(yestdaydate))
	cursor.execute(filequery)
	filecheck = cursor.fetchone()
	fileCheckList=[]
	fileCheckList.append(filecheck)
	
	if filecheck == None:
		nullstoinsert = [('NULL', 'NULL')]
		checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'UTCASY' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
		cursor.execute(checktoupdate)
		result = cursor.fetchone()
		if result != None:
			for x in nullstoinsert:
				filename, filedate = x
				tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'UTCASY' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(tableupdatequery)
		else:
			for x in nullstoinsert:
				# print(x)
				filename, filedate = x
				fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('UTCASY','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(fileupdatequery)
	else:
		if loc == 'NULL':
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'UTCASY' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result3 = cursor.fetchone()
			if result3 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = 'NULL', FileDate = '{0}', LastRecvFile = '{1}', LastRecvDate = '{2}' Where Vendor = 'UTCASY' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(yestdaydate, filename, filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('UTCASY','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format('NULL',yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
		else:
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'UTCASY' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result2 = cursor.fetchone()
			if result2 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'UTCASY' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('UTCASY','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
		
			
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	
	print("Finished UTC ASSEM File Upload....\n")
	
### UTC File Processing Function ###	
def utcFile():

	print("Start UTC File upload...")
	
	with pysftp.Connection(host=myHostname, port=22, username=myUsername, password=myPassword, cnopts=opts) as sftp:
		print("connection succesfully established ...")
		
		# switch to remote directory
		sftp.cwd('./Pioneer/ready/outgoing/UTC_WIP')
		# print(sftp.pwd)
		filetoget = sftp.listdir()
		# print(filetoget)
		# sftp.cwd('./pioneer/ready/incoming')
		
		# obtain structure of the remote directory
		directory_structure = sftp.listdir_attr()
		# for attr in directory_structure:
			# print(attr)
		
		# print data
		for file in filetoget:
			print(file)
			filetoclean = re.match(r".*{0}.*".format(yestdaydate), file)
			# print(sftp.pwd)
			# input()
			sftp.get_d( sftp.pwd, 'C:\\PB1\\3rdPartyFiles\\', preserve_mtime=True)
			if filetoclean == None:
				sftp.remove(sftp.pwd+'/'+file)
			else:
				pass
		
	# input()
		
		
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=VALIDATION; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))		## Create Connection to SQL Server 
	cursor = sapsqlcon.cursor()		## Create Cursor Object
	
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*UTC_Everspin*WIP*Report*{0}*'.format(yestdaydate))		## Find the UTC WIP File with the correct date

	if findfile != []:
		loc = (findfile[0])		## Assign the file name to the location path variable


		wb = xlrd.open_workbook(loc)		## Open the workbook at the file path location
		sheet = wb.sheet_by_index(0)		## Assign which sheet to read the data from.  0 is Sheet1, 1 is Sheet2, etc.

		sheet.cell_value(0,0)		## Assign what portion of the Cells to start in.  0,0 starts in A1 postion

		### Calculation for which Row to start on and creating a row count to ensure all rows are captured in the output ###
		count = 1
		xclrows = (sheet.nrows) - count
		xclstart = 1
		

		
		# print(xclrows)
		# print(sheet.row_values(xclstart))
		# input()

		### While loop to go through all rows in file and append to list which column/row data should be transfered to table in database custom table ###
		while count <= xclrows:
			results =(sheet.row_values(xclstart))
			verify = (results[0], results[1], results[2], results[3], results[4],results[6],results[7],results[8], str(results[10]).split('.')[0], results[18], results[19])
			utcList.append(verify)	
			count = count + 1
			xclstart = xclstart + 1
	
	else:
		loc = 'NULL'

		
	### For loop to iterate through appended list and insert values to custom table ###
	if utcList != None:
		for x in utcList:
			lot, parentlot, waflot, opncode, status, targetdev, item, recvdate, recvqty, sclot, lotkind = x
			comp = re.match("[C,R,Z][6,A-Z][9,0,A-Z][0-8,A-Z]",opncode) ## -- All Completed and MRB Codes --##
			comp2 = re.match("[Z]\w+",opncode)
			scrap = re.match("C699",opncode)
			allother = re.match("[B,Q-Y][0-9]\w+",opncode)
			eng = re.match("C5\w+",opncode)
			bobcat = re.match("BOB\w+",item)
			
			# query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}' and Stage = 'FT'".format('UTC', item,lot))
			# cursor.execute(query)
			# result = cursor.fetchone()
			# if result != None:
				# query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', VendorFileName = '{3}', OpnCode = '{4}', RecvQty = '{5}' Where TransID = '{6}'".format(uploaddate, yestdaydate, status, str(loc).split('\\')[3], opncode, recvqty))
				# cursor.execute(query2)
			# else:
				# query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, OpnCode) values ('{0}','{1}','{2}','{3}',right(replace('{4}','/',''),4)+left(replace('{5}','/',''),4),'{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', '{14}')".format(item, targetdev, lot, 'FT', recvdate, recvdate, recvqty, status, 'UTC', uploaddate, datesearch, str(loc).split('\\')[3], 'Y', parentlot, opncode))
				# cursor.execute(query1)
			if comp == None:
				pass
			else:
				if opncode == comp.group(0):
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}' and Stage in ('FG','FT')".format('UTC', item,lot))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						#if bobcat != None:
						#	if item == bobcat.group(0):
						#		query3 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', VendorFileName = '{3}', OpnCode = '{4}', RecvQty = '{5}', IsValid = 'N', UpdateDate = '{6}', UpdateReason = 'Bobcat is Engineering Part.  Not valid at this time.', SourceLot = '{7}', WaferLot='{8}', Stage = 'FG' Where TransID = '{9}'".format(uploaddate, yestdaydate, status, str(loc).split('\\')[3], opncode, recvqty,uploaddate,sclot,waflot,result[0]))
						#		cursor.execute(query3)
						#else:
							if lotkind.lower() == 'p':
								query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', VendorFileName = '{3}', OpnCode = '{4}', RecvQty = '{5}', IsValid = 'Y', SourceLot = '{6}', WaferLot='{7}', UpdateDate = NULL, UpdateReason = NULL, Stage = 'FG' Where TransID = '{8}'".format(uploaddate, yestdaydate, status, str(loc).split('\\')[3], opncode, recvqty,sclot,waflot,result[0]))
								cursor.execute(query2)
							elif lotkind.lower()=='e':
								query3 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', VendorFileName = '{3}', OpnCode = '{4}', RecvQty = '{5}', IsValid = 'N', SourceLot = '{6}', WaferLot='{7}', UpdateDate = '{8}', UpdateReason = 'Marked as Engineering.  Not Valid for Report', Stage = 'FG' Where TransID = '{9}'".format(uploaddate, yestdaydate, status, str(loc).split('\\')[3], opncode, recvqty,sclot,waflot,uploaddate,result[0]))
								cursor.execute(query3)
								
					else:
						# if bobcat != None:
							# if item == bobcat.group(0):
								# query2 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, OpnCode, OrigUploadDate, FABWIPProcessed, SourceLot,WaferLot,UpdateDate, UpdateReason) values ('{0}','{1}','{2}','{3}',right(replace('{4}','/',''),4)+left(replace('{5}','/',''),4),'{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', '{14}','{15}','N', '{16}','{17}','{18}','{19}')".format(item, targetdev, lot, 'FG', recvdate, recvdate, recvqty, status, 'UTC', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', parentlot, opncode, uploaddate, sclot,waflot, uploaddate, 'Bobcat is Engineering Part.  Not valid at this time.'))
								# cursor.execute(query2)
						# else:
							if lotkind.lower() =='p':
								query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, OpnCode, OrigUploadDate, FABWIPProcessed, SourceLot,WaferLot) values ('{0}','{1}','{2}','{3}',right(replace('{4}','/',''),4)+left(replace('{5}','/',''),4),'{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', '{14}','{15}','N', '{16}','{17}')".format(item, targetdev, lot, 'FG', recvdate, recvdate, recvqty, status, 'UTC', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot, opncode, uploaddate, sclot,waflot))
								cursor.execute(query1)
							elif lotkind.lower() =='e':
								query5 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, UpdateDate, UpdateReason, ParentLot, OpnCode, OrigUploadDate, FABWIPProcessed, SourceLot,WaferLot) values ('{0}','{1}','{2}','{3}',right(replace('{4}','/',''),4)+left(replace('{5}','/',''),4),'{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','Part is Marked for Engineering.  Not valid.','{14}','{15}','{16}','N','{17}','{18}')".format(item, targetdev, lot, 'FG', recvdate, recvdate, recvqty, status, 'UTC', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', uploaddate, parentlot, opncode, uploaddate, sclot,waflot))
								cursor.execute(query5)
			if comp2 == None:
				pass
			else:
				if opncode == comp2.group(0):
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}' and Stage in ('FG','FT')".format('UTC', item,lot))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						# if bobcat != None:
							# if item == bobcat.group(0):
								# query3 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', VendorFileName = '{3}', OpnCode = '{4}', RecvQty = '{5}', IsValid = 'N', SourceLot = '{6}', WaferLot='{7}', UpdateDate = '{8}', UpdateReason = 'Bobcat is Engineering Part.  Not valid at this time.', Stage = 'FG' Where TransID = '{9}'".format(uploaddate, yestdaydate, status, str(loc).split('\\')[3], opncode, recvqty,sclot,waflot,uploaddate,result[0]))
								# cursor.execute(query3)
						# else:
							if lotkind.lower() == 'p':
								query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', VendorFileName = '{3}', OpnCode = '{4}', RecvQty = '{5}', IsValid = 'Y', SourceLot = '{6}', WaferLot='{7}', UpdateDate = NULL, UpdateReason = NULL, Stage = 'FG' Where TransID = '{8}'".format(uploaddate, yestdaydate, status, str(loc).split('\\')[3], opncode, recvqty,sclot,waflot,result[0]))
								cursor.execute(query2)
							elif lotkind.lower()=='e':
								query3 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', VendorFileName = '{3}', OpnCode = '{4}', RecvQty = '{5}', IsValid = 'N', SourceLot = '{6}', WaferLot='{7}', UpdateDate = '{8}', UpdateReason = 'Marked as Engineering.  Not Valid for Report', Stage = 'FG' Where TransID = '{9}'".format(uploaddate, yestdaydate, status, str(loc).split('\\')[3], opncode, recvqty,sclot,waflot,uploaddate,result[0]))
								cursor.execute(query3)
					else:
						# if bobcat != None:
							# if item == bobcat.group(0):
								# query2 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, OpnCode, OrigUploadDate, FABWIPProcessed, SourceLot,WaferLot,UpdateDate, UpdateReason) values ('{0}','{1}','{2}','{3}',right(replace('{4}','/',''),4)+left(replace('{5}','/',''),4),'{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', '{14}','{15}','N', '{16}','{17}','{18}','{19}')".format(item, targetdev, lot, 'FG', recvdate, recvdate, recvqty, status, 'UTC', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', parentlot, opncode, uploaddate, sclot,waflot,uploaddate,'Bobcat is Engineering Part. Not valid at this time.'))
								# cursor.execute(query2)
						# else:
							if lotkind.lower() =='p':
								query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, OpnCode, OrigUploadDate, FABWIPProcessed, SourceLot,WaferLot) values ('{0}','{1}','{2}','{3}',right(replace('{4}','/',''),4)+left(replace('{5}','/',''),4),'{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', '{14}','{15}','N', '{16}','{17}')".format(item, targetdev, lot, 'FG', recvdate, recvdate, recvqty, status, 'UTC', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot, opncode, uploaddate, sclot,waflot))
								cursor.execute(query1)
							elif lotkind.lower() =='e':
								query5 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, UpdateDate, UpdateReason, ParentLot, OpnCode, OrigUploadDate, FABWIPProcessed, SourceLot,WaferLot) values ('{0}','{1}','{2}','{3}',right(replace('{4}','/',''),4)+left(replace('{5}','/',''),4),'{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','Part is Marked for Engineering.  Not valid.', '{14}','{15}', '{16}', 'N','{17}','{18}')".format(item, targetdev, lot, 'FG', recvdate, recvdate, recvqty, status, 'UTC', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', uploaddate, parentlot, opncode, uploaddate, sclot,waflot))
								# print(query5)
								cursor.execute(query5)
			if scrap == None:
				pass
			else:
				if opncode == scrap.group(0):
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}' and Stage in ('FG','FT')".format('UTC', item,lot))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', VendorFileName = '{3}', OpnCode = '{4}', RecvQty = '{5}', IsValid = 'N', UpdateDate = '{6}', UpdateReason = 'Part is Scrap. Record is not valid', SourceLot = '{7}', WaferLot='{8}', Stage = 'FG' Where TransID = '{9}'".format(uploaddate, yestdaydate, status, str(loc).split('\\')[3], opncode, recvqty, uploaddate, sclot,waflot, result[0]))
						cursor.execute(query2)
					else:
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, OpnCode, UpdateDate, UpdateReason, OrigUploadDate,FABWIPProcessed, SourceLot, WaferLot) values ('{0}','{1}','{2}','{3}',right(replace('{4}','/',''),4)+left(replace('{5}','/',''),4),'{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', '{14}', '{15}','{16}','{17}','N','{18}','{19}')".format(item, targetdev, lot, 'FG', recvdate, recvdate, recvqty, status, 'UTC', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', parentlot, opncode, uploaddate, 'Part is Scrap. Record is not valid', uploaddate, sclot,waflot))
						cursor.execute(query1)			
			if allother == None:
				pass
			else:
				if opncode == allother.group(0):
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}' and Stage = 'FT'".format('UTC', item,lot))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						# if bobcat != None:
							# if item == bobcat.group(0):
								# query3 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', VendorFileName = '{3}', OpnCode = '{4}', RecvQty = '{5}', IsValid = 'N', UpdateDate = '{6}', UpdateReason = 'Bobcat is Engineering Part.  Not valid at this time.', SourceLot = '{7}', WaferLot= '{8}' Where TransID = '{9}'".format(uploaddate, yestdaydate, status, str(loc).split('\\')[3], opncode, recvqty, uploaddate, sclot, waflot, result[0]))
								# cursor.execute(query3)
						# else:
							if lotkind.lower() == 'p':
								query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', VendorFileName = '{3}', OpnCode = '{4}', RecvQty = '{5}', IsValid = 'Y', SourceLot = '{6}', WaferLot='{7}', UpdateDate = NULL, UpdateReason = NULL, Stage = 'FT' Where TransID = '{8}'".format(uploaddate, yestdaydate, status, str(loc).split('\\')[3], opncode, recvqty,sclot,waflot,result[0]))
								cursor.execute(query2)
							elif lotkind.lower()=='e':
								query3 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', VendorFileName = '{3}', OpnCode = '{4}', RecvQty = '{5}', IsValid = 'N', SourceLot = '{6}', WaferLot='{7}', UpdateDate = '{8}', UpdateReason = 'Marked as Engineering.  Not Valid for Report', Stage = 'FT' Where TransID = '{9}'".format(uploaddate, yestdaydate, status, str(loc).split('\\')[3], opncode, recvqty,sclot,waflot,uploaddate,result[0]))
								cursor.execute(query3)

					else:
						# if bobcat != None:
							# if item == bobcat.group(0):
								# query2 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, OpnCode, UpdateDate, UpdateReason, OrigUploadDate,FABWIPProcessed, SourceLot,WaferLot) values ('{0}','{1}','{2}','{3}',right(replace('{4}','/',''),4)+left(replace('{5}','/',''),4),'{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', '{14}', '{15}','{16}','{17}','N','{18}','{19}')".format(item, targetdev, lot, 'FT', recvdate, recvdate, recvqty, status, 'UTC', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', parentlot, opncode, uploaddate, 'Bobcat is Engineering Part.  Not valid at this time.', uploaddate, sclot,waflot))
								# cursor.execute(query2)
						# else:
							if lotkind.lower() == 'p':
								query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, OpnCode, UpdateDate, UpdateReason, OrigUploadDate,FABWIPProcessed, SourceLot,WaferLot) values ('{0}','{1}','{2}','{3}',right(replace('{4}','/',''),4)+left(replace('{5}','/',''),4),'{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', '{14}', '{15}','{16}','{17}','N','{18}','{19}')".format(item, targetdev, lot, 'FT', recvdate, recvdate, recvqty, status, 'UTC', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot, opncode, 'NULL', 'NULL', uploaddate, sclot,waflot))
								cursor.execute(query1)
							elif lotkind.lower() == 'e':
								query5 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, OpnCode, UpdateDate, UpdateReason, OrigUploadDate,FABWIPProcessed, SourceLot,WaferLot) values ('{0}','{1}','{2}','{3}',right(replace('{4}','/',''),4)+left(replace('{5}','/',''),4),'{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', '{14}', '{15}','{16}','{17}','N','{18}','{19}')".format(item, targetdev, lot, 'FT', recvdate, recvdate, recvqty, status, 'UTC', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', parentlot, opncode, uploaddate, 'Marked as Engineering.  Not valid.', uploaddate, sclot,waflot))
								cursor.execute(query5)
			if eng == None:
				pass
			else:	
				if opncode == eng.group(0):
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}' and Stage = 'FT'".format('UTC', item,lot))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', VendorFileName = '{3}', OpnCode = '{4}', RecvQty = '{5}', IsValid = 'N', UpdateDate = '{6}', UpdateReason = 'Part is assigned to Engineering OpnCode', SourceLot ='{7}', WaferLot='{8}' Where TransID = '{9}'".format(uploaddate, yestdaydate, status, str(loc).split('\\')[3], opncode, recvqty,uploaddate, sclot, waflot, result[0]))
						cursor.execute(query2)
					else:
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, TargetDevice, LotNum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid, ParentLot, OpnCode, UpdateDate, UpdateReason, OrigUploadDate,FABWIPProcessed,SourceLot,WaferLot) values ('{0}','{1}','{2}','{3}',right(replace('{4}','/',''),4)+left(replace('{5}','/',''),4),'{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}', '{14}', '{15}','{16}','{17}','N','{18}','{19}')".format(item, targetdev, lot, 'FT', recvdate, recvdate, recvqty, status, 'UTC', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', parentlot, opncode, uploaddate, 'Part is assigned to Engineering OpnCode', uploaddate, sclot,waflot))
						cursor.execute(query1)
				else:
					pass
					
					
	## ----  Update Third Party Recevied File (TPRF) table in Validation database for 3P file received ---- ##
	filequery = ("Select isnull(FileName,LastRecvFile) 'FileName', isnull(FileDate,LastRecvDate) 'FileDate' FROM VALIDATION.dbo.TPRF WHERE AsOfDate = '{0}' and Vendor = 'UTC'".format(yestdaydate))
	cursor.execute(filequery)
	filecheck = cursor.fetchone()
	fileCheckList=[]
	fileCheckList.append(filecheck)
	
	if filecheck == None:
		nullstoinsert = [('NULL', 'NULL')]
		checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'UTC' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
		cursor.execute(checktoupdate)
		result = cursor.fetchone()
		if result != None:
			for x in nullstoinsert:
				filename, filedate = x
				tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'UTC' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(tableupdatequery)
		else:
			for x in nullstoinsert:
				# print(x)
				filename, filedate = x
				fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('UTC','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(fileupdatequery)
	else:
		if loc == 'NULL':
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'UTC' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result3 = cursor.fetchone()
			if result3 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = 'NULL', FileDate = '{0}', LastRecvFile = '{1}', LastRecvDate = '{2}' Where Vendor = 'UTC' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(yestdaydate, filename, filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('UTC','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format('NULL',yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
		else:
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'UTC' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result2 = cursor.fetchone()
			if result2 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'UTC' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('UTC','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
					
		
			
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	
	print("Finished UTC File Upload....\n")	

### UDG ASSEM File Processing function ###
def udgAsyFile():

	print("Start UDG ASSEM File upload...")
	
	with pysftp.Connection(host=myHostname, port=22, username=myUsername, password=myPassword, cnopts=opts) as sftp:
		print("connection succesfully established ...")
		
		# switch to remote directory
		sftp.cwd('./Pioneer/ready/outgoing/UDG_WIP')
		# print(sftp.pwd)
		filetoget = sftp.listdir()
		# print(filetoget)
		# sftp.cwd('./pioneer/ready/incoming')
		
		# obtain structure of the remote directory
		directory_structure = sftp.listdir_attr()
		# for attr in directory_structure:
			# print(attr)
		
		# print data
		for file in filetoget:
			print(file)
			filetoclean = re.match(r".*{0}.*".format(yestdaydate), file)
			sftp.get_d( sftp.pwd, 'C:\\PB1\\3rdPartyFiles\\', preserve_mtime=True)
			if filetoclean == None:
				sftp.remove(sftp.pwd+'/'+file)
			else:
				pass
	
	# input()
		
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=VALIDATION; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))		## Create Connection to SQL Server 
	cursor = sapsqlcon.cursor()		## Create Cursor Object
	
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*DAILY_WIP_DG*{0}*'.format(yestdaydate))		## Find the UTC Assembly File with the correct date
	
	if findfile != []:
		loc = (findfile[0])		## Assign the file name to the location path variable


		wb = xlrd.open_workbook(loc)		## Open the workbook at the file path location
		sheet = wb.sheet_by_name('Detail')		## Assign which sheet to read the data from.  0 is Sheet1, 1 is Sheet2, etc.
		
		sheet.cell_value(0,0)		## Assign what portion of the Cells to start in.  0,0 starts in A1 postion

		### Calculation for which Row to start on and creating a row count to ensure all rows are captured in the output ###
		count = 4
		xclrows = (sheet.nrows) - count
		xclstart = 6
		

		
		# print(xclrows)
		# print(sheet.row_values(xclstart))
		# input()

		### While loop to go through all rows in file and append to list which column/row data should be transfered to table in database custom table ###
		while count <= xclrows:
			results =(sheet.row_values(xclstart))
			verify = (results[0], results[3], results[4], results[5], str(results[6]).split('.')[0], results[8], results[11], str(results[13]).split('.')[0], results[14], str(results[17]).split('.')[0], str(int(0 if results[18]=='' else results[18])+int(0 if results[19]=='' else results[19])+int(0 if results[20]=='' else results[20])+int(0 if results[21]=='' else results[21])+int(0 if results[22]=='' else results[22])+int(0 if results[23]=='' else results[23])+int(0 if results[24]=='' else results[24])+int(0 if results[25]=='' else results[25])+int(0 if results[26]=='' else results[26])+int(0 if results[27]=='' else results[27])+int(0 if results[28]=='' else results[28])+int(0 if results[29]=='' else results[29])).split('.')[0],str(results[32]).split('.')[0])
			udgList.append(verify)	
			count = count + 1
			xclstart = xclstart + 1
		
		# for x in udgList:
			# print(x)
		# input()
	else:
		loc = 'NULL'
		
	### For loop to iterate through appended list and insert values to custom table ###
	if udgList != None:
		for x in udgList:
			id, item, compitem, parentlot, ponum, lot, status, dieqty, recvdate, startqty, curqty, shipqty = x
			if id.lower() == 'subtotal':
				pass
			elif id.lower() == 'grand total':
				pass
			elif item == '':
				pass
			else:
				if lot == '':
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and ParentLot = '{2}' and Stage = 'ASSEM'".format('UDG', compitem, parentlot))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						query3 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', RecvQty = '{3}', VendorFileName = '{4}' Where TransID = '{5}'".format(uploaddate, yestdaydate, status, dieqty, str(loc).split('\\')[3], result[0]))
						cursor.execute(query3)
					else:
						if status.lower() == 'wip':
							if shipqty == '':
								query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (TargetDevice, LotNum, PONum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid,ParentLot, OrigUploadDate,FABWIPProcessed, ItemCode) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','N','{14}')".format(item, lot, ponum, 'ASSEM', recvdate, startqty, status, 'UDG', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot, uploaddate, compitem))
								cursor.execute(query1)
							else:
								query4 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (TargetDevice, LotNum, PONum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid,ParentLot, OrigUploadDate,FABWIPProcessed, ItemCode, UpdateDate, UpdateReason) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','N','{14}',{15}',{16}')".format(item, lot, ponum, 'ASSEM', recvdate, startqty, status, 'UDG', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', parentlot, uploaddate, compitem, uploaddate, 'Part has shipped.  Not valid on this report.'))
								cursor.execute(query4)
						else:
							query2 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (TargetDevice, LotNum, PONum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid,ParentLot,OrigUploadDate,FABWIPProcessed, ItemCode) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','N','{14}')".format(item, parentlot, ponum, 'ASSEM', recvdate, dieqty, status, 'UDG', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot, uploaddate,compitem))
							cursor.execute(query2)
				else:
					query13 = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}' and Stage = 'ASSEM'".format('UDG', item, lot))
					cursor.execute(query13)
					result = cursor.fetchone()
					if result != None:
						if shipqty == '':
							if curqty == '':
								if startqty == '':
									query12 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', RecvQty = '{3}', VendorFileName = '{4}' Where TransID = '{5}'".format(uploaddate, yestdaydate, status, dieqty, str(loc).split('\\')[3], result[0]))
									cursor.execute(query12)
								else:
									query3 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', RecvQty = '{3}', VendorFileName = '{4}' Where TransID = '{5}'".format(uploaddate, yestdaydate, status, startqty, str(loc).split('\\')[3], result[0]))
									cursor.execute(query3)
							else:
								query5 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', RecvQty = '{3}', VendorFileName = '{4}' Where TransID = '{5}'".format(uploaddate, yestdaydate, status, curqty, str(loc).split('\\')[3], result[0]))
								cursor.execute(query5)
						else:
							query6 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', RecvQty = '{3}', VendorFileName = '{4}', IsValid = 'N', UpdateDate = '{5}', UpdateReason = 'Part has Shipped.  Not valid on this Report.' Where TransID = '{6}'".format(uploaddate, yestdaydate, status, startqty, str(loc).split('\\')[3], uploaddate, result[0]))
							cursor.execute(query6)
					else:
						if status.lower() == 'wip':
							if shipqty == '':
								if curqty == '':
									if startqty == '':
										query11 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (TargetDevice, LotNum, PONum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid,ParentLot,OrigUploadDate,FABWIPProcessed, ItemCode) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','N','{14}')".format(item, lot, ponum, 'ASSEM', recvdate, dieqty, status, 'UDG', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot, uploaddate, compitem))
										cursor.execute(query11)
									else:
										query7 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (TargetDevice, LotNum, PONum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid,ParentLot,OrigUploadDate,FABWIPProcessed, ItemCode) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','N','{14}')".format(item, lot, ponum, 'ASSEM', recvdate, startqty, status, 'UDG', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot, uploaddate, compitem))
										cursor.execute(query7)
								else:
									query8 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (TargetDevice, LotNum, PONum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid,ParentLot,OrigUploadDate,FABWIPProcessed, ItemCode) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','N','{14}')".format(item, lot, ponum, 'ASSEM', recvdate, curqty, status, 'UDG', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot, uploaddate, compitem))
									cursor.execute(query8)
							else:
								query9 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (TargetDevice, LotNum, PONum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid,ParentLot,OrigUploadDate,FABWIPProcessed, ItemCode, UpdateDate, UpdateReason) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','N','{14}','{15}','{16}')".format(item, lot, ponum, 'ASSEM', recvdate, dieqty, status, 'UDG', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', parentlot, uploaddate,compitem, uploaddate, 'Part has shipped.  Not valid on this Report.'))
								cursor.execute(query9)
						else:
							query10 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (TargetDevice, LotNum, PONum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid,ParentLot,OrigUploadDate,FABWIPProcessed,ItemCode) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','N','{14}')".format(item, parentlot, ponum, 'ASSEM', recvdate, dieqty, status, 'UDG', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot,uploaddate,compitem))
							cursor.execute(query10)
							
							
	## ----  Update Third Party Recevied File (TPRF) table in Validation database for 3P file received ---- ##
	filequery = ("Select isnull(FileName,LastRecvFile) 'FileName', isnull(FileDate,LastRecvDate) 'FileDate' FROM VALIDATION.dbo.TPRF WHERE AsOfDate = '{0}' and Vendor = 'UDG'".format(yestdaydate))
	cursor.execute(filequery)
	filecheck = cursor.fetchone()
	fileCheckList=[]
	fileCheckList.append(filecheck)
	
	if filecheck == None:
		nullstoinsert = [('NULL', 'NULL')]
		checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'UDG' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
		cursor.execute(checktoupdate)
		result = cursor.fetchone()
		if result != None:
			for x in nullstoinsert:
				filename, filedate = x
				tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'UDG' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(tableupdatequery)
		else:
			for x in nullstoinsert:
				# print(x)
				filename, filedate = x
				fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('UDG','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(fileupdatequery)
	else:
		if loc == 'NULL':
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'UDG' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result3 = cursor.fetchone()
			if result3 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = 'NULL', FileDate = '{0}', LastRecvFile = '{1}', LastRecvDate = '{2}' Where Vendor = 'UDG' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(yestdaydate, filename, filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('UDG','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format('NULL',yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
		else:
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'UDG' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result2 = cursor.fetchone()
			if result2 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'UDG' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('UDG','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
	
			
			
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	
	print("Finished UDG ASSEM File Upload....\n")
	
### CHM VE INV File Processing function ###
def chmVeInvFile():

	print("Start CHM INV File upload...")
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=VALIDATION; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))		## Create Connection to SQL Server 
	cursor = sapsqlcon.cursor()		## Create Cursor Object
	
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*VE_INV_*{0}*'.format(yestdaydate))		## Find the CHM VE WIP File with the correct date

	if findfile != []:
		loc = (findfile[0])		## Assign the file name to the location path variable


		wb = xlrd.open_workbook(loc)		## Open the workbook at the file path location
		sheet = wb.sheet_by_index(0)		## Assign which sheet to read the data from.  0 is Sheet1, 1 is Sheet2, etc.

		sheet.cell_value(0,0)		## Assign what portion of the Cells to start in.  0,0 starts in A1 postion

		### Calculation for which Row to start on and creating a row count to ensure all rows are captured in the output ###
		count = 1
		xclrows = (sheet.nrows) - count
		# print(xclrows)
		xclstart = 1

		### While loop to go through all rows in file and append to list which column/row data should be transfered to table in database custom table ###
		while count <= xclrows:
			results =(sheet.row_values(xclstart))
			verify = (results[1], results[2], results[3], results[4], str(results[5]).split('.')[0], str(results[6]).split('.')[0], str(results[7]).split('.')[0], str(results[9]).split('.')[0])
			chmEWSList2.append(verify)	
			# print(count)
			count = count + 1
			# print(xclstart)
			xclstart = xclstart + 1

			
		# print(asemList)
		# input()
		
		# print(str(loc).split('\\')[3])
		# input()
		
	### For loop to iterate through appended list and insert values to custom table ###
	if chmEWSList2 != None:
		for x in chmEWSList2:
			item, lot, status, wafid, recvqty, curqty, ponum, recvdate = x
			if item == 'Product':
				pass
			else:
				if status.lower() != 'scrap':
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode = '{1}' and LotNum = '{2}' and FabStage = '{3}' and Stage = 'EWS'".format('CHM-INV', item, lot, wafid))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', RecvQty = '{3}', CurrentQty = '{4}', VendorFileName = '{5}' where TransID = '{6}'".format(uploaddate, yestdaydate, status, recvqty, curqty, str(loc).split('\\')[3], result[0]))
						cursor.execute(query2)
					else:
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, VendorStatus, RecvDate, RecvQty, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, FabStage, OrigUploadDate,FABWIPProcessed) values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','N')".format(item, lot, ponum, 'EWS', status, recvdate, recvqty, curqty, 'CHM-INV', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y',wafid, uploaddate))
						cursor.execute(query1)
				elif status.lower() == 'eng':
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode = '{1}' and LotNum = '{2}' and FabStage = '{3}' and Stage = 'EWS'".format('CHM-INV', item, lot, wafid))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', RecvQty = '{3}', CurrentQty = '{4}', VendorFileName = '{5}', IsValid = 'N', UpdateDate = '{6}', UpdateReason = 'Status is ENG. Record is not Valid' where TransID = '{7}'".format(uploaddate, yestdaydate, status, recvqty, curqty, str(loc).split('\\')[3], uploaddate, result[0]))
						cursor.execute(query2)
					else:
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, VendorStatus, RecvDate, RecvQty, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid,UpdateDate,UpdateReason, FabStage, OrigUploadDate,FABWIPProcessed) values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','N')".format(item, lot, ponum, 'EWS', status, recvdate, recvqty, curqty, 'CHM-INV', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', UploadDate, 'Status is ENG. Record is not Valid',wafid, uploaddate))
						cursor.execute(query1)
				else:
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode = '{1}' and LotNum = '{2}' and FabStage = '{3}' and Stage = 'EWS'".format('CHM-INV', item, lot, wafid))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', RecvQty = '{3}', CurrentQty = '{4}', VendorFileName = '{5}', IsValid = 'N', UpdateDate = '{6}', UpdateReason = 'Status is Scrap. Record is not Valid' where TransID = '{7}'".format(uploaddate, yestdaydate, status, recvqty, curqty, str(loc).split('\\')[3], uploaddate, result[0]))
						cursor.execute(query2)
					else:
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, VendorStatus, RecvDate, RecvQty, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid,UpdateDate,UpdateReason, FabStage, OrigUploadDate,FABWIPProcessed) values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','N')".format(item, lot, ponum, 'EWS', status, recvdate, recvqty, curqty, 'CHM-INV', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', UploadDate, 'Status is Scrap. Record is not Valid',wafid, uploaddate))
						cursor.execute(query1)
						
						
	## ----  Update Third Party Recevied File (TPRF) table in Validation database for 3P file received ---- ##
	filequery = ("Select isnull(FileName,LastRecvFile) 'FileName', isnull(FileDate,LastRecvDate) 'FileDate' FROM VALIDATION.dbo.TPRF WHERE AsOfDate = '{0}' and Vendor = 'CHMINV'".format(yestdaydate))
	cursor.execute(filequery)
	filecheck = cursor.fetchone()
	fileCheckList=[]
	fileCheckList.append(filecheck)
	
	if filecheck == None:
		nullstoinsert = [('NULL', 'NULL')]
		checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'CHMINV' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
		cursor.execute(checktoupdate)
		result = cursor.fetchone()
		if result != None:
			for x in nullstoinsert:
				filename, filedate = x
				tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'CHMINV' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(tableupdatequery)
		else:
			for x in nullstoinsert:
				# print(x)
				filename, filedate = x
				fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('CHMINV','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(fileupdatequery)
	else:
		checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'CHMINV' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
		cursor.execute(checktoupdate)
		result2 = cursor.fetchone()
		if result2 != None:
			for x in fileCheckList:
				filename, filedate = x
				tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'CHMINV' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(tableupdatequery)
		else:
			for x in fileCheckList:
				filename, filedate = x
				fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('CHMINV','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(fileupdatequery)
						
			
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	
	print("Finished CHM INV File Upload....\n")	
	
### GTC File Processing function ###
def gtcFile():

	print("Start GTC WIP File upload...")
	
	with pysftp.Connection(host=myHostname, port=22, username=myUsername, password=myPassword, cnopts=opts) as sftp:
		print("connection succesfully established ...")
		
		# switch to remote directory
		sftp.cwd('./Pioneer/ready/outgoing/GTC_WIP')
		# print(sftp.pwd)
		filetoget = sftp.listdir()
		# print(filetoget)
		# sftp.cwd('./pioneer/ready/incoming')
		
		# obtain structure of the remote directory
		directory_structure = sftp.listdir_attr()
		# for attr in directory_structure:
			# print(attr)
		
		# print data
		for file in filetoget:
			print(file)
			filetoclean = re.match(r".*{0}.*".format(yestdaydate), file)
			sftp.get_d( sftp.pwd, 'C:\\PB1\\3rdPartyFiles\\', preserve_mtime=True)
			if filetoclean == None:
				sftp.remove(sftp.pwd+'/'+file)
			else:
				pass
			
			
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=VALIDATION; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))		## Create Connection to SQL Server 
	cursor = sapsqlcon.cursor()		## Create Cursor Object
	
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\Everspin*WIP*Report*{0}*'.format(yestdaydate))		## Find the GTC WIP File with the correct date

	if findfile != []:
		loc = (findfile[0])		## Assign the file name to the location path variable


		wb = xlrd.open_workbook(loc)		## Open the workbook at the file path location
		sheet = wb.sheet_by_index(0)		## Assign which sheet to read the data from.  0 is Sheet1, 1 is Sheet2, etc.

		sheet.cell_value(0,0)		## Assign what portion of the Cells to start in.  0,0 starts in A1 postion

		### Calculation for which Row to start on and creating a row count to ensure all rows are captured in the output ###
		count = 2
		xclrows = (sheet.nrows) - count
		#print(xclrows)
		xclstart = 3

		### While loop to go through all rows in file and append to list which column/row data should be transfered to table in database custom table ###
		while count <= xclrows:
			results =(sheet.row_values(xclstart))
			verify = (results[0], results[1], results[2], str(results[3]).split('.')[0], results[5], str(results[7]).split('.')[0], str(results[13]).split('.')[0])
			gtcList.append(verify)	
			# print(count)
			count = count + 1
			# print(xclstart)
			xclstart = xclstart + 1

	# print(gtcList)
	# input()
	else:
		loc = 'NULL'
		
	### For loop to iterate through appended list and insert values to custom table ###
	if gtcList != None:
		for x in gtcList:
			plot, item, status, recvdate, lot, recvqty, curqty = x
			query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode = '{1}' and LotNum = '{2}' and Stage = 'EWS'".format('GTC', item, lot))
			cursor.execute(query)
			result = cursor.fetchone()
			if plot == 'WaferLot':
				pass
			else:
				if result != None:
					query2 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', RecvQty = '{3}', CurrentQty = '{4}', VendorFileName = '{5}' where TransID = '{6}'".format(uploaddate, yestdaydate, status, recvqty, curqty, str(loc).split('\\')[3], result[0]))
					cursor.execute(query2)
				else:
					query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, Stage, VendorStatus, RecvDate, RecvQty, CurrentQty, Vendor, UploadDate, FileDate, VendorFileName, IsValid, OrigUploadDate,FABWIPProcessed) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','N')".format(item, lot, 'EWS', status, recvdate, recvqty, curqty, 'GTC', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', uploaddate))
					cursor.execute(query1)
					
					
	## ----  Update Third Party Recevied File (TPRF) table in Validation database for 3P file received ---- ##
	filequery = ("Select isnull(FileName,LastRecvFile) 'FileName', isnull(FileDate,LastRecvDate) 'FileDate' FROM VALIDATION.dbo.TPRF WHERE AsOfDate = '{0}' and Vendor = 'GTC'".format(yestdaydate))
	cursor.execute(filequery)
	filecheck = cursor.fetchone()
	fileCheckList=[]
	fileCheckList.append(filecheck)
	
	if filecheck == None:
		nullstoinsert = [('NULL', 'NULL')]
		checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'GTC' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
		cursor.execute(checktoupdate)
		result = cursor.fetchone()
		if result != None:
			for x in nullstoinsert:
				filename, filedate = x
				tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'GTC' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(tableupdatequery)
		else:
			for x in nullstoinsert:
				# print(x)
				filename, filedate = x
				fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('GTC','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
				cursor.execute(fileupdatequery)
	else:
		if loc == 'NULL':
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'GTC' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result3 = cursor.fetchone()
			if result3 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = 'NULL', FileDate = '{0}', LastRecvFile = '{1}', LastRecvDate = '{2}' Where Vendor = 'GTC' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(yestdaydate, filename, filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('GTC','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format('NULL',yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
		else:
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'GTC' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result2 = cursor.fetchone()
			if result2 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'GTC' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('GTC','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
					
					
					
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	
	print("Finished GTC WIP File Upload....\n")	
	
### UTL ASSEM File Processing function ###
def utlAsyFile():

	print("Start UTL ASSEM File upload...")
	
	with pysftp.Connection(host=myHostname, port=22, username=myUsername, password=myPassword, cnopts=opts) as sftp:
		print("connection succesfully established ...")
		
		# switch to remote directory
		sftp.cwd('./Pioneer/ready/outgoing/UTL_WIP')
		# print(sftp.pwd)
		filetoget = sftp.listdir()
		# print(filetoget)
		# sftp.cwd('./pioneer/ready/incoming')
		
		# obtain structure of the remote directory
		directory_structure = sftp.listdir_attr()
		# for attr in directory_structure:
			# print(attr)
		
		# print data
		for file in filetoget:
			print(file)
			filetoclean = re.match(r".*{0}.*".format(yestdaydate), file)
			sftp.get_d( sftp.pwd, 'C:\\PB1\\3rdPartyFiles\\', preserve_mtime=True)
			if filetoclean == None:
				sftp.remove(sftp.pwd+'/'+file)
			else:
				pass
	
	# input()
		
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=VALIDATION; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))		## Create Connection to SQL Server 
	cursor = sapsqlcon.cursor()		## Create Cursor Object
	
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\MSIN*_{0}*'.format(yestdaydate))		## Find the UTC Assembly File with the correct date

	if findfile != []:
		loc = (findfile[0])		## Assign the file name to the location path variable


		wb = xlrd.open_workbook(loc)		## Open the workbook at the file path location
		sheet = wb.sheet_by_index(0)		## Assign which sheet to read the data from.  0 is Sheet1, 1 is Sheet2, etc.

		sheet.cell_value(0,0)		## Assign what portion of the Cells to start in.  0,0 starts in A1 postion

		### Calculation for which Row to start on and creating a row count to ensure all rows are captured in the output ###
		count = 2
		xclrows = (sheet.nrows) - count
		xclstart = 3
		

		
		# print(xclrows)
		# print(sheet.row_values(xclstart))
		# input()

		### While loop to go through all rows in file and append to list which column/row data should be transfered to table in database custom table ###
		while count <= xclrows:
			results =(sheet.row_values(xclstart))
			verify = (results[0], results[1], results[2], results[3], results[4], results[6], results[7], str(results[33]).split('.')[0])
			# print(verify)
			utlList.append(verify)	
			count = count + 1
			xclstart = xclstart + 1
		
	# input()
	else:
		loc = 'NULL'
		
	### For loop to iterate through appended list and insert values to custom table ###
	if utlList != None:
		for x in utlList:
			id, item, ponum, lotid, lot, parentlot, recvdate, startqty = x
			if id.lower() == 'wafer total':
				pass
			elif id.lower() == 'grand total':
				pass
			elif item == '':
				pass
			elif lot == '--------':
				pass
			else:
				if lot == '':
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and ParentLot = '{2}' and FabStage = '{3}' and Stage = 'ASSEM'".format('UTL', item, parentlot, lotid))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						query3 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = 'WIP', RecvQty = '{2}', VendorFileName = '{3}' Where TransID = '{4}'".format(uploaddate, yestdaydate, startqty, str(loc).split('\\')[3], result[0]))
						cursor.execute(query3)
					else:
						itemcheck = re.match(r'^ES.*',item)
						if itemcheck == None:
							query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid,ParentLot, OrigUploadDate,FABWIPProcessed, FabStage) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','N','{14}')".format(item, parentlot, ponum, 'ASSEM', recvdate, startqty, 'WIP', 'UTL', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot, uploaddate, lotid))
							cursor.execute(query1)
						else:
							query2 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid,ParentLot,OrigUploadDate,FABWIPProcessed, UpdateDate, UpdateReason) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','N','{14}', '{15}')".format(item, parentlot, ponum, 'ASSEM', recvdate, startqty, 'WIP', 'UTL', uploaddate, yestdaydate, str(loc).split('\\')[3], 'N', parentlot, uploaddate,uploaddate, 'Item is Shield.  Not valid part.'))
							cursor.execute(query2)
				else:
					query = ("Select * from VALIDATION.dbo.THIRD_PARTY_FILES where Vendor = '{0}' and ItemCode= '{1}' and LotNum = '{2}' and FabStage = '{3}' and Stage = 'ASSEM'".format('UTL', item, lot, lotid))
					cursor.execute(query)
					result = cursor.fetchone()
					if result != None:
						query3 = ("Update VALIDATION.dbo.THIRD_PARTY_FILES SET UploadDate = '{0}', FileDate = '{1}', VendorStatus = '{2}', RecvQty = '{3}', VendorFileName = '{4}' Where TransID = '{5}'".format(uploaddate, yestdaydate, 'Complete', startqty, str(loc).split('\\')[3], result[0]))
						cursor.execute(query3)
					else:
						query1 = ("Insert Into VALIDATION.dbo.THIRD_PARTY_FILES (ItemCode, LotNum, PONum, Stage, RecvDate, RecvQty, VendorStatus, Vendor, UploadDate, FileDate, VendorFileName, IsValid,ParentLot,OrigUploadDate,FABWIPProcessed, FabStage) values ('{0}','{1}','{2}','{3}',replace('{4}','/',''),'{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','N','{14}')".format(item, lot, ponum, 'ASSEM', recvdate, startqty, 'Complete', 'UTL', uploaddate, yestdaydate, str(loc).split('\\')[3], 'Y', parentlot, uploaddate,lotid))
						cursor.execute(query1)
						
						
	## ----  Update Third Party Recevied File (TPRF) table in Validation database for 3P file received ---- ##
	filequery = ("Select isnull(FileName,LastRecvFile) 'FileName', isnull(FileDate,LastRecvDate) 'FileDate' FROM VALIDATION.dbo.TPRF WHERE AsOfDate = '{0}' and Vendor = 'UTL'".format(yestdaydate))
	cursor.execute(filequery)
	filecheck = cursor.fetchone()
	fileCheckList=[]
	fileCheckList.append(filecheck)
	
	if filecheck == None:
		nullstoinsert = [('NULL', 'NULL')]
		if loc == 'NULL':
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'UTL' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result = cursor.fetchone()
			if result != None:
				for x in nullstoinsert:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'UTL' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(loc,yestdaydate,filename,filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in nullstoinsert:
					# print(x)
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('UTL','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(loc,yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
		else:
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'UTL' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result = cursor.fetchone()
			if result != None:
				for x in nullstoinsert:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'UTL' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],filename,filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in nullstoinsert:
					# print(x)
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('UTL','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
	else:
		if loc == 'NULL':
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'UTL' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result3 = cursor.fetchone()
			if result3 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = 'NULL', FileDate = '{0}', LastRecvFile = '{1}', LastRecvDate = '{2}' Where Vendor = 'UTL' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(yestdaydate, filename, filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('UTL','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format('NULL',yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
		else:
			checktoupdate = ("Select * from VALIDATION.dbo.TPRF WHERE Vendor = 'UTL' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')")
			cursor.execute(checktoupdate)
			result2 = cursor.fetchone()
			if result2 != None:
				for x in fileCheckList:
					filename, filedate = x
					tableupdatequery = ("Update VALIDATION.dbo.TPRF SET FileName = '{0}', FileDate = '{1}', LastRecvFile = '{2}', LastRecvDate = '{3}' WHERE Vendor = 'UTL' and AsOfDate = REPLACE(CONVERT(DATE,GETDATE(),112),'-','')".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(tableupdatequery)
			else:
				for x in fileCheckList:
					filename, filedate = x
					fileupdatequery = ("Insert Into VALIDATION.dbo.TPRF (Vendor, FileName, FileDate, LastRecvFile, LastRecvDate, AsOfDate) values ('UTL','{0}','{1}','{2}','{3}',REPLACE(CONVERT(DATE,GETDATE(),112),'-',''))".format(str(loc).split('\\')[3],yestdaydate,filename,filedate))
					cursor.execute(fileupdatequery)
						
						

			
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	
	print("Finished UTL ASSEM File Upload....\n")


	
		
### Run Program Functions ###
		
amkorFile()
chmAsyFile()
oseAsyFile()
promiseFile()
utcAsyFile()
utcFile()
utlAsyFile()
udgAsyFile()

## -- Files not currently loaded -- ##
# chmVeInvFile()
# gtcFile()    --- Empty File, no longer needs to be loaded per Greg Garb -- 06/05/2020
# asemFile()
# chmVeWipFile()

