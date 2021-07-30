from SharepointUpload_py3 import SharepointThirdParty
import os
import glob
import datetime
import cred
import pypyodbc
import xlrd
from xlrd import XL_CELL_EMPTY
import xlwt

currentday = datetime.date.today()		## Gets the current Days date
firstday = currentday.replace(day=1)		## Uses the current date to determine the first day of the month
lastmonth = firstday - datetime.timedelta(days=1)		## Uses the first day of the month and finds the last day of the previous month
datesearch = lastmonth.strftime('%Y%m%d')		## Assigns the yyyymmdd format for using in finding the file with the correct date
uploaddate = currentday.strftime('%Y%m%d')		## Assigns the yyyymmdd format to the current date for noting the day of upload
yestday = currentday - datetime.timedelta(days=1)		## sets variable to yesterday's date
oseyestdate = currentday - datetime.timedelta(days=1)
yestdaydate = yestday.strftime('%Y%m%d')
osedate = oseyestdate.strftime('%m%d%y')
folderdate = currentday.strftime('%d%m%Y')

folderurl = '/CP/Shared Documents/Automation/Subledger Automation/Input'

## -- Create folder for the day -- ##

print('Start Folder Creation')

start_make_folder = SharepointThirdParty()
start_make_folder.folderCreate(folderurl)

print('Folder Created')

## -- upload amkor file --##
def amkorFile():
	print('Start amkor upload')
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\Amkor*WIP_Report_*{0}*'.format(yestdaydate))
	if findfile != []:
		loc = findfile[0]
		
	file_name = loc
	file_create = 'Amkor_WIP_Report.xls'
	
	start_upload = SharepointThirdParty()
	start_upload.main(file_create, file_name, folderurl+'/'+folderdate)
	print('Success of file upload')
	
def chmAsyFile():
	print('Start CHPMosAssembly upload')
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*wpsp006a*ASSY*{0}*'.format(yestdaydate))
	if findfile != []:
		loc = findfile[0]
		
	file_name = loc
	file_create = 'wpsp006a_Everspin_ASSY.xls'
	
	start_upload = SharepointThirdParty()
	start_upload.main(file_create, file_name, folderurl+'/'+folderdate)	
	print('Success of file upload')
	
def oseAsyFile():
	print('Start OSEAssembly upload')
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*OSE*WIP*Report*{0}*'.format(osedate))
	if findfile != []:
		loc = findfile[0]
		
	file_name = loc
	file_create = 'OSE WIP Report - EVERSPIN.xls'
	
	start_upload = SharepointThirdParty()
	start_upload.main(file_create, file_name, folderurl+'/'+folderdate)	
	print('Success of file upload')
	
def promisFile():
	print('Start Promise upload')
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\Promis*EWS*WIP*{0}*'.format(yestdaydate))
	if findfile != []:
		loc = findfile[0]
		
	file_name = loc
	file_create = 'Promis_EWS_WIP.xls'
	
	start_upload = SharepointThirdParty()
	start_upload.main(file_create, file_name, folderurl+'/'+folderdate)	
	print('Success of file upload')
	
def utcAsyFile():
	print('Start UTCAssembly upload')
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*Everspin*AssyWIP*{0}*'.format(yestdaydate))
	if findfile != []:
		loc = findfile[0]
		
	file_name = loc
	file_create = 'EVERSPIN_AssyWIP.xls'
	
	start_upload = SharepointThirdParty()
	start_upload.main(file_create, file_name, folderurl+'/'+folderdate)
	print('Success of file upload')
	
def utcFile():
	print('Start UTC upload')
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*UTC_Everspin*WIP*Report*{0}*'.format(yestdaydate))
	if findfile != []:
		loc = findfile[0]
		
	file_name = loc
	file_create = 'UTC_EVERSPIN_WIP_REPORT.xls'
	
	start_upload = SharepointThirdParty()
	start_upload.main(file_create, file_name, folderurl+'/'+folderdate)
	print('Success of file upload')
	
def utlAsyFile():
	print('Start UTLAssembly upload')
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\MSIN*_{0}*'.format(yestdaydate))
	if findfile != []:
		loc = findfile[0]
		
	file_name = loc
	file_create = 'MSINV076.xls'
	
	start_upload = SharepointThirdParty()
	start_upload.main(file_create, file_name, folderurl+'/'+folderdate)
	print('Success of file upload')
	
def udgAsyFile():
	print('Start UDGAssembly upload')
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*DAILY_WIP_DG*{0}*'.format(yestdaydate))
	if findfile != []:
		loc = findfile[0]
		
	file_name = loc
	file_create = 'DAILY_WIP_DG.xls'
	
	start_upload = SharepointThirdParty()
	start_upload.main(file_create, file_name, folderurl+'/'+folderdate)
	print('Success of file upload')
	
def chmAsyInvFile():
	print('Start CHMosAssembly Inventory upload')
	findfile = glob.glob('C:\\PB1\\3rdPartyFiles\\*inventory_report*{0}*'.format(yestdaydate))
	if findfile != []:
		loc = findfile[0]
		
	file_name = loc
	file_create = 'inventory_report.xls'

	start_upload = SharepointThirdParty()
	start_upload.main(file_create, file_name, folderurl+'/'+folderdate)
	print('Success of file upload')

### Run Subledger Report and place in OneDrive location ###
def subledgerReport():
	print('Start Subledger upload')
	sapsqlcon = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))		## Create Connection to SQL Server 
	cursor = sapsqlcon.cursor()		## Create Cursor Object
	
	subrep = [('ItemCode','Family','Stage','Whse','AbsEntry','OnHand Lot','OnHand ParentLot','BatchAtt2','Lot AddmissionDate','Age of Lot (Days)','OnHand/WIP Qty','WO#','PerUnitLotCost','TotalLotCost','GLAccount','Selection Date','AsOfDate')]
	
	query = ("Select * from EverspinTech.dbo.vw_Subledger_report Order by Case When Stage = 'FAB' then 1 when Stage = 'EWS' then 2 when Stage = 'ASSEM' then 3 when Stage = 'FT' then 4 when Stage = 'FG' then 5 end ASC, Family, ItemCode")
	cursor.execute(query)
	result = cursor.fetchone()
	
	while result:
		subrep.append(result)
		result = cursor.fetchone()
	
	
	book = xlwt.Workbook()		## Create New Workbook
	sheet1 = book.add_sheet("Subledger")		## Create New sheet in workbook
	for r, row in enumerate(subrep):		## iterate through .csv file rows
		for c, col in enumerate(row):		## iterate through .csv file columns
			sheet1.write(r,c,col)		## write data to new file
	book.save("C:\\PB1\\Subledger\\Subledger.xls")
	
	cursor.close()
	sapsqlcon.close()	
	
	findfile = glob.glob('C:\\PB1\\Subledger\\Subledger.xls')
	if findfile != []:
		loc = findfile[0]
		
	file_name = loc
	file_create = 'Subleger.xls'
	
	start_upload = SharepointThirdParty()
	start_upload.main(file_create, file_name, folderurl+'/'+folderdate)
	print('Success of file upload')


## -- run functions -- ##	
amkorFile()
chmAsyFile()
oseAsyFile()
promisFile()
utcAsyFile()
utcFile()
utlAsyFile()
udgAsyFile()
chmAsyInvFile()
subledgerReport()
