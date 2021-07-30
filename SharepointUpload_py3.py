### ---- Modules to import ------- ###
import json
import csv
import requests
import pypyodbc
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.runtime.client_request import ClientRequest
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.http.http_method import HttpMethod
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.client_credential import ClientCredential
import os
import datetime
import cred
import xlrd
from xlrd import XL_CELL_EMPTY
import xlwt

### --- Define your global variables.  ----- ####
url = "https://everspintechnologiesinc.sharepoint.com/CP"
username = cred.username
password = cred.password
relative_url = '/CP/Shared Documents/SAP/Sales, Mkt & Operations'
relative_url_dcom = '/CP/Shared Documents/SAP/Sales, Mkt & Operations/Decommit Report'
relative_url_finance = '/CP/Shared Documents/SAP/Finance'
relative_url_wip = '/CP/Shared Documents/SAP/Sales, Mkt & Operations/SAP_WIP_Processing'
relative_url_history = '/CP/Shared Documents/SAP/Sales, Mkt & Operations/BBB History'
relative_url_MBS = '/CP/Shared Documents/SAP/Sales, Mkt & Operations/Monthly Backlog Snapshots'
# relative_url_file = '/CP/Shared Documents/SAP/TestFile.txt'  ### --- For testing ----###
today = datetime.date.today()
opexyear = datetime.date.today().year
folderdate = today.strftime('%d%m%Y')



####  ----- Backlog File Creation ----- #####
def fileOpenBackLogCreate():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("Select * from EverspinTech.dbo.vw_OpenBacklogV3 Order by [S.O. Doc No.]")
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=['s.o. doc no.','line item no','s.o. date','customer name','p.o. no.','end customer','contract manufacturer','design region','part number','cust. request ship out date','orig cust. request ship out date','supp. promise ship out date','orig. supp. promise ship out date','batch qty','target price','ext. sales price','actual ship out date','batch status','batch status2','u_familygroup','item group','as of date','parent customer','parent end customer','quote 1','quote 2','linequotenum']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\OpenBacklogv3.csv'
	file_create = 'OpenBacklogv3.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
####  ----- Backlog File End of the Month Creation ----- #####
def eomOpenBackLog():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	
	currentday = datetime.date.today()
	datecheck = currentday.strftime('%Y%m%d')
	
	testdate = ("Select replace(EOMONTH(GETDATE()),'-','')")
	cursor.execute(testdate)
	result = cursor.fetchone()
	if result[0] == datecheck:
		query=("Select * from EverspinTech.dbo.vw_OpenBacklogV3 Order by [S.O. Doc No.]")
		cursor.execute(query)
		### --- Create .csv file --- ###
		#r = cursor.fetchone()
		#print(r)
		#input()
		columns=['s.o. doc no.','line item no','s.o. date','customer name','p.o. no.','end customer','contract manufacturer','design region','part number','cust. request ship out date','orig cust. request ship out date','supp. promise ship out date','orig. supp. promise ship out date','batch qty','target price','ext. sales price','actual ship out date','batch status','batch status2','u_familygroup','item group','as of date','parent customer','parent end customer','quote 1','quote 2','linequotenum']
		
		# results = cursor.fetchone()
		# while results:
			# columns.append(results)
			# results=cursor.fetchone()
		
		file_name = 'C:\\PB1\\SalesReports\\OpenBacklogv3_'+datecheck+'.csv'
		file_create = 'OpenBacklogv3_'+datecheck+'.csv'
		
		# book = xlwt.Workbook()		## Create New Workbook
		# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
		# for r, row in enumerate(columns):		## iterate through .csv file rows
			# for c, col in enumerate(row):		## iterate through .csv file columns
				# sheet1.write(r,c,col)		## write data to new file
		# book.save(file_name)
		
		with open(file_name,'w',newline="") as outfile:
			report = csv.writer(outfile,delimiter=',')
			report.writerow(columns)
			report.writerows(cursor)
		cursor.close()
		connection.close()
		### --- Call to upload class --- ###
		start_upload = SharepointUpload()
		start_upload.main(file_create, file_name, relative_url_MBS)
	
####  ----- Backlog HISTORY File Creation ----- #####
def fileOpenBackLogCreateHistory():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("Select * from vw_OpenBacklogV3 Order by [S.O. Doc No.]")
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=['s.o. doc no.','line item no','s.o. date','customer name','p.o. no.','end customer','contract manufacturer','design region','part number','cust. request ship out date','orig cust. request ship out date','supp. promise ship out date','orig. supp. promise ship out date','batch qty','target price','ext. sales price','actual ship out date','batch status','batch status2','u_familygroup','item group','as of date','parent customer','parent end customer','quote 1','quote 2','linequotenum']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\OpenBacklogv3'+str(today)+'.csv'
	file_create = 'OpenBacklogv3'+str(today)+'.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url_history)

#### --- Billings File Create --- ###	
def fileBillingsCreate():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("Select * from vw_BillingsV4 Order by [S.O. Doc No.]")
	cursor.execute(query)
	### --- Create .csv file --- ###
	columns=['s.o. doc no.','line item no','s.o. date','customer name','p.o. no.','end customer','contract manufacturer','design region','part number','cust. request ship out date','orig cust. request ship out date','supp. promise ship out date','orig. supp. promise ship out date','batch qty','target price','ext. sales price','actual ship out date','batch status','batch status2','u_familygroup','item group','as of date','parent customer','parent end customer','quote 1','quote 2','linequotenum']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\BillingsV4.csv'
	file_create = 'BillingsV4.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###	
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
#### --- Billings File Create --- ###	
def fileBillingsCreateHistory():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("Select * from vw_BillingsV4 Order by [S.O. Doc No.]")
	cursor.execute(query)
	### --- Create .csv file --- ###
	columns=['s.o. doc no.','line item no','s.o. date','customer name','p.o. no.','end customer','contract manufacturer','design region','part number','cust. request ship out date','orig cust. request ship out date','supp. promise ship out date','orig. supp. promise ship out date','batch qty','target price','ext. sales price','actual ship out date','batch status','batch status2','u_familygroup','item group','as of date','parent customer','parent end customer','quote 1','quote 2','linequotenum']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\BillingsV4'+str(today)+'.csv'
	file_create = 'BillingsV4'+str(today)+'.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###	
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url_history)
		
#### --- Billings File Create (Cancelled and Invoiced) --- ###	
def fileBillingsCancelandInvoice():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("Select * from vw_Billingsv4_Cancelled_and_Invoiced Order by [S.O. Doc No.]")
	cursor.execute(query)
	### --- Create .csv file --- ###
	columns=['s.o. doc no.','line item no','s.o. date','customer name','p.o. no.','end customer','contract manufacturer','design region','part number','cust. request ship out date','orig cust. request ship out date','supp. promise ship out date','orig. supp. promise ship out date','batch qty','target price','ext. sales price','actual ship out date','batch status','batch status2','u_familygroup','item group','as of date','parent customer','parent end customer','quote 1','quote 2','linequotenum']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\BillingsV4_Cancelled_and_Invoiced.csv'
	file_create = 'BillingsV4_Cancelled_and_Invoiced.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###	
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
def msdShipDate():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query = ("Select * from vw_MSDShipDate")
	cursor.execute(query)
	### --- Create .csv file --- ###
	columns=['so number','customer','itemcode','quantity','supplier promised ship date','original msd date','reason for msd date update','as of date']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\MSDShipDate.csv'
	file_create = 'MSDShipDate.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
def backlogBillingProduct():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	populate = ("EXEC LOTMOVE.dbo.BacklogBillingProductPop")
	query = ("Declare @year int = convert(int,left(convert(date,getdate(),112),4)) Exec('Select T2.ItmsGrpNam [Family], FamilyGroup [Family Group], T0.ItemCode [Item Code], Sum(BacklogQ1) [Backlog ('+@Year+' - Q1)],SUM(BacklogQtyQ1) [Backlog Quantity ('+@year+' - Q1)], Sum(BilledQ1) [Billing ('+@Year+' - Q1)], Sum(BilledQtyQ1) [Billed Quantity ('+@year+' - Q1)], Sum([SumQ1]) [Total ('+@Year+' - Q1)], Sum(SumQtyQ1) [Total Quantity ('+@year+' - Q1)], Sum(BacklogQ2) [Backlog ('+@Year+' - Q2)],SUM(BacklogQtyQ2) [Backlog Quantity ('+@year+' - Q2)], Sum(BilledQ2) [Billing ('+@Year+' - Q2)], Sum(BilledQtyQ2) [Billed Quantity ('+@year+' - Q2)], Sum([SumQ2]) [Total ('+@Year+' - Q2)], Sum(SumQtyQ2) [Total Quantity ('+@year+' - Q2)], Sum(BacklogQ3) [Backlog ('+@Year+' - Q3)],SUM(BacklogQtyQ3) [Backlog Quantity ('+@year+' - Q3)], Sum(BilledQ3) [Billing ('+@Year+' - Q3)], Sum(BilledQtyQ3) [Billed Quantity ('+@year+' - Q3)], Sum([SumQ3]) [Total ('+@Year+' - Q3)], Sum(SumQtyQ3) [Total Quantity ('+@year+' - Q3)], Sum(BacklogQ4) [Backlog ('+@Year+' - Q4)],SUM(BacklogQtyQ4) [Backlog Quantity ('+@year+' - Q4)], Sum(BilledQ4) [Billing ('+@Year+' - Q4)], Sum(BilledQtyQ4) [Billed Quantity ('+@year+' - Q4)], Sum([SumQ4]) [Total ('+@Year+' - Q4)], Sum(SumQtyQ4) [Total Quantity ('+@year+' - Q4)], Sum([GrandSum]) [Grand Sum], Sum([GrandSumQty]) [Grand Sum Quantity], convert(date,getdate(),112) [As of Date] from lotmove.dbo.BacklogBillingProduct T0 left join EverspinTech.dbo.OITM T1 on T0.ItemCode collate database_default = T1.ItemCode collate database_default left join EverspinTech.dbo.OITB T2 on T1.ItmsGrpCod = T2.ItmsGrpCod group by FamilyGroup,T0.ItemCode, T2.ItmsGrpNam order by Sum([GrandSum]) DESC, FamilyGroup asc, T0.ItemCode asc')")
	cursor.execute(populate)
	cursor.execute(query)
	### --- Create .csv file --- ###
	columns=['family','family group','item code','backlog (2021 - q1)','backlog quantity (2021 - q1)','billing (2021 - q1)','billed quantity (2021 - q1)','total (2021 - q1)','total quantity (2021 - q1)','backlog (2021 - q2)','backlog quantity (2021 - q2)','billing (2021 - q2)','billed quantity (2021 - q2)','total (2021 - q2)','total quantity (2021 - q2)','backlog (2021 - q3)','backlog quantity (2021 - q3)','billing (2021 - q3)','billed quantity (2021 - q3)','total (2021 - q3)','total quantity (2021 - q3)','backlog (2021 - q4)','backlog quantity (2021 - q4)','billing (2021 - q4)','billed quantity (2021 - q4)','total (2021 - q4)','total quantity (2021 - q4)','grand sum','grand sum quantity','as of date']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\BacklogBillingByProduct.csv'
	file_create = 'BacklogBillingByProduct.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
def backlogBillingAccount():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	populate = ("EXEC LOTMOVE.dbo.BacklogBillingSummaryAccountPop")
	query = ("Declare @year int = convert(int,left(convert(date,getdate(),112),4)) Exec('Select EndCustomer [End Customer],Max(Region) [Region],T2.ItmsGrpNam [Family],FamilyGroup [Family Group],T0.ItemCode [Item Code], Sum(BacklogQ1) [Backlog ('+@Year+' - Q1)],SUM(BacklogQtyQ1) [Backlog Quantity ('+@year+' - Q1)], Sum(BilledQ1) [Billing ('+@Year+' - Q1)], Sum(BilledQtyQ1) [Billed Quantity ('+@year+' - Q1)], Sum([SumQ1]) [Total ('+@Year+' - Q1)], Sum(SumQtyQ1) [Total Quantity ('+@year+' - Q1)], Sum(BacklogQ2) [Backlog ('+@Year+' - Q2)],SUM(BacklogQtyQ2) [Backlog Quantity ('+@year+' - Q2)], Sum(BilledQ2) [Billing ('+@Year+' - Q2)], Sum(BilledQtyQ2) [Billed Quantity ('+@year+' - Q2)], Sum([SumQ2]) [Total ('+@Year+' - Q2)], Sum(SumQtyQ2) [Total Quantity ('+@year+' - Q2)], Sum(BacklogQ3) [Backlog ('+@Year+' - Q3)],SUM(BacklogQtyQ3) [Backlog Quantity ('+@year+' - Q3)], Sum(BilledQ3) [Billing ('+@Year+' - Q3)], Sum(BilledQtyQ3) [Billed Quantity ('+@year+' - Q3)], Sum([SumQ3]) [Total ('+@Year+' - Q3)], Sum(SumQtyQ3) [Total Quantity ('+@year+' - Q3)], Sum(BacklogQ4) [Backlog ('+@Year+' - Q4)],SUM(BacklogQtyQ4) [Backlog Quantity ('+@year+' - Q4)], Sum(BilledQ4) [Billing ('+@Year+' - Q4)], Sum(BilledQtyQ4) [Billed Quantity ('+@year+' - Q4)], Sum([SumQ4]) [Total ('+@Year+' - Q4)], Sum(SumQtyQ4) [Total Quantity ('+@year+' - Q4)], Sum([GrandSum]) [Grand Sum], Sum([GrandSumQty]) [Grand Sum Quantity], convert(date,getdate(),112) [As Of Date] from LOTMOVE.dbo.BacklogBillingSummaryAccount T0 inner join OITM T1 on T0.Itemcode = T1.Itemcode inner join OITB T2 on T1.ItmsGrpCod = T2.ItmsGrpCod group by Endcustomer,T2.ItmsGrpNam,FamilyGroup,T0.ItemCode order by Sum([GrandSum]) DESC, EndCustomer Asc, FamilyGroup asc, T0.ItemCode asc')")
	cursor.execute(populate)
	cursor.execute(query)
	### --- Create .csv file --- ###
	columns=['end customer','region','family','family group','item code','backlog (2021 - q1)','backlog quantity (2021 - q1)','billing (2021 - q1)','billed quantity (2021 - q1)','total (2021 - q1)','total quantity (2021 - q1)','backlog (2021 - q2)','backlog quantity (2021 - q2)','billing (2021 - q2)','billed quantity (2021 - q2)','total (2021 - q2)','total quantity (2021 - q2)','backlog (2021 - q3)','backlog quantity (2021 - q3)','billing (2021 - q3)','billed quantity (2021 - q3)','total (2021 - q3)','total quantity (2021 - q3)','backlog (2021 - q4)','backlog quantity (2021 - q4)','billing (2021 - q4)','billed quantity (2021 - q4)','total (2021 - q4)','total quantity (2021 - q4)','grand sum','grand sum quantity','as of date']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\BacklogBillingSummaryAccountwithProduct.csv'
	file_create = 'BacklogBillingSummaryAccountwithProduct.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
def backlogBillingCustomer():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	populate = ("EXEC LOTMOVE.dbo.BacklogBillingSummaryCustomerPop")
	query = ("Declare @year int = convert(int,left(convert(date,getdate(),112),4)) Exec('Select EndCustomer [End Customer],Max(Region) [Region],T2.ItmsGrpNam [Family],FamilyGroup [Family Group],T0.ItemCode [Item Code], Sum(BacklogQ1) [Backlog ('+@Year+' - Q1)],SUM(BacklogQtyQ1) [Backlog Quantity ('+@year+' - Q1)], Sum(BilledQ1) [Billing ('+@Year+' - Q1)], Sum(BilledQtyQ1) [Billed Quantity ('+@year+' - Q1)], Sum([SumQ1]) [Total ('+@Year+' - Q1)], Sum(SumQtyQ1) [Total Quantity ('+@year+' - Q1)], Sum(BacklogQ2) [Backlog ('+@Year+' - Q2)],SUM(BacklogQtyQ2) [Backlog Quantity ('+@year+' - Q2)], Sum(BilledQ2) [Billing ('+@Year+' - Q2)], Sum(BilledQtyQ2) [Billed Quantity ('+@year+' - Q2)], Sum([SumQ2]) [Total ('+@Year+' - Q2)], Sum(SumQtyQ2) [Total Quantity ('+@year+' - Q2)], Sum(BacklogQ3) [Backlog ('+@Year+' - Q3)],SUM(BacklogQtyQ3) [Backlog Quantity ('+@year+' - Q3)], Sum(BilledQ3) [Billing ('+@Year+' - Q3)], Sum(BilledQtyQ3) [Billed Quantity ('+@year+' - Q3)], Sum([SumQ3]) [Total ('+@Year+' - Q3)], Sum(SumQtyQ3) [Total Quantity ('+@year+' - Q3)], Sum(BacklogQ4) [Backlog ('+@Year+' - Q4)],SUM(BacklogQtyQ4) [Backlog Quantity ('+@year+' - Q4)], Sum(BilledQ4) [Billing ('+@Year+' - Q4)], Sum(BilledQtyQ4) [Billed Quantity ('+@year+' - Q4)], Sum([SumQ4]) [Total ('+@Year+' - Q4)], Sum(SumQtyQ4) [Total Quantity ('+@year+' - Q4)], Sum([GrandSum]) [Grand Sum], Sum([GrandSumQty]) [Grand Sum Quantity], convert(date,getdate(),112) [As of Date] from LOTMOVE.dbo.BacklogBillingSummaryCustomer T0 Left join OITM T1 on T0.ItemCode = T1.Itemcode inner join OITB T2 on T1.ItmsGrpCod = T2.ItmsGrpCod group by Endcustomer, T2.ItmsGrpNam,FamilyGroup,T0.ItemCode order by Sum([GrandSum]) DESC, EndCustomer Asc, FamilyGroup asc, T0.ItemCode asc')")
	cursor.execute(populate)
	cursor.execute(query)
	### --- Create .csv file --- ###
	columns=['end customer','region','family','family group','item code','backlog (2021 - q1)','backlog quantity (2021 - q1)','billing (2021 - q1)','billed quantity (2021 - q1)','total (2021 - q1)','total quantity (2021 - q1)','backlog (2021 - q2)','backlog quantity (2021 - q2)','billing (2021 - q2)','billed quantity (2021 - q2)','total (2021 - q2)','total quantity (2021 - q2)','backlog (2021 - q3)','backlog quantity (2021 - q3)','billing (2021 - q3)','billed quantity (2021 - q3)','total (2021 - q3)','total quantity (2021 - q3)','backlog (2021 - q4)','backlog quantity (2021 - q4)','billing (2021 - q4)','billed quantity (2021 - q4)','total (2021 - q4)','total quantity (2021 - q4)','grand sum','grand sum quantity','as of date']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\BacklogBillingSummaryCustomerWithProduct.csv'
	file_create = 'BacklogBillingSummaryCustomerWithProduct.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
def invwithoutbacklog():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query = ("Select * from Everspintech.dbo.vw_InvWithOutBackLog")
	cursor.execute(query)
	### --- Create .csv file --- ###
	columns=['product no.','warehouse','onhand','family','product type','as of date']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\InvWithOutBackLog'+str(today)+'.csv'
	file_create = 'InventoryWithoutBacklog.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
def invAgingReport():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query = ("Select * from Everspintech.dbo.vw_InvAgingReport")
	cursor.execute(query)
	### --- Create .csv file --- ###
	columns=['product no.','lot no.','warehouse','onhand','lotqty','family','product type','createdate','as of date']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\InvAgingReport.csv'
	file_create = 'InventoryAgingReport.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
def shipLinearityReport():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	currentday = datetime.date.today()
	datecheck = currentday.strftime('%Y%m%d')
	# print(datecheck)
	testdate = ("Select replace(EOMONTH(GETDATE()),'-','')")
	cursor.execute(testdate)
	result = cursor.fetchone()
	# print(result[0])
	if result[0] == datecheck:
		query = ("Select * from Everspintech.dbo.vw_ShipLinearityReport Order By [As of Date]")
		cursor.execute(query)
		### --- Create .csv file --- ###
		columns=['booking','backlog','shipments total','shipments toggle','shipments sensor','shipments sttmram','shipments embedded','q1aop/q2-q4qrf','royalty and nre fcst','net adjustments','backlog + shipments','as of date','day','linearity','midpoint guidance','revenue to date']
		
		# results = cursor.fetchone()
		# while results:
			# columns.append(results)
			# results=cursor.fetchone()
		
		file_name = 'C:\\PB1\\SalesReports\ShipLinearityReport_'+datecheck+'.csv'
		file_create = 'ShipLinearityReport.csv'
		
		# book = xlwt.Workbook()		## Create New Workbook
		# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
		# for r, row in enumerate(columns):		## iterate through .csv file rows
			# for c, col in enumerate(row):		## iterate through .csv file columns
				# sheet1.write(r,c,col)		## write data to new file
		# book.save(file_name)
		
		with open(file_name,'w',newline="") as outfile:
			report = csv.writer(outfile,delimiter=',')
			report.writerow(columns)
			report.writerows(cursor)
		cursor.close()
		connection.close()
		### --- Call to upload class --- ###
		start_upload = SharepointUpload()
		start_upload.main(file_create, file_name, relative_url)
	else:	
		query = ("Select * from Everspintech.dbo.vw_ShipLinearityReport Order By [As of Date]")
		cursor.execute(query)
		### --- Create .csv file --- ###
		columns=['booking','backlog','shipments total','shipments toggle','shipments sensor','shipments sttmram','shipments embedded','q1aop/q2-q4qrf','royalty and nre fcst','net adjustments','backlog + shipments','as of date','day','linearity','midpoint guidance','revenue to date']
		
		# results = cursor.fetchone()
		# while results:
			# columns.append(results)
			# results=cursor.fetchone()
		
		file_name = 'C:\\PB1\\SalesReports\ShipLinearityReport.csv'
		file_create = 'ShipLinearityReport.csv'
		
		# book = xlwt.Workbook()		## Create New Workbook
		# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
		# for r, row in enumerate(columns):		## iterate through .csv file rows
			# for c, col in enumerate(row):		## iterate through .csv file columns
				# sheet1.write(r,c,col)		## write data to new file
		# book.save(file_name)
		
		with open(file_name,'w',newline="") as outfile:
			report = csv.writer(outfile,delimiter=',')
			report.writerow(columns)
			report.writerows(cursor)
		cursor.close()
		connection.close()
		### --- Call to upload class --- ###
		start_upload = SharepointUpload()
		start_upload.main(file_create, file_name, relative_url)

def shipLinearityReportTest():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='AUTOM_WIP_SANDBOX', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	currentday = datetime.date.today()
	datecheck = currentday.strftime('%Y%m%d')
	# print(datecheck)
	testdate = ("Select replace(EOMONTH(GETDATE()),'-','')")
	cursor.execute(testdate)
	result = cursor.fetchone()
	# print(result[0])
	if result[0] == datecheck:
		query = ("Select * from AUTOM_WIP_SANDBOX.dbo.vw_ShipLinearityReport Order By [As of Date]")
		cursor.execute(query)
		### --- Create .csv file --- ###
		columns=[i[0] for i in cursor.description]
		file_name = 'C:\\PB1\\SalesReports\ShipLinearityReport_Test_'+datecheck+'.csv'
		file_create = 'ShipLinearityReport_Test.csv'
		with open(file_name,'w',newline="") as outfile:
			report = csv.writer(outfile,delimiter=',')
			report.writerow(columns)
			report.writerows(cursor)
		cursor.close()
		connection.close()
		### --- Call to upload class --- ###
		start_upload = SharepointUpload()
		start_upload.main(file_create, file_name, relative_url)
	else:	
		query = ("Select * from AUTOM_WIP_SANDBOX.dbo.vw_ShipLinearityReport Order By [As of Date]")
		cursor.execute(query)
		### --- Create .csv file --- ###
		columns=[i[0] for i in cursor.description]
		file_name = 'C:\\PB1\\SalesReports\ShipLinearityReport_Test.csv'
		file_create = 'ShipLinearityReport_Test.csv'
		with open(file_name,'w',newline="") as outfile:
			report = csv.writer(outfile,delimiter=',')
			report.writerow(columns)
			report.writerows(cursor)
		cursor.close()
		connection.close()
		### --- Call to upload class --- ###
		start_upload = SharepointUpload()
		start_upload.main(file_create, file_name, relative_url)		
		

#### --- Creates combined report for Billings and Backlog --- ####	
def billingBacklogCombine():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("Select * from vw_BillingAndBacklogv1 Order by [S.O. Doc No.]")
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=['s.o. doc no.','line item no','s.o. date','customer name','p.o. no.','end customer','contract manufacturer','design region','part number','cust. request ship out date','orig cust. request ship out date','supp. promise ship out date','orig. supp. promise ship out date','batch qty','target price','ext. sales price','actual ship out date','batch status','batch status2','u_familygroup','item group','as of date','parent customer','parent end customer','quote 1','quote 2','type','year & qrt']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\BillingAndBacklog.csv'
	file_create = 'BillingAndBacklogComboReport.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
#### --- Creates Inv OnHand with Standard Cost Report --- ####	
def invOnHandwithStdCost():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("Select * from vw_InvOnHandwithStdCost")
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=['item code','item name','family','warehouse','standard cost','on hand','total value','family group','active y/n','as of date']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\InvOnHandwithStdCost.csv'
	file_create = 'InvOnHandwithStdCost.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
#### --- Creates Sales Quotation Report --- ####	
def salesQuoteReport():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("Select * from vw_SalesQuoteforSharepoint")
	cursor.execute(query)
	### --- Create .csv file --- ###

	columns =['doc num', 'doc status', 'doc date', 'doc due date', 'tax date', 'valid from date', 'customer code', 'customer name','itemcode', 'item description', 'quantity', 'price', 'line total', 'u_fieldmin', 'u_requestdcost', 'u_quotedresale', 'doc total', 'u_endcust', 'u_endcustname', 'u_endcustter', 'u_endcustcitystate', 'u_endcustlocation', 'u_contmanufcust', 'u_contmanname', 'u_internalremarks', 'u_legacyquotenum', 'u_distributor', 'u_distcontact', 'u_registrationnum', 'u_salesforceopnum', 'u_location', 'u_programname', 'u_quotereceiveddate', 'as of date']

	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
		
	file_name = 'C:\\PB1\\SalesReports\\SalesQuoteReport.csv'
	file_create = 'SalesQuoteReport.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
		
	cursor.close()
	connection.close()
	
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)

#### --- Creates Yield Report --- ####	
def yieldReport():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	populate=("Exec PIO_PopYieldTable")
	query=("select * from LOTMOVE.dbo.YieldReport")
	cursor.execute(populate)
	cursor.commit()
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=['family','package','productstage','totalplannedqty','totalcompleteqty','totalyieldloss','totalyieldpercentage','asofdate','daterange']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\YieldReport.csv'
	file_create = 'YieldReport.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)	
		
#### --- Creates OTD Report --- ####	
def otdReport():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("select * from vw_OTDReport")
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=['s/o no','so date','part number','part description','days between docdate & cust req ship date','part number lead time in wks','part number lead time in days','days between lead time & calculate days b/w docdate & custreq shipdate','ship date vs lead time','actual ship date','msd date','on time ship days - msd','on time delivery - msd','crd date','on time ship days - crd','on time delivery - crd','docstatus','delivery no','as of date']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\OTDReport.csv'
	file_create = 'OTDReport.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
#### --- Creates OTD Report --- ####	
def otdReport2():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("select * from vw_OTDReportV3")
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=['s/o no','so date','so date update date','part number','part description','days between docdate & cust req ship date','part number lead time in wks','part number lead time in days','days between lead time & calculate days b/w docdate & custreq shipdate','ship date vs lead time	updated - ship date vs lead time','actual ship date	msd date','on time ship days - msd','on time delivery - msd','crd date','on time ship days - crd','on time delivery - crd','docstatus','delivery no','as of date','linenum','history customer req ship date','days between update date and customer req ship date','falls within date parameters','updated - day between original crd & new crd']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\OTDReportUpdated.csv'
	file_create = 'OTDReportUpdated.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
#### --- Creates Vendor Report --- ####	
def vendorReport():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("select * from vw_VendorReport_Sharepoint")
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=['Posting Date','Delivery Date','PO Exp Date','PO Revision Number','PO Type','PO Line Number','Vendor Code','Vendor Name','BP Group','PO Quote Number','Item Code','Item Description','Tool','SAP Number','Tool ID','Department Code','Department Name','G/L','Reporting Group','QTY','QTY Open','QTY Closed','Unit Price','Line Item Total','Amount Paid','Document Status','Remarks','Buyer Name','Payer Name','Document Number','A/P Document Date','Outgoing Payment Number','Doc Type']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\VendorReport\\VendorReport.csv'
	file_create = 'VendorReport.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url_finance)
	
#### --- Creates OPEX Report --- ####	
def opexReport():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("TRUNCATE TABLE VALIDATION.dbo.OPEX_REPORT; INSERT INTO VALIDATION.dbo.OPEX_REPORT EXEC PB1_OPEX_Crystal_Report '{0}'".format(opexyear))
	cursor.execute(query)
	query2 = ("SELECT * FROM VALIDATION.dbo.OPEX_REPORT")
	cursor.execute(query2)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=[i[0] for i in cursor.description]
	# columns=['Balance','AcctTitle','AcctDesc','Department','DocId','DocType','GlAcct','TransMonth','TransYear','Program','Qrt']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\VendorReport\\OPEXReport.csv'
	file_create = 'OPEXReport.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url_finance)
		
#### --- Creates Inv OnHand w/WIP report --- ####	
def invOnHandWip():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("Select * from vw_InvOHWIPReportSharepoint")
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=['family','familygroup','itemcode','sap lot','parentlot','whscode','onhand','wip','onhand/wip','date','cost total','processstage','asofdate']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\InvOHWIPReport.csv'
	file_create = 'InvOHWIPReport.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
#### --- Monthly Product Cost Summary --- ####	
def prdCostSumMonth():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("EXEC PB1_ProductCostSummaryMonthlyReport")
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=['itemcode','partfamily','package','grade','quantity','unitcost','stdcost','yield','mrborgcost','mrbnewcost','mrbquantity','mrbwritedown','mrbyieldwritedown','stage','asofdate','date range']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\ProdCostSumMonthly'+str(today)+'.csv'
	file_create = 'ProdCostSumMonthly'+str(today)+'.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
#### --- Quarterly Product Cost Summary --- ####	
def prdCostSumQtr():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("EXEC PB1_ProductCostSummaryQtrReport")
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=['itemcode','partfamily','package','grade','quantity','unitcost','stdcost','yield','mrborgcost','mrbnewcost','mrbquantity','mrbwritedown','mrbyieldwritedown','stage','asofdate','date range']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\ProdCostSumQtrly'+str(today)+'.csv'
	file_create = 'ProdCostSumQtrly'+str(today)+'.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
#### --- Decommit Report --- ####	
def deCommitReport():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("Select * from vw_DecommitReport")
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=[i[0] for i in cursor.description]
	# columns=['sales order #','po #','customer name','end customer name','product family','part number','quantity','line total','new supplier prom ship date','prev supplier prom ship date','customer req ship date','original cust req ship date']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\DecommitReport'+str(today)+'.csv'
	file_create = 'DecommitReport'+str(today)+'.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url_dcom)
	
def ComChangeLog():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("Select * from vw_CommitDateChangeLog ORDER BY [Sales Order #], [SO Line No], [UpdateDate]")
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=[i[0] for i in cursor.description]
	# columns=['sales order #','po line item no','so line no','po #','customer name','end customer name','product family','part number','quantity','line total','new supplier prom ship date','prev supplier prom ship date','updatedate','u_spsdupdate','u_endcustter']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\CommitDateChangeLog.csv'
	file_create = 'CommitDateChangeLog.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
	
def NewOrderandCancelLog():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("Select * from vw_NewOrderandCacellationLog")
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=[i[0] for i in cursor.description]
	# columns=['docnum','canceled','cancellation date','docdate','cardcode','cardname','polinenum','solinenum','itemcode','dscription','quantity','price','linetotal','u_endcustter','product family','u_endcustname','numatcard','u_suppromshipdate','original supplier prom ship date','u_spsdupdate','indecommitreport']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\NewOrderAndCacellationLog.csv'
	file_create = 'NewOrderAndCacellationLog.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)
	
	
#### --- WIP Dashboard --- ####	
def wipDashboard():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("Select * from vw_WIPDashboard")
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=['docdate','docentry','source','itemcode','family','stage','lotnumber','parentlot','asofdate']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\WIPDashboard.csv'
	file_create = 'WIPDashboard.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url_wip)
	
	
	
#### --- User session Time report --- ####	
def userSessionTime():
	#### --- Connect to Server and run query --- ####
	connection = pypyodbc.connect(DRIVER='{SQL Server}', Server='10.0.0.6\SAPB1_SQL', Database='Everspintech', uid='{0}'.format(cred.uid), pwd='{0}'.format(cred.pwd))
	cursor = connection.cursor()
	query=("Select * from vw_UserSessionTime Order By UserCode, LoginDate")
	cursor.execute(query)
	### --- Create .csv file --- ###
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=['usercode','logindate','sessiontime']
	
	# results = cursor.fetchone()
	# while results:
		# columns.append(results)
		# results=cursor.fetchone()
	
	file_name = 'C:\\PB1\\SalesReports\\UserSessionTime.csv'
	file_create = 'UserSessionTime.csv'
	
	# book = xlwt.Workbook()		## Create New Workbook
	# sheet1 = book.add_sheet("in")		## Create New sheet in workbook
	# for r, row in enumerate(columns):		## iterate through .csv file rows
		# for c, col in enumerate(row):		## iterate through .csv file columns
			# sheet1.write(r,c,col)		## write data to new file
	# book.save(file_name)
	
	with open(file_name,'w',newline="") as outfile:
		report = csv.writer(outfile,delimiter=',')
		report.writerow(columns)
		report.writerows(cursor)
	cursor.close()
	connection.close()
	### --- Call to upload class --- ###
	start_upload = SharepointUpload()
	start_upload.main(file_create, file_name, relative_url)

	

### --- Test to determine conection and if file exists in Sharepoint file ----- ####
#def read_file():
#	ctx_auth = AuthenticationContext(url)
#	if ctx_auth.acquire_token_for_user(username,password):
#		ctx = ClientContext(url, ctx_auth)
#		request = ClientRequest(ctx)
#		options = RequestOptions("{0}/_api/web/GetFolderByServerRelativeUrl('{1}')/Files('TestFile.txt')".format(url, relative_url))
#		options.set_header('Accept', 'application/json; odata=verbose')
#		options.method = HttpMethod.Post
#		data = request.execute_query_direct(options)
#		s = json.loads(data.content)
#		print(s['d']['Name'])
		
#	else:
#		print(ctx_auth.get_last_error())

	
### --- Main upload for File to Sharepoint ---- ###	
class SharepointUpload():
	
	
	def main(self, fcreate, fname, furl):
		file_to_create = fcreate
		file_name = fname
	#### --- Create Sharepoint Authorized Connection ---- ####
		ctx_auth = AuthenticationContext(url)
		if ctx_auth.acquire_token_for_user(username, password):
			ctx= ClientContext(url,ctx_auth)
			web=ctx.web
			ctx.load(web)
			ctx.execute_query()
			automation_folder = ctx.web.get_folder_by_server_relative_url(furl)
			
			### --- Upload File to Sharepoint Site ---- ####
			with open(file_name, 'rb') as outfile:
				file_to_upload = outfile.read()
				
			file_upload = automation_folder.upload_file(file_to_create, file_to_upload)
			ctx.execute_query()			
		else:
			print(ctx_auth.get_last_error())
			
### ---- Class for Creating folder and uploading 3rd party files ----- ###
class SharepointThirdParty():
	
	
	def main(self, fcreate, fname, furl):
		file_to_create = fcreate
		file_name = fname
	#### --- Create Sharepoint Authorized Connection ---- ####
		ctx_auth = AuthenticationContext(url)
		if ctx_auth.acquire_token_for_user(username, password):
			ctx= ClientContext(url,ctx_auth)
			web=ctx.web
			ctx.load(web)
			ctx.execute_query()
			automation_folder = ctx.web.get_folder_by_server_relative_url(furl)
			
			### --- Upload File to Sharepoint Site ---- ####
			with open(file_name, 'rb') as outfile:
				file_to_upload = outfile.read()
				
			file_upload = automation_folder.upload_file(file_to_create, file_to_upload)
			ctx.execute_query()			
		else:
			print(ctx_auth.get_last_error())
			
	def folderCreate(self, folderurl):
		
		#### --- Create Sharepoint Authorized Connection ---- ####
		ctx_auth = AuthenticationContext(url)
		if ctx_auth.acquire_token_for_user(username, password):
			ctx= ClientContext(url,ctx_auth)
			ctx.request_form_digest()
			request = ClientRequest(ctx)
			automation_folder = ctx.web.get_folder_by_server_relative_url(folderurl)
			makefolder = automation_folder.folders.add(folderdate)
			ctx.execute_query()
		else:
			print(ctx_auth.get_last_error())

			
### ---- Class for uploading SAP to 3rd party Detail report ----- ###
class SharepointSAPtoThirdRep():
	
	
	def main(self, fcreate, fname, furl):
		file_to_create = fcreate
		file_name = fname
	#### --- Create Sharepoint Authorized Connection ---- ####
		ctx_auth = AuthenticationContext(url)
		if ctx_auth.acquire_token_for_user(username, password):
			ctx= ClientContext(url,ctx_auth)
			web=ctx.web
			ctx.load(web)
			ctx.execute_query()
			automation_folder = ctx.web.get_folder_by_server_relative_url(furl)
			
			### --- Upload File to Sharepoint Site ---- ####
			with open(file_name, 'rb') as outfile:
				file_to_upload = outfile.read()
				
			file_upload = automation_folder.upload_file(file_to_create, file_to_upload)
			ctx.execute_query()			
		else:
			print(ctx_auth.get_last_error())
			
			
### --- Script Start ---- ####			
#fileOpenBackLogCreate()
#fileBillingsCreate()
#msdShipDate()

#read_file()