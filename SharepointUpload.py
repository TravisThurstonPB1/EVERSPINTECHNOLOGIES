### ---- Modules to import ------- ###
import json
import csv
import requests
import pypyodbc
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.runtime.client_request import ClientRequest
from office365.runtime.utilities.request_options import RequestOptions
from office365.runtime.utilities.http_method import HttpMethod
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.file import File
import os
import datetime
import cred

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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\OpenBacklogv3.csv'
	file_create = 'OpenBacklogv3.csv'
	with open(file_name,'wb') as outfile:
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
		columns=[i[0] for i in cursor.description]
		file_name = 'C:\\PB1\\SalesReports\\OpenBacklogv3_'+datecheck+'.csv'
		file_create = 'OpenBacklogv3_'+datecheck+'.csv'
		with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\OpenBacklogv3'+str(today)+'.csv'
	file_create = 'OpenBacklogv3'+str(today)+'.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\BillingsV4.csv'
	file_create = 'BillingsV4.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\BillingsV4'+str(today)+'.csv'
	file_create = 'BillingsV4'+str(today)+'.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\BillingsV4_Cancelled_and_Invoiced.csv'
	file_create = 'BillingsV4_Cancelled_and_Invoiced.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\MSDShipDate.csv'
	file_create = 'MSDShipDate.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\BacklogBillingByProduct.csv'
	file_create = 'BacklogBillingByProduct.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\BacklogBillingSummaryAccountwithProduct.csv'
	file_create = 'BacklogBillingSummaryAccountwithProduct.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\BacklogBillingSummaryCustomerWithProduct.csv'
	file_create = 'BacklogBillingSummaryCustomerWithProduct.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\InvWithOutBackLog'+str(today)+'.csv'
	file_create = 'InventoryWithoutBacklog.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\InvAgingReport.csv'
	file_create = 'InventoryAgingReport.csv'
	with open(file_name,'wb') as outfile:
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
		columns=[i[0] for i in cursor.description]
		file_name = 'C:\\PB1\\SalesReports\ShipLinearityReport_'+datecheck+'.csv'
		file_create = 'ShipLinearityReport.csv'
		with open(file_name,'wb') as outfile:
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
		columns=[i[0] for i in cursor.description]
		file_name = 'C:\\PB1\\SalesReports\ShipLinearityReport.csv'
		file_create = 'ShipLinearityReport.csv'
		with open(file_name,'wb') as outfile:
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
		with open(file_name,'wb') as outfile:
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
		with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\BillingAndBacklog.csv'
	file_create = 'BillingAndBacklogComboReport.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\InvOnHandwithStdCost.csv'
	file_create = 'InvOnHandwithStdCost.csv'
	with open(file_name,'wb') as outfile:
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
	#r = cursor.fetchone()
	#print(r)
	#input()
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\SalesQuoteReport.csv'
	file_create = 'SalesQuoteReport.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\YieldReport.csv'
	file_create = 'YieldReport.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\OTDReport.csv'
	file_create = 'OTDReport.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\OTDReportUpdated.csv'
	file_create = 'OTDReportUpdated.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\VendorReport\\VendorReport.csv'
	file_create = 'VendorReport.csv'
	with open(file_name,'wb') as outfile:
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
	file_name = 'C:\\PB1\\VendorReport\\OPEXReport.csv'
	file_create = 'OPEXReport.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\InvOHWIPReport.csv'
	file_create = 'InvOHWIPReport.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\ProdCostSumMonthly'+str(today)+'.csv'
	file_create = 'ProdCostSumMonthly'+str(today)+'.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\ProdCostSumQtrly'+str(today)+'.csv'
	file_create = 'ProdCostSumQtrly'+str(today)+'.csv'
	with open(file_name,'wb') as outfile:
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
	file_name = 'C:\\PB1\\SalesReports\\DecommitReport'+str(today)+'.csv'
	file_create = 'DecommitReport'+str(today)+'.csv'
	with open(file_name,'wb') as outfile:
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
	file_name = 'C:\\PB1\\SalesReports\\CommitDateChangeLog.csv'
	file_create = 'CommitDateChangeLog.csv'
	with open(file_name,'wb') as outfile:
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
	file_name = 'C:\\PB1\\SalesReports\\NewOrderAndCacellationLog.csv'
	file_create = 'NewOrderAndCacellationLog.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\WIPDashboard.csv'
	file_create = 'WIPDashboard.csv'
	with open(file_name,'wb') as outfile:
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
	columns=[i[0] for i in cursor.description]
	file_name = 'C:\\PB1\\SalesReports\\UserSessionTime.csv'
	file_create = 'UserSessionTime.csv'
	with open(file_name,'wb') as outfile:
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
			ctx.request_form_digest()
			request = ClientRequest(ctx)
			full_url = ("{0}/_api/web/GetFolderByServerRelativeUrl('{1}')/Files/add(url='{2}', overwrite=true)".format(url, furl, file_to_create))
			options = RequestOptions(full_url)
			options.set_header('Accept', 'application/json; odata=nometadata')
			options.set_header('Content-Type', 'application/octet-stream')
			options.set_header('Content-Length', str(os.path.getsize(file_name)))
			options.set_header('X-RequestDigest', ctx.contextWebInformation.form_digest_value)
			options.method = HttpMethod.Post
	### --- Upload File to Sharepoint Site ---- ####
			with open(file_name, 'rb') as outfile:
				ctx.authenticate_request(options)
				data = requests.post(url=full_url, data=outfile, headers=options.headers, auth=options.auth)
	### --- Verify succuess of upload ---- ###
				if data.status_code == 200:
					print("success")
				else:
					print(data.status_code)
					return data.json()['error']
			
		else:
			print(ctx_auth.get_last_error())

			
### --- Script Start ---- ####			
#fileOpenBackLogCreate()
#fileBillingsCreate()
#msdShipDate()

#read_file()