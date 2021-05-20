import sys
import pypyodbc
import csv
import pymysql
import re 
import itertools
import time
#import progressbar
import mysqllogin


start_time=time.time()


preimportEVS = []
preimportEVS1 = []
insertSAPCreate = []
insertSAPCreate2 = []
insertSAPComp = []
insertSAPComp2 = []
insertSAPComp3 = []
parselist = []
parselist2 = []
parselist3 = []
manualSAP = []



def dataGather():
	print ("Starting Data Gathering...")
	mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', port=3306, database='mtsdb')
	cursor1 = mysqlcon.cursor()
	mySQLcomVT1 = ("select PROM.partID, PROM.startQty, CASE WHEN PROM.partID = 'WC01N10C-ENG' THEN 'E_GTC' ELSE 'E_CHD' End as 'WhsFinish', CMOS.MFGPartID, PROM.startQty, 'F_CHD' as 'WhsStart', PROM.waferLot, PROM.promisLot, ifnull(PROM.shipQty,0) as 'shipQty' from mtsdb.tblPromisLotInfo PROM inner join mtsdb.tblCMOSLotInfo CMOS on PROM.waferLot = CMOS.waferLot where  PROM.lotStartDate >= date_add(curdate(), interval -720 day) and (CMOS.startQty = PROM.startQty or ifnull(PROM.shipQty,0)!=0) and PROM.partID != 'WB06M35M' order by PROM.waferLot desc")
	cursor1.execute(mySQLcomVT1)
#	columns = [i[0] for i in cursor.description]
#	directory ='C:\CSVReport\Output_Assembly'
#	report = csv.writer(open(directory + timestr + '.csv', 'w', newline=''), delimiter = ',')
#	report.writerow(columns)
	results = cursor1.fetchone()

	while results:
		preimportEVS.append(results)
		results = cursor1.fetchone()
		#bar.update()
	#bar.finish()
#	print (preimportEVS)
#	print (manualSAP)
#	input()
	print ("Spinweb Test data gathering complete.")
	cursor1.close()
	mysqlcon.close()
	print("\n -----{} seconds------".format(time.time()-start_time))

###### Take List of Spinweb data and validate against existing data to determine what should be done with the data #########	
def dataParse():
	print ("Starting Data Parse...")
	connection = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
	cursor = connection.cursor()
	
	try:
#		for x in preimportEVS:
#			print(x)
#		input()	
		print("Check to see if reported Complete...")	
		for x in preimportEVS:
			pw, pqty, whsf, rw, sqty, whss, wlot, plot, shpqty = x
			test4 = ("select IBT1.Itemcode, IBT1.Batchnum, IBT1.BsDocEntry from EverspinTech.dbo.IBT1 with(Nolock) where batchnum = '{0}' and Basetype = 59".format(plot))
			cursor.execute(test4)
			testresult4 = cursor.fetchone()
			# print("Testresult4", testresult4)
#			input()
			if testresult4 != None:
				pass
			else:
				if shpqty > 0:
					test7 = ("select OWOR.DocEntry, OWOR.ItemCode, OWOR.U_SpinwebNo from EverspinTech.dbo.OWOR with(Nolock) where U_spinwebno = '{0}' and status = 'R'".format(wlot))
				# test7 = ("Select IBT1.ItemCode, IBT1.BatchNum, IBT1.BsDocEntry from EverspinTech.dbo.IBT1 with(Nolock) where batchnum = '{0}' and Basetype = 60 and BsDocType = 202".format(wlot))
					cursor.execute(test7)
					testresult9 = cursor.fetchone()
					# print("Testresult9", testresult9)
					if testresult9 != None:
						verify=(plot, wlot, pw, rw, whsf, shpqty, shpqty, testresult9[0])
						insertSAPComp.append(verify)
					else:
						verify = (pw, pqty, whsf, rw, sqty, whss, wlot)
						insertSAPCreate2.append(verify)
						# pass
						# reason = ('0', pw,sqty,wlot,"Shipped in Spinweb, but no Production Order in SAP associated to Wafer Lot.  Line query 85")
						# manualSAP.append(reason)
				# else:
					# test5 = ("select OWOR.DocEntry, OWOR.ItemCode, OWOR.U_SpinwebNo from EverspinTech.dbo.OWOR where U_spinwebno = '{0}' and status = 'R' and isnull(U_ParentPRDO,0) = 0".format(wlot))
					# cursor.execute(test5)
					# testresult5=cursor.fetchone()
					# print("testresult5", testresult5)
					# if testresult5 != None:
						# verify=(plot, wlot, pw, rw, whsf, shpqty, shpqty, testresult5[0])
# #						print(verify)
						# insertSAPComp.append(verify)
				else:
					test6=("Select IBT1.ItemCode, IBT1.BatchNum, IBT1.BaseEntry from EverspinTech.dbo.IBT1 with(Nolock) where batchnum = '{0}' and BaseType = 20".format(wlot))
					cursor.execute(test6)
					testresult6=cursor.fetchone()
					# print("testresult6", testresult6)
					if testresult6 != None:
						test8=("Select S0.ItemCode, S0.BatchNum 'SAP Lot', S0.WhsCode, isnull(S0.InQty,0)-isnull(S1.OutQty,0) 'OnHand' From (select ItemCode, Batchnum, whscode, sum(quantity) 'InQty' from EverspinTech.dbo.IBT1 with(nolock) where Direction=0 AND Docdate <= convert(date, getdate(),112) group by ItemCode, BatchNum, WhsCode) S0 left Join (select Distinct ItemCode, BatchNum, WhsCode, sum(quantity) 'OutQty' from EverspinTech.dbo.IBT1 with(nolock) where Direction=1 and DocDate <= convert(date, getdate(),112) Group by ItemCode, BatchNum, WhsCode) S1 on S0.ItemCode = S1.ItemCode and S0.BatchNum = S1.BatchNum and S0.WhsCode = S1.WhsCode Where S0.BatchNum = '{0}' and S0.ItemCode = '{1}'".format(wlot, rw))
						cursor.execute(test8)
						testresult10=cursor.fetchone()
						# print("testresult10", testresult10)
						if testresult10[3] >= sqty:
							verify= (pw, pqty, whsf, rw, sqty, whss, wlot)
							insertSAPCreate2.append(verify)
						elif testresult10[3] == 0:
							pass
						else:
							reason = ('N/A', pw, sqty, wlot, "On Hand quantity in SAP less than Start qty.  Line 110")
							manualSAP.append(reason)
					else:
#							pass
						reason = ('N/A', pw, sqty, wlot, "Cannot Find Wafer Lot in SAP.  Line query 114.")
						manualSAP.append(reason)
			
			#bar.update()
		

	except:
		print ("An error Occured with Parsing the Data.")
		raise
	#bar.finish()
#	print("Error Entries\n", len(manualSAP), "\n", manualSAP, "\n")
#	print(insertSAPComp, "\n")
	# print("GRPO Entries\n", insertSAPCreate, "\n", insertSAPCreate2, "\n")
	# input()
	print("Data Parsing Complete.")
	print("\n -----{} seconds------".format(time.time()-start_time))
	
	cursor.close()
	connection.close()

def createPRDOTbl ():
	print("Starting Insert of PRDO Create Data....")
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))			
	cursor = sapsqlcon.cursor()
	
	#### Gather created GRPO's #######
	query = ("Select PCGR.QtyToRecv 'PlannedQty', 'E_CHD' 'WhseFinish', PCGR.ItemCode, PCGR.QtyToRecv 'StartQty', 'F_CHD' 'WhseStart', PCGR.waferLot from VALIDATION.dbo.PROCESSED_CREATE_GRPO_RW PCGR with(nolock) where PCGR.createdate >= dateadd(hour, -1, getdate())")
	cursor.execute(query)
	testresult6 = cursor.fetchone()
	
	while testresult6:
		insertSAPCreate.append(testresult6)
		testresult6 = cursor.fetchone()
		#bar.update()
	#bar.finish()
	
	#### Get Itemcode to be created from Spinweb #####
	print("Gathering ItemCode Finish part....")
	mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', port=3306, database='mtsdb')
	cursor1 = mysqlcon.cursor()
	
	try:
		for x in insertSAPCreate:
			pqty, whsf, ics, sqty, whss, wlot = x
			swquery = ("select CMOS.partID 'PW', CMOS.waferLot, CMOS.MFGPartID 'RW', CMOS.rcvdQty 'rvcdQty', TRANS.PONumber 'SAPPO', ifnull(CMOS.rcvdInvoice,'NA')'InvoiceNo' from mtsdb.tblCMOSLotInfo CMOS Left join mtsdb.tblTransitLotInfo TRANS on CMOS.partID = TRANS.deviceID and TRANS.LotID = CMOS.waferLot where CMOS.MFGPartID = '{0}' and CMOS.waferLot = '{1}' and CMOS.rcvdQty is not null".format(ics, wlot))
			cursor1.execute(swquery)
			testresult8 = cursor1.fetchone()
			if testresult8 != None:
				resultset = (testresult8[0], pqty,whsf,ics,sqty,whss,wlot)
				insertSAPCreate2.append(resultset)
			else:
				pass
			#bar.update()
	except:
		print("\nAn Error Occured in the Gathering of the Item to be Created.")
		raise
	
	#### Verify if records previously processed and if NULL insert recort into create table #####
	try:
		for x in insertSAPCreate2:
			icf, pqty, whsf, ics, sqty, whss, wlot = x
			query2 = ("Select * from VALIDATION.dbo.CREATE_PRDO_PW with(nolock) WHERE ItemCodeFinish = '{0}' and SpinwebNo = '{1}' and PlannedQty = '{2}' and ItemCodeStart = '{3}' ".format(icf, wlot, pqty, ics))
			cursor.execute(query2)
			testresult = cursor.fetchone()
			if testresult != None:
				pass
			else:
				query3 = ("Select * from VALIDATION.dbo.PROCESSED_CREATE_PRDO_PW with(nolock) WHERE ItemCodeFinish = '{0}' and SpinwebNo = '{1}' and PlannedQty = '{2}' and ItemCodeStart = '{3}' ".format(icf, wlot, pqty, ics))
				cursor.execute(query3)
				testresult7=cursor.fetchone()
				if testresult7 != None:
					pass
				else:
					query4 = ("Insert Into VALIDATION.dbo.CREATE_PRDO_PW (ItemCodeFinish, PlannedQty, WhseFinish, ItemCodeStart, StartQty, WhseStart, SpinwebNo) values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}')".format(icf, pqty, whsf, ics, sqty, whss, wlot))
					cursor.execute(query4)
			#bar.update()
#			print(x)
		
	except:
		print ("\nAn error Occured in the Insert To Create PRDO Table.")
		raise
		
	#bar.finish()
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	cursor1.close()
	mysqlcon.close()
	print ("Insert of Create PRDO Data is Complete.")
	print("\n -----{} seconds------".format(time.time()-start_time))
	
def reportCompTbl():
	print("Starting Report Complete Data insert preparation....")
	connection = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
	cursor = connection.cursor()
	
	try:
		for x in insertSAPComp:
			plot, wlot, pw, rw, whsf, sqty, shpqty, prdo = x
			query3 = ("select OWOR.DocEntry, OWOR.ItemCode, OWOR.U_SpinwebNo, Cast(abs(OWOR.CmpltQty)as int) 'Complete' from EverspinTech.dbo.OWOR with(Nolock) where OWOR.U_spinwebno = '{0}' and OWOR.status = 'R'".format(wlot))
			cursor.execute(query3)
			result = cursor.fetchone()
			if shpqty != 0:
				if result[3] <= shpqty:
#					print("will insert")
					insertSAPComp2.append(x)
				else:
					reason = (prdo, pw, shpqty, plot, "Ship Qty greater than remainder to ship.  Line 221 Query")
					manualSAP.append(reason)
			else:
				reason = (prdo, pw, shpqty, plot, "No Shipped Quantity from Spinweb.  Cannot report Complete.  Line Query 224.")
				manualSAP.append(reason)
				
			#bar.update()
		
		print("Start check for previous completion....")
		for x in insertSAPComp2:
			plot, wlot, pw, rw, whsf, sqty, shpqty, prdo = x
			query1 = ("SELECT ItemCodeFinish, PlannedQty, WhseFinish, ItemCodeStart, ShipQty, WhseStart, ParentLotNo, NewLotNo, ParentPRDO FROM VALIDATION.dbo.PROCESSED_REPORT_COMP_PW with(nolock) WHERE ParentPRDO = '{0}' and NewLotNo = '{1}' and ParentLotNo = '{2}'".format(prdo, plot, wlot))
			cursor.execute(query1)
			result = cursor.fetchone()
			if result != None:
				reason = (prdo, pw, shpqty, plot, "Already Processed in Validation and SAP.  Line 231 Query")
				manualSAP.append(reason)
			else:
				query2=("INSERT INTO VALIDATION.dbo.REPORT_COMP_PW (ItemCodeFinish, PlannedQty, WhseFinish, ItemCodeStart, ShipQty, WhseStart, ParentLotNo, NewLotNo, ParentPRDO) VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', 'F_CHD', '{5}', '{6}', '{7}')".format(pw, sqty, whsf, rw, shpqty, wlot, plot, prdo))
				cursor.execute(query2)

			#bar.update()
			
	except:
		print ("An error occurred reporting complete.  ReportComp function.")
		raise
#	print ("Sample: {0} ".format(insertSAP[0]))
	
	#bar.finish()
	cursor.commit()
	cursor.close()
	connection.close()
	
	
	print ("Insert of Report Complete Data is Complete.")
	print("\n -----{} seconds------".format(time.time()-start_time))
	
def manualEntry():
	print("Starting Error Report Insertion...")
	
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))			
	cursor = sapsqlcon.cursor()
		
	try:
		if manualSAP != False:
			for x in manualSAP:
				prdo, ic, qty, lot, err = x
				query1=("Select * from VALIDATION.dbo.ERROR_ENTRY_PW with(nolock) where ItemCode = '{0}' and LotNumber = '{1}' and ErrorReason = '{2}'".format(ic,lot,err))
				cursor.execute(query1)
				result = cursor.fetchone()
				if result != None:
					pass
				else:
					query2= ("insert into VALIDATION.dbo.ERROR_ENTRY_PW (ParentPRDO, Itemcode, Quantity, LotNumber, ErrorReason) values ('{0}', '{1}', '{2}', '{3}', '{4}')".format(prdo, ic, qty, lot, err))
					cursor.execute(query2)
				#bar.update()
		else:
			pass
	except:
		print ("An error occured finding ABI Number.")
		raise
#	print ("Sample: {0} ".format(insertSAPman2[0]))
	#bar.finish()
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	
	print ("Insert Error Report Information Complete.")
	print("\n -----{} seconds------".format(time.time()-start_time))
	
###### Program Start #######
dataGather()
dataParse()
createPRDOTbl()
reportCompTbl()
manualEntry()