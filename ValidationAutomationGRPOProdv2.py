import sys
import pypyodbc
import csv
import pymysql
import re 
import itertools
import time
#import progress#bar
import mysqllogin

start_time=time.time()
#bar = progress#bar.Progress#bar(max_value=progress#bar.UnknownLength)
#timestr = time.strftime(".%Y.%m.%d-%H_%M_%S")

preimportEVS = []
preimportEVS1 = []
insertSAPCreate = []
insertSAPCreate2 = []
insertSAPComp = []
insertSAPComp2 = []
insertSAPComp3 = []
parselist = []
parselist2 = []
manualSAP = []


###### Get Data from SpinWeb and append to list ########
def dataGather():
	print ("\n Starting Data Gathering...")
	mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', database='mtsdb')
	cursor1 = mysqlcon.cursor()
	mySQLcomVT1 = ("select CMOS.partID 'PW', CMOS.waferLot, CMOS.MFGPartID 'RW', abs(sum(CMOS.rcvdQty)) 'rcvdQty', CMOS.rcvdPO 'SAPPO', ifnull(CMOS.rcvdInvoice,'NA') 'InvoiceNo' from mtsdb.tblCMOSLotInfo CMOS Left join mtsdb.tblTransitLotInfo TRANS on CMOS.partID = TRANS.deviceID and TRANS.LotID = CMOS.waferLot where CMOS.rcvdDate >= date_add(curdate(), interval -360 day) and CMOS.rcvdQty is not null and ifnull(CMOS.rcvdPO,'NA')<>'NA' group by CMOS.partID, CMOS.MFGPartID, CMOS.rcvdPO, CMOS.rcvdInvoice, CMOS.waferLot")
	cursor1.execute(mySQLcomVT1)
	results = cursor1.fetchone()

	while results:
		verify = (results[0],results[1], results[2],float(results[3]), results[4], results[5])
		preimportEVS.append(verify)
		results = cursor1.fetchone()
		#bar.update()
	#bar.finish()
#	print (len(preimportEVS))
#	print(" Unfiltered list")
	# for x in preimportEVS:
		# print(x)
		
#	print(" Set list")
#	filter = set(preimportEVS)
#	for x in (filter):
#		print(x)
#	input()
	print ("\n Spinweb Test data gathering complete.")
	print("\n -----{} seconds------".format(time.time()-start_time))
	cursor1.close()
	mysqlcon.close()

###### Take List of Spinweb data and validate against existing data to determine what should be done with the data #########	
def dataParse():
	print ("\n Starting Data Parse...")
	connection = pypyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
	cursor = connection.cursor()
	
	try:
#		for x in preimportEVS:
#			print(x)
#		input()	
#		print("\n Checking available amounts on hand...")	
#		for x in preimportEVS:
#			pw, wlot, rw, qty, ponum, inv = x
#			test4 = ("select OPor.docnum, POR1.Objtype, por1.Itemcode, cast(sum(isnull(por1.Openqty,0)) as Int)'OpenQty' from EverspinTech.dbo.OPOR inner join EverspinTech.dbo.POR1 on OPOR.docentry = POR1.Docentry where OPOR.DocNum = '{0}' and POR1.ItemCode = '{1}' and POR1.LineStatus <> 'C' Group by OPOR.DocNum, POR1.ObjType, POR1.ItemCode".format(ponum, rw))
#			cursor.execute(test4)
#			testresult4 = cursor.fetchone()
#			print("\n", testresult4)
#			print(" First Pass.")
#			input()
#			if testresult4 != None:
#				if testresult4[3] >= qty:
#					print("\n", x)
#					print(" Good to receive.")
#					parselist.append(x)
#				else:
#					print(x)
#					print(testresult4)
#					reason = (ponum, rw, 'NA', qty, "Open Quantity is not enough to receive wafer lot(s).  Open Quantity is " + str(testresult4[3]), '0', inv)
#					print(reason)
#					manualSAP.append(reason)
#			else:
#				print(x, " Returned None Result, first pass")
#				pass
#			#bar.update()
			
#		print("Retrieving Lot numbers...")	
#		for x in parselist:
#			mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', database='mtsdb')
#			cursor1 = mysqlcon.cursor()
#			pw, rw, qty, ponum, inv = x
#			test5 = ("select CMOS.partID 'PW', CMOS.waferLot, CMOS.MFGPartID 'RW', CMOS.rcvdQty 'rvcdQty', TRANS.PONumber 'SAPPO', ifnull(CMOS.rcvdInvoice,'NA')'InvoiceNo' from mtsdb.tblCMOSLotInfo CMOS Left join mtsdb.tblTransitLotInfo TRANS on CMOS.partID = TRANS.deviceID and TRANS.LotID = CMOS.waferLot where CMOS.partID = '{0}' and CMOS.MFGPartID = '{1}' and TRANS.PONumber = '{2}' and TRANS.startDate >= date_add(curdate(), interval -120 day) and CMOS.rcvdQty is not null".format(pw, rw, ponum))
#			cursor1.execute(test5)
#			testresult5 = cursor1.fetchone()
#			while testresult5:
#				parselist2.append(testresult5)
#				testresult5=cursor1.fetchone()
#				#bar.update()			
			
			
#		print("Verifying Lot numbers...")
#		set(parselist2)
#		print("Length before duplicate removal: ", len(parselist2))
#		print("Length without duplicates: ", len(set(parselist2)))
#		input()
#		for x in (set(parselist2)):
#			print(x)
#		input()
		for x in preimportEVS:
			pw, wlot, rw, qty, ponum, inv = x
			test1 = ("Select T2.DistNumber, T1.BaseEntry, T1.DocEntry,  CAST(SUM(T0.Quantity) as int) from EverspinTech.dbo.ITL1 T0 with(nolock) INNER JOIN EverspinTech.dbo.OITL T1 with(nolock) on T0.LogEntry = T1.LogEntry and T1.StockEff =1 INNER JOIN EverspinTech.dbo.OBTN T2 with(nolock) on T0.ItemCode = T2.ItemCode and T0.SysNumber = T2.SysNumber Where T2.DistNumber = '{0}' and T1.DocType in ('18','20') Group By t2.DistNumber, T1.DocEntry, T1.BaseEntry".format(wlot))
			cursor.execute(test1)
			testresult = cursor.fetchone()
#			print(x)
			if testresult != None:
				test2 = ("Select OPDN.DocNum FROM EverspinTech.dbo.OPDN WITH(NOLOCK) where OPDN.DocEntry = '{0}'".format(testresult[2]))
				cursor.execute(test2)
				testresult2 = cursor.fetchone()
#				print(testresult2)
				if testresult2 != None:
					pass
					# reason = (ponum, rw, wlot, qty, "Already Received via GRPO.  Verify.", testresult2[0], inv)
					# manualSAP.append(reason)
				else:
					test3 = ("Select OPCH.DocNum from EverspinTech.dbo.OPCH WITH(NOLOCK) where OPCH.DocEntry = '{0}'".format(testresult[2]))
					cursor.execute(test3)
					testresult3 = cursor.fetchone()
					if testresult3 != None:
						pass
						# reason = (ponum, rw, wlot, qty, "Already Received via Invoice.  Verify.", testresult3[0], inv)
						# manualSAP.append(reason)
					else:
#						print(testresult2, " Test 3 was a None result")
						pass
			else:
#				print(x, "Line 139")
				if ponum == 'NIL ':
					if ponum == None:
						reason = ('0', rw, wlot, qty, "PO Number is missing in Spinweb.  Line 142", '0', inv)
						manualSAP.append(reason)
					else:
						reason = ('0', rw, wlot, qty, "PO Number is NIL in Spinweb.  Line 145", '0', inv)
						manualSAP.append(reason)
				else:
					test4 = ("select OPor.docnum, POR1.Objtype, por1.Itemcode, cast(sum(isnull(por1.Openqty,0)) as Int)'OpenQty' from EverspinTech.dbo.OPOR WITH(NOLOCK) inner join EverspinTech.dbo.POR1 WITH(NOLOCK) on OPOR.docentry = POR1.Docentry where OPOR.DocNum = '{0}' and POR1.ItemCode = '{1}' and POR1.LineStatus <> 'C' Group by OPOR.DocNum, POR1.ObjType, POR1.ItemCode".format(ponum, rw))
					cursor.execute(test4)
					testresult4 = cursor.fetchone()
					# print("\n", testresult4)
					# print(" First Pass.")
#				input()
					if testresult4 != None:
						if testresult4[3] >= qty:
							# print("\n", x)
							# print(" Good to receive.")
							insertSAPCreate.append(x)
						else:
	#						print(x)
	#						print(testresult4)
							reason = (ponum, rw, wlot, qty, "Open Quantity is not enough to receive wafer lot(s).  Open Quantity is " + str(testresult4[3]), '0', inv)
							# print(reason)
							manualSAP.append(reason)
					else:
						# print(x, " Returned None Result, first pass")
						pass
			#bar.update()
#				print("create")
#				insertSAPCreate.append(x)
				

		

	except:
		print ("An error Occured with Parsing the Data.")
		raise
	#bar.finish()
#	print("Error Entries\n", len(manualSAP), "\n", manualSAP, "\n")
#	print("Insert Entries\n", insertSAPCreate, "\n")
#	print("Error Entries\n", manualSAP)
#	print("GRPO Entries\n", len(insertSAPCreate), "\n", insertSAPCreate, "\n")
#	input()
	print("Data Parsing Complete.")
	print("\n -----{} seconds------".format(time.time()-start_time))

def createGRPOTbl ():
	print("Starting Insert of PRDO Create Data....")
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))			
	cursor = sapsqlcon.cursor()
	try:
		for x in insertSAPCreate:
			pw, wlot, rw, qty, ponum, inv = x
			query = ("Select * from VALIDATION.dbo.CREATE_GRPO_RW WITH(NOLOCK) WHERE PONum = '{0}' and ItemCode = '{1}' and waferLot = '{2}' and QtyToRecv = '{3}' ".format(ponum, rw, wlot, qty))
			cursor.execute(query)
			testresult = cursor.fetchone()
			if testresult != None:
				pass
			else:
				query2 = ("Insert Into VALIDATION.dbo.CREATE_GRPO_RW (PONum, ItemCode, QtyToRecv, waferLot, invNum, WhseCode) values ('{0}', '{1}', '{2}', '{3}', '{4}', 'F_CHD')".format(ponum, rw, qty, wlot, inv))
				cursor.execute(query2)
			#bar.update()
#			print(x)
		
	except:
		print ("\nAn error Occured in the Insert To Create PRDO Table.")
		raise
		
	#bar.finish()
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	print ("Insert of Create PRDO Data is Complete.")
	print("\n -----{} seconds------".format(time.time()-start_time))
	
def manualEntry():
	print("Starting Error Report Insertion...")
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))			
	cursor = sapsqlcon.cursor()
	try:
		for x in manualSAP:
			ponum, rw, wlot, qty, err, grpo, inv = x
			query = ("select * from VALIDATION.dbo.PROCESSED_ERROR_ENTRY_GRPO WITH(NOLOCK) WHERE PONum = '{0}' and ItemCode = '{1}' and LotNumber = '{2}' and Quantity = '{3}' and ErrorReason = '{4}' and GRPONum = '{5}' and invNum = '{6}'".format(ponum, rw, wlot, qty, err, grpo, inv))
			cursor.execute(query)
			result = cursor.fetchone()
			if result != None:
				pass
			else:
				query1= ("insert into VALIDATION.dbo.ERROR_ENTRY_GRPO (PONum, ItemCode, LotNumber, Quantity, ErrorReason, GRPONum, invNum) values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}')".format(ponum, rw, wlot, qty, err, grpo, inv))
				cursor.execute(query1)
				#bar.update()

	except:
		print ("An error occured In Processing Error Entry Information.")
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
createGRPOTbl()
manualEntry()
