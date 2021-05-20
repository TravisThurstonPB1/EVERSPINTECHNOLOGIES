import sys
import pypyodbc
import csv
import pymysql
import re 
import itertools
import time
import mysqllogin

start_time = time.time()


preimportEVS = []
preimportEVS1 = []
insertSAPCreate = []
insertSAPCreate2 = []
insertSAPCreate3 = []
insertSAPComp = []
insertSAPComp2 = []
insertSAPComp3 = []
manualSAP = []
scrapList = {}
parse1 = []
parse2 = []
parse3 = []



def dataGather():
	print ("Starting Data Gathering...")
	mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', port=3306, database='mtsdb')
	cursor1 = mysqlcon.cursor()
	query = ("select distinct PROM.partID, CASE WHEN PROM.partID in ('WC01N10C-ENG', 'WB01N10C') THEN 'E_GTC' WHEN PROM.partID = 'WB06M35M' THEN 'F_CHD' ELSE 'E_CHD' End as 'WhsStart', PROM.waferLot, ifnull(S0.labLot,'0') 'ShipLot', ifnull(MRAM.promisLot,'NA') 'EWS Lot Start', ifnull(MRAM.currentQty,0) 'EWS Qty', 'E_CHD' as'EWSLocation', ifnull(MRAM.currentStage,'NA') 'currentStage', ifnull(MRAM.lotStatus,'NA') 'lotStatus', CASE WHEN MRAM.lotStatus = 'Scrapped' then 'Scrapped' else ifnull(MRAM.labLot,'NotShip') END as 'Ship Lot', CASE WHEN MRAM.lotStatus = 'Scrapped' then 'Scrapped' else cast(ifnull(S0.shipDieCnt,'0')as int) END as 'Ship Die Count', CASE WHEN MRAM.lotStatus = 'Scrapped' then 'Scrapped' When ifnull(WAF.shipToLoc,'')='' then 'E_CHD' else concat(\"A_\", ifnull(WAF.shipToLoc,'NotShip')) END as 'Ship To Loc' from mtsdb.tblPromisLotInfo PROM inner join mtsdb.tblCMOSLotInfo CMOS on PROM.waferLot = CMOS.waferLot Left join mtsdb.tblMRAMLabLotInfo MRAM on PROM.promisLot = MRAM.promisLot left join (Select MRAM2.labLot, Case When sum(SHIP.shipDieCnt) = 0 then sum(SHIP.repairBin1Final)+sum(SHIP.repairBin2Final) else sum(SHIP.shipDieCnt) end 'shipDieCnt' from mtsdb.tblWaferProbeRecord SHIP inner join mtsdb.tblMRAMLabLotInfo MRAM2 on SHIP.promisLot = MRAM2.promisLot where FIND_IN_SET(SHIP.Wafer,(Select T0.waferlist from mtsdb.tblMRAMLabLotInfo T0 where T0.labLot = MRAM2.labLot))Group by MRAM2.labLot) S0 on MRAM.labLot = S0.labLot Left join mtsdb.tblWaferLotInfo WAF on MRAM.labLot = WAF.shipLot where PROM.partID != 'WB01N10C' and right(PROM.partID,3) != 'ENG' and PROM.lotStartDate >= date_add(curdate(), interval -720 day) and ifnull(PROM.shipQty,0)!=0 and MRAM.returnToFab != 2 ")
	cursor1.execute(query)
	results = cursor1.fetchone()

	
	while results:
		preimportEVS.append(results)
		results = cursor1.fetchone()


	print ("Spinweb Test data gathering complete.")
	
	# for x in preimportEVS:
		# print (x)
	# input()
	
	print("\n -----{} seconds------".format(time.time()-start_time))
	
	cursor1.close()
	mysqlcon.close()

	
def dataParse():
	print ("Starting Data Parse...")
	connection = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
	cursor = connection.cursor()
	
	# -- Parse1 List is for Items that should be created.  Parse2 list is for Items that should be reported complete. -- #
	
	try:
		for x in preimportEVS:
			item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs = x
			if stage.lower() == 'finished wafer inventory':
				if lotstat.lower() == 'ready for shipment':
					parse2.append(x)
			elif stage.lower() == 'lot disposition':
				if lotstat.lower() == 'in progress':
					parse1.append(x)
				elif lotstat.lower() == 'hold qa':
					parse1.append(x)
				elif lotstat.lower() == 'hold mrb':
					parse1.append(x)
				elif lotstat.lower() == 'hold others':
					parse1.append(x)
				elif lotstat.lower() == 'retest request':
					parse1.append(x)
				elif lotstat.lower() == 'retest in progress':
					parse1.append(x)
				elif lotstat.lower() == 'retest hold':
					parse1.append(x)
				elif lotstat.lower() == 'retest complete':
					if diecount != '0':
						parse2.append(x)  ## May need to adjust this to be not compared to completed parts.
					else:
						parse1.append(x)
				elif lotstat.lower() == 'disposition complete':
					if diecount != '0':
						parse2.append(x)
					else:
						parse1.append(x)
				elif lotstat.lower() == 'physical moved':
					if diecount != '0':
						parse2.append(x)
					else:
						parse1.append(x)
			elif stage.lower() == 'shipped':
				if diecount != '0':
					parse2.append(x)
				else:
					parse1.append(x)
			elif stage.lower() == 'tested wafer':
				if lotstat.lower() == 'wait for oqa':
					if diecount != '0':
						parse2.append(x)
					else:
						parse1.append(x)
				elif lotstat.lower() == 'oqa hold':
					if diecount != '0':
						parse2.append(x)
					else:
						parse1.append(x)
				elif lotstat.lower() == 'pass oqa':
					if diecount != '0':
						parse2.append(x)
					else:
						parse1.append(x)
				elif lotstat.lower() == 'oqa waived':
					if diecount != '0':
						parse2.append(x)
					else:
						parse1.append(x)
				elif lotstat.lower() == 'wait for pe disposition':
					parse1.append(x)
				elif lotstat.lower() == 'wait for qa signoff':
					if diecount != '0':
						parse2.append(x)
					else:
						parse1.append(x)
				elif lotstat.lower() == 'wait for mrb':
					if diecount != '0':
						parse2.append(x)
					else:
						parse1.append(x)
			elif stage.lower == 'wafer probe':
				if lotstat.lower() == 'wait for test':
					parse1.append(x)
				elif lotstat.lower() == 'in progress':
					if diecount != '0':
						parse2.append(x)
					else:
						parse1.append(x)
				elif lotstat.lower() == 'hold':
					if diecount != '0':
						parse2.append(x)
					else:
						parse1.append(x)
				elif lotstat.lower() == 'swp complete':
					parse1.append(x)
				elif lotstat.lower() == 'ready for shipment':
					if diecount != '0':
						parse2.append(x)
					else:
						parse1.append(x)
				elif lotstat.lower() == 'outside process':
					parse1.append(x)
				elif lotstat.lower() == 'sample complete':
					parse1.append(x)
				elif lotstat.lower() == 'test complete':
					if diecount != '0':
						parse2.append(x)
					else:
						parse1.append(x)
			elif stage.lower() == 'scrapped':
				scrapList[ewslot] = int(ewsqty)
				#loss = (ewslot, ewsqty)
				#scrapList.append(loss)
			else:
				parse1.append(x)
				
		# print(parse1, "\n")
		# print(parse2, "\n")
# #		print(scrapList, "\n")
		# input() 
		
		for x in parse1:
			item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs = x
			query = ("Select Ibt1.ItemCode, ibt1.Batchnum, ibt1.BsDocEntry from EverspinTech.dbo.IBT1 with(nolock) where (select OITT.code from EverspinTech.dbo.OITT with(nolock) inner join EverspinTech.dbo.ITT1 with(nolock) on ITT1.Father = OITT.code where ITT1.Code = '{0}') = IBT1.ItemCode and BatchNum = '{1}' and Bsdoctype = 202 and BaseType =59".format(item,ewslot))
			cursor.execute(query)
			results = cursor.fetchone()
			# print("results line 78", results)
			if results != None:
				pass
				# reason = (results[2], results[0], diecount, ewslot, 'EWS Lot has already been processed.   Line 196', '0')
				# manualSAP.append(reason)
			else:
				query1=("select ItemCode, Batchnum, BsDocEntry, OITT.Code, cast(OITT.Qauntity as int) 'Quantity' from EverspinTech.dbo.IBT1 with(nolock) LEFT JOIN (select oitt.Code, Itt1.Code 'CompItem', OITT.Qauntity from EverspinTech.dbo.oitt with(nolock) inner join EverspinTech.dbo.ITT1 with(nolock) on oitt.code = itt1.father) OITT on IBT1.ItemCode = OITT.CompItem where batchnum = '{0}' and ItemCode = '{1}' and BaseType = 60".format(ewslot,item))
				cursor.execute(query1)
				results2 = cursor.fetchone()
				# print("results2 line 86", results2)
				if results2 != None:
					reason = (results2[2], item, ewsqty, plot, 'Lot has been issued in WIP, status is '+stage+' & '+lotstat+'.  Quantity is '+diecount+'.  line 204', '0')
					manualSAP.append(reason)
				else:
					query2=("select ItemCode, Batchnum, BsDocEntry, OITT.Code, cast(OITT.Qauntity as int) 'Quantity' from EverspinTech.dbo.IBT1 with(nolock) LEFT JOIN (select oitt.Code, Itt1.Code 'CompItem', OITT.Qauntity from EverspinTech.dbo.oitt with(nolock) inner join EverspinTech.dbo.ITT1 with(nolock) on oitt.code = itt1.father) OITT on IBT1.ItemCode = OITT.CompItem where batchnum = '{0}' and ItemCode = '{1}' and BaseType in (59,20)".format(ewslot,item))
					cursor.execute(query2)
					results3 = cursor.fetchone()
					# print(results3)
					if results3 != None:
						verify = (results3[3], item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs)
						insertSAPCreate.append(verify)
					else:
						reason=('0',item,ewsqty,ewslot,'Lot does not exist in SAP. Line 220','0')
						manualSAP.append(reason)
		
		for x in parse2:
			# print(x)
			item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs = x
			query = ("Select Ibt1.ItemCode, ibt1.Batchnum, ibt1.BsDocEntry from EverspinTech.dbo.IBT1 with(nolock) where (select OITT.code from EverspinTech.dbo.OITT with(nolock) inner join EverspinTech.dbo.ITT1 with(nolock) on ITT1.Father = OITT.code where ITT1.Code = '{0}') = IBT1.ItemCode and BatchNum = '{1}' and Bsdoctype = 202 and BaseType =59".format(item,shiplot))
			cursor.execute(query)
			results = cursor.fetchone()
			# print("results line 99", results)
			if results != None:
				pass
				# reason = (results[2], results[0], diecount, ewslot, 'EWS Lot has already been processed.   Line 227', '0')
				# manualSAP.append(reason)
			else:
				query1=("select ItemCode, Batchnum, BsDocEntry, OITT.Code, cast(OITT.Qauntity as int) 'Quantity' from EverspinTech.dbo.IBT1 with(nolock) LEFT JOIN (select oitt.Code, Itt1.Code 'CompItem', OITT.Qauntity from EverspinTech.dbo.oitt with(nolock) inner join EverspinTech.dbo.ITT1 with(nolock) on oitt.code = itt1.father) OITT on IBT1.ItemCode = OITT.CompItem where batchnum = '{0}' and ItemCode = '{1}' and BaseType = 60 and BsDocType=202 Order by BsDocEntry DESC".format(ewslot,item))
				cursor.execute(query1)
				results2 = cursor.fetchone()
				# print("results2 line 107", results2)
				if results2 != None:
					query2 = ("select OWOR.Status, OWOR.DocNum from EverspinTech.dbo.OWOR with(nolock) where DocNum = {0}".format(results2[2]))
					cursor.execute(query2)
					results3 = cursor.fetchone()
					# print("results3 line 112", results3)
					if results3[0] == 'L':
						pass
						# reason = (results3[1], item, ewsqty, plot, 'EWS Lot is associated to closed production order. Line 240','0')
						# manualSAP.append(reason)
					else:
						verify = (results2[3], results2[4], results2[2], item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs)
						insertSAPComp.append(verify)
					
				else:
					query3 = ("select ItemCode, Batchnum, BsDocEntry, OITT.Code, cast(OITT.Qauntity as int) 'Quantity' from EverspinTech.dbo.IBT1 with(nolock) LEFT JOIN (select oitt.Code, Itt1.Code 'CompItem', OITT.Qauntity from EverspinTech.dbo.oitt with(nolock) inner join EverspinTech.dbo.ITT1 with(nolock) on oitt.code = itt1.father) OITT on IBT1.ItemCode = OITT.CompItem where batchnum = '{0}' and ItemCode = '{1}' and BaseType in (59,20)".format(ewslot,item))
					cursor.execute(query3)
					result4 = cursor.fetchone()
					# print("result4 line 123", result4)
					if result4 != None:
						verify = (result4[3], item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs)
						insertSAPCreate.append(verify)
					else:
						reason=('0',item,ewsqty,plot,'Lot does not exist in SAP. Line 255','0')
						manualSAP.append(reason)
	except:
		print ("An error Occurred with Parsing the Data.")
		raise

	# print(manualSAP, "\n")
	# print(insertSAPComp, "\n")
	# print(insertSAPCreate, "\n")
	# input()
	
	print("Data Parsing Complete.")
	
	print("\n -----{} seconds------".format(time.time()-start_time))
	
	cursor.close()
	connection.close()

def createPRDOTbl ():
	print("Starting PRDO Create Data insert preparation....")
	connection = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
	cursor = connection.cursor()
		
		
	try:
		if insertSAPCreate != False:
			for x in insertSAPCreate:
				finitem, item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs = x
				lotcheck = scrapList.get(ewslot)
				if lotcheck != None: 
					eqty = scrapList[ewslot]
					updateqty = int(eqty) + int(ewsqty)
#					print(updateqty)
					verify = (finitem, item, WhsSt, wlot, plot, ewslot, updateqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs)
					insertSAPCreate2.append(verify)
					del scrapList[ewslot]
				else:
#					print("Not in scraplist :", x)
					insertSAPCreate2.append(x)
			
			print ("Insert Information Preparation for Create PRDO Table Complete\n")
			
			print ("Start Create PRDO Data insert....")
				
			for x in insertSAPCreate2:
				finitem, item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs = x
				query = ("Select OITT.Code 'Parent', ITT1.Code 'Child', cast(OITT.Qauntity as int) 'Quantity' from EverspinTech.dbo.OITT with(nolock) inner join EverspinTech.dbo.ITT1 with(nolock) on OITT.Code = ITT1.Father where ITT1.Code = '{0}'".format(item))
				cursor.execute(query)
				result = cursor.fetchone()
				#print(item, wlot, result)
				#input()
				if result != None:
					planqty = ewsqty * result[2]
					verify = (finitem, planqty, item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs)
					insertSAPCreate3.append(verify)
				else:
					reason = ('0',item,ewsqty,plot,'Component Item does not belong to a BOM in SAP. Line 194','0')
					manualSAP.append(reason)

				
			for x in insertSAPCreate3:
				finitem, planqty, item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs = x
				query1 = ("Select * from VALIDATION.dbo.PROCESSED_CREATE_PRDO_EWS Where ItemCodeFinish = '{0}' and SpinwebABI = '{1}' and StartQty = '{2}'".format(finitem, ewslot, ewsqty))
				cursor.execute(query1)
				result = cursor.fetchone()
				if result == None:
					query2 = ("Select * from VALIDATION.dbo.CREATE_PRDO_EWS Where ItemCodeFinish = '{0}' and SpinwebABI = '{1}' and StartQty = '{2}'".format(finitem, ewslot, ewsqty))
					cursor.execute(query2)
					result2 = cursor.fetchone()
					if result2 == None:
						query3 = ("Insert Into VALIDATION.dbo.CREATE_PRDO_EWS (SpinwebABI, SAPPONo, ItemCodeFinish, PlannedQty, WhseFinish, ItemCodeStart, StartQty, WhseStart, ParentLotNo) values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}')".format(ewslot, '0', finitem, planqty, ewswhs, item, ewsqty, WhsSt, ewslot))
						cursor.execute(query3)
					else:
						pass
				else:
					pass
					
		else:
			pass
	except:
		print ("An error occured Inserting records Number. Line 160")
		raise

	
	cursor.commit()
	cursor.close()
	connection.close()
	print ("Insert of Create PRDO Data is Complete.")
	
	print("\n -----{} seconds------".format(time.time()-start_time))
	
def reportCompTbl():
	print("Starting Report Complete Data insert preparation....")
	connection = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
	cursor = connection.cursor()
	
	try:
		if insertSAPComp != False:
			for x in insertSAPComp:
				finitem, planqty, prdo, item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs = x
				lotcheck = scrapList.get(ewslot)
				if lotcheck != None:
					eqty = scrapList[ewslot]
					verify = (finitem, planqty, prdo, item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs, eqty)
					insertSAPComp2.append(verify)
					del scrapList[ewslot]
				else:
					verify=(finitem, planqty, prdo, item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs, '0')
					insertSAPComp2.append(verify)
		
			print("Finish Report Complete Preparation.\n")
			
			print("Start Report Complete Insertion....")
			
			for x in insertSAPComp2:
				finitem, planqty, prdo, item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs, scrap = x
				closeprdo = int(ewsqty) + int(scrap)
				if closeprdo == 25:
					verify = (finitem, planqty, prdo, item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs, scrap, 'Y')
					insertSAPComp3.append(verify)
				else:
					verify = (finitem, planqty, prdo, item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs, scrap, 'N')
					insertSAPComp3.append(verify)
				
			for x in insertSAPComp3:
				finitem, planqty, prdo, item, WhsSt, wlot, plot, ewslot, ewsqty, ewswhs, stage, lotstat, shiplot, diecount, shiptowhs, scrap, closeprdo = x
				query = ("Select * from VALIDATION.dbo.PROCESSED_REPORT_COMP_EWS where ItemCodeFinish = '{0}' and Quantity = '{1}' and NewLotNo = '{2}'".format(finitem, diecount, shiplot))
				cursor.execute(query)
				result = cursor.fetchone()
				if result == None:
					query1 = ("Select * from VALIDATION.dbo.REPORT_COMP_EWS where ItemCodeFinish = '{0}' and Quantity ='{1}' and NewLotNo = '{2}'".format(finitem, diecount, shiplot))
					cursor.execute(query1)
					result2 = cursor.fetchone()
					if result2 == None:
						query2 = ("Insert into VALIDATION.dbo.REPORT_COMP_EWS (SpinwebABI, SAPPRDONo, CompletionType, Quantity, ParentLotNo, NewLotNo, WhseFinish, ItemCodeStart, ItemCodeFinish, ScrapQty, closePRDO) values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}')".format(ewslot, prdo, 'Complete', diecount, ewslot, shiplot, shiptowhs, item, finitem, scrap, closeprdo))
						cursor.execute(query2)
					else:
						pass
				else:
					pass
		else:
			pass
	except:
		print ("An error occured finding ABI Number.")
		raise

	cursor.commit()
	cursor.close()
	connection.close()
	print ("Insert of Report Complete Data is Complete.")
	
	print("\n -----{} seconds------".format(time.time()-start_time))
	
def manualEntry():
	print("Starting Error Report Insertion...")
	connection = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
	cursor = connection.cursor()
		
	try:
		if manualSAP != False:
			for x in manualSAP:
				prdo, item, qty, lot, error, sourceid = x
				query = ("Select * from VALIDATION.dbo.ERROR_ENTRY_EWS where ItemCode = '{0}' and LotNumber = '{1}' and ErrorReason = '{2}' and Quantity = '{3}'".format(item, lot, error, qty))
				cursor.execute(query)
				result = cursor.fetchone()
				if result == None:
					query1= ("insert into VALIDATION.dbo.ERROR_ENTRY_EWS (PRDO, ItemCode, Quantity, LotNumber, ErrorReason, SourceTransID) values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}')".format(prdo, item, qty, lot, error, sourceid))
					cursor.execute(query1)
				else:
					pass

		else:
			pass
	except:
		print ("An error occured finding ABI Number.")
		raise

	cursor.commit()
	cursor.close()
	connection.close()
	print ("Insert Error Report Information Complete.")
	
	print("\n -----{} seconds------".format(time.time()-start_time))
	
###### Program Start #######
dataGather()
dataParse()
createPRDOTbl()
reportCompTbl()
manualEntry()