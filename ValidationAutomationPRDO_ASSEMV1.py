import sys
import pypyodbc
import csv
import pymysql
import re 
import itertools
import time
import mysqllogin

start_time=time.time()

preimportEVS = []
preimportEVS1 = []
insertSAPCreate = []
insertSAPCreate2 = []
insertSAPComp = []
insertSAPComp2 = []
insertSAPComp3 = []
insertSAPman1 = []
insertSAPman2 = []
manualSAP = []
unique = []
parse1 = []
parse2 = []
parse3 = []
parse4 = []




def dataGather():
	print ("Starting Data Gathering...")
	mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', port=3306, database='mtsdb')
	cursor = mysqlcon.cursor()
	#----- Added Scrap quantity calculation to main Query on 5/13/2021 -----#
	mySQLcomVT1 = ("""Select T2.ABI 'ABI'
				, IFNULL(T1.AssyLot,'') 'AssyLot'
				, CASE WHEN ifnull(T2.PONumber,'') = '' THEN 0 ELSE T2.PONumber END as 'PONumber'
                , T2.waferLot
				, T2.deviceName
				, ifnull(T1.assyTraceCode, '0') 'traceCode'
				, T2.ABIType
				, concat("A_", T2.subconName) 'WhsStart'
				, 'T_UTC' as 'WhsFinish'
				, CAST(ifnull(T2.dieQty,0) as int) 'StartQty'
                , CAST(IFNULL(T1.assyinQty,0) as int) 'AssemStart'
                , ifnull(T1.shipQty, 0) 'shipQty'
                , CAST(IFNULL(ifnull(T3.dieQty,'0')/IFNULL(count(T4.waferLotAll),1),'0') as int) 'Scrap' 

                from mtsdb.tblABILog T2 
                left join mtsdb.tblAssyLotInfo T1  on Case when T1.ABI like 'EABI%' then left(replace(replace(T1.ABI,'_',''),'-',''),18) else left(replace(replace(T1.ABI,'_',''),'-',''),17) end = replace(T2.ABI,'_','') 
                left join mtsdb.tblABILog T3 on T2.waferLot = T3.waferLot and T3.lotRelease = 'Scrap' 
                LEFT JOIN mtsdb.tblAssyLotInfo T4 on T1.waferLotAll = T4.waferLotAll 

                where T2.issueDate >= date_add(curdate(), INTERVAL -120 DAY) and IFNULL(T2.ABI,'') not like 'EABI%' and T2.PONumber != '1' and T2.lotRelease != 'Scrap'

                Group by T1.ABI, T1.AssyLot, T2.PoNumber, T1.waferLotAll, T1.assyPartNumber
                , T1.assyTraceCode, T1.lotType, T1.assyLocation, T2.dieQty, T1.assyinQty, T1.shipQty, T3.dieQty, T2.deviceName""")
	cursor.execute(mySQLcomVT1)
	results = cursor.fetchone()

	while results:
		preimportEVS.append(results)
		results = cursor.fetchone()

	print("Finished initial Gather, starting filter...")
	try:
		for x in preimportEVS:
			abi, assylot, ponum, waferlot, finitem, tracecode, lottype, whsstart, whsfinish, startqty, assystqty, shipqty, scrpqty = x	
			if ponum == '0':
				ponumupdate = re.findall("_(.*?)_",abi)
				verify = (abi, assylot, ponumupdate[0], waferlot, finitem, tracecode, lottype, whsstart, whsfinish, startqty, assystqty, shipqty, scrpqty)
				preimportEVS1.append(verify)
			elif finitem.endswith('-ASY'):
				preimportEVS1.append(x)
			elif finitem.startswith('EMD'):
				preimportEVS1.append(x)
			else:
				reason = (abi, ponum, finitem, assylot, shipqty, "Assy Part does not end in -ASY, Cannot Create PRDO. Line 49", '0', '0')
				manualSAP.append(reason)

	except:
		print("Error in Gathering Data")
		raise

#	for x in preimportEVS1:
#		print (x)
#	print (manualSAP)
#	input()
	print ("Spinweb Test data gathering complete.")
	
	print("\n -----{} seconds------".format(time.time()-start_time))
	
	cursor.close()
	mysqlcon.close()

	
def dataParse():
    print ("Starting Data Parse...")
    connection = pypyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor = connection.cursor()

    try:
        for x in preimportEVS1:
        # print(x)
            abi, assylot, ponum, waferlot, finitem, tracecode, lottype, whsstart, whsfinish, startqty, assystqty, shipqty, scrpqty  = x
            test6 = ("select oitt.code, Itt1.Code 'CompItem' from EverspinTech.dbo.oitt with(nolock) inner join EverspinTech.dbo.ITT1 with(nolock) on oitt.code = itt1.father where OITT.code = '{0}'".format(finitem))
            cursor.execute(test6)
            testresult6 = cursor.fetchone()
            if testresult6 != None:
                verify = (abi, assylot, ponum, waferlot, testresult6[1], finitem, tracecode, lottype, whsstart, whsfinish, startqty, assystqty, shipqty, scrpqty)
                parse1.append(verify)
            else:
                pass

    #-- Checking for Report Complete --#			
        for x in parse1:
            abi, assylot, ponum, waferlot, ewsitem, finitem, tracecode, lottype, whsstart, whsfinish, startqty, assystqty, shipqty, scrpqty = x

            test2 = ("""Select T3.DocNum, T3.U_SpinwebNo, T3.U_PONum, T3.ItemCode, T3.PlannedQty, T0.Quantity, T2.DistNumber, T3.Status

                    From EverspinTech.dbo.ITL1 T0 with(nolock)
                    inner join EverspinTech.dbo.OITL T1 with(nolock) on T0.LogEntry = T1.LogEntry and T1.StockEff =1
                    inner join EverspinTech.dbo.OBTN T2 with(nolock) on T0.ItemCode = T2.ItemCode and T0.SysNumber = T2.SysNumber
                    LEFT JOIN EverspinTech.dbo.OWOR T3 with(nolock) on T1.BaseEntry = T3.DocEntry and T1.BaseType = 202

                    Where T3.U_PONum = '{0}' and T0.ItemCode = '{1}' and T2.DistNumber = '{2}' and T3.Status = 'R'""".format(ponum, finitem, assylot))
            cursor.execute(test2)
            result = cursor.fetchone()
            if result != None:
                if result[5] == shipqty:
                    pass
                    # reason = (abi, ponum, finitem, assylot, shipqty, "AssyLot already received. Line 96", '0', '0')
                    # manualSAP.append(reason)
                elif result[5] < shipqty:
                    compqty = shipqty - result[5]
                    verify = (abi, result[0], ponum, ewsitem, finitem, assylot, waferlot, assystqty, compqty, whsstart, whsfinish, 'Complete', scrpqty)
                    insertSAPComp.append(verify)
                else:
                    reason = (abi, ponum, finitem, assylot, shipqty, "Greater amount completed in SAP than Spinweb.  Line 103", result[0], '0')
                    manualSAP.append(reason)
            else:
                test5=("""Select T3.DocNum, T3.U_SpinwebNo, T3.U_PONum, T3.ItemCode, T3.PlannedQty, T0.Quantity, T2.DistNumber, T3.Status

                        From EverspinTech.dbo.ITL1 T0 with(nolock)
                        inner join EverspinTech.dbo.OITL T1 with(nolock) on T0.LogEntry = T1.LogEntry and T1.StockEff =1
                        inner join EverspinTech.dbo.OBTN T2 with(nolock) on T0.ItemCode = T2.ItemCode and T0.SysNumber = T2.SysNumber
                        LEFT JOIN EverspinTech.dbo.OWOR T3 with(nolock) on T1.BaseEntry = T3.DocEntry and T1.BaseType = 202

                        Where T3.U_PONum = '{0}' and T0.ItemCode = '{1}' and T2.DistNumber = '{2}' and T3.PlannedQty = '{3}' and T3.Status = 'R'""".format(ponum,ewsitem,waferlot,startqty))
                cursor.execute(test5)
                result4 = cursor.fetchone()
                if result4 != None:
                    verify = (abi, result4[0], ponum, ewsitem, finitem, assylot, waferlot, assystqty, shipqty, whsstart, whsfinish, 'Complete', scrpqty)
                    insertSAPComp.append(verify)
                else:
                    verify = (abi, waferlot, ewsitem, finitem)
                    unique.append(verify)

        parse2 = list(set(unique))

#-- Checking for PRDO Creation --#
        mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', port=3306, database='mtsdb')
        cursor1 = mysqlcon.cursor()
        for x in parse2:
            abi, waferlot, ewsitem, finitem = x
            query = ("""Select distinct T2.ABI 'ABI'
                    , CASE WHEN ifnull(T2.PONumber,'') = '' THEN 0 ELSE T2.PONumber END as 'PONumber'
                    , concat("A_",T2.subconName) 'WhsStart'
                    , 'T_UTC' as 'WhsFinish'
                    , ifnull(T2.dieQty,0) 'StartQty' 

                    from mtsdb.tblABILog T2 

                    where T2.ABI = '{0}' and T2.waferLot = '{1}'""".format(abi,waferlot))
            cursor1.execute(query)
            results = cursor1.fetchone()
            verify = (abi, results[1], finitem, results[4], results[3], ewsitem, results[2], waferlot)
            parse3.append(verify)

        for x in parse3:
            abi, ponum, finitem, startqty, whsfinish, ewsitem, whsstart, waferlot = x
            #			print(x)
            test4 = ("""Select T3.DocNum, T3.U_SpinwebNo, T3.U_PONum, T3.ItemCode, T3.PlannedQty, SUM(T0.Quantity), T2.DistNumber, T3.Status

                    From EverspinTech.dbo.ITL1 T0 with(nolock)
                    inner join EverspinTech.dbo.OITL T1 with(nolock) on T0.LogEntry = T1.LogEntry and T1.StockEff =1
                    inner join EverspinTech.dbo.OBTN T2 with(nolock) on T0.ItemCode = T2.ItemCode and T0.SysNumber = T2.SysNumber
                    LEFT JOIN EverspinTech.dbo.OWOR T3 with(nolock) on T1.BaseEntry = T3.DocEntry and T1.BaseType = 202

                    Where T3.U_PONum = '{0}' and T0.ItemCode = '{1}' and T2.DistNumber = '{2}' and T3.PlannedQty = '{3}' and T3.Status = 'L'
                    GROUP BY T3.DocNum, T3.U_SpinwebNo, T3.U_PONum, T3.ItemCode, T3.PlannedQty, T2.DistNumber, T3.Status""".format(ponum, ewsitem, waferlot, startqty))
            cursor.execute(test4)
            result2 = cursor.fetchone()
            if result2 != None:
                pass
                # reason = (abi, ponum, finitem, waferlot, startqty, "Assembly Lot has already been processed.  Line 134", result2[0], '0')
                # manualSAP.append(reason)
            else:
                test3 = ("""Select T0.ItemCode, T2.DistNumber, T1.LocCode, SUM(T0.Quantity) 'OnHand'

                        From EverspinTech.dbo.ITL1 T0 with(nolock)
                        inner join EverspinTech.dbo.OITL T1 with(nolock) on T0.LogEntry = T1.LogEntry and T1.StockEff =1
                        inner join EverspinTech.dbo.OBTN T2 with(nolock) on T0.ItemCode = T2.ItemCode and T0.SysNumber = T2.SysNumber

                        Where T2.DistNumber = '{0}' and T0.ItemCode = '{1}' 
                        GROUP BY T0.ItemCode, T2.DistNumber, T1.LocCode
                        HAVING SUM(T0.Quantity)<>0""".format(waferlot, ewsitem))
                cursor.execute(test3)
                result3 = cursor.fetchone()
                if result3 != None:
                    if result3[3] >= startqty:
                        if finitem == 'Bobcat16TS-ASY':
                            planqty = startqty / 2
                            getitem=("Select ItemCode from OITM with(nolock) where ItemCode = '{0}'".format(finitem))
                            cursor.execute(getitem)
                            getitemcode = cursor.fetchone()
                            verify = (abi, ponum, getitemcode[0], startqty, whsfinish, ewsitem, result3[2], waferlot, planqty) 
                            insertSAPCreate.append(verify)
                        elif finitem == 'Bobcat16BG-ASY':
                            planqty = startqty / 2
                            getitem=("Select ItemCode from OITM with(nolock) where ItemCode = '{0}'".format(finitem))
                            cursor.execute(getitem)
                            getitemcode = cursor.fetchone()
                            verify = (abi, ponum, getitemcode[0], startqty, whsfinish, ewsitem, result3[2], waferlot, planqty) 
                            insertSAPCreate.append(verify)
                        else:
                            getitem=("Select ItemCode from OITM with(nolock) where ItemCode = '{0}'".format(finitem))
                            cursor.execute(getitem)
                            getitemcode = cursor.fetchone()
                            verify = (abi, ponum, getitemcode[0], startqty, whsfinish, ewsitem, result3[2], waferlot) 
                            insertSAPCreate.append(verify)
                    else:
                        reason = (abi, ponum, ewsitem, waferlot, startqty, "Not Enough Quantity On Hand in SAP, cannot create PRDO.  Line 145", '0', '0')
                        manualSAP.append(reason)
                else:
                    reason =(abi, ponum, ewsitem, waferlot, startqty, "LotNum joined to EWS Item Code does Not Exist in SAP, cannot create PRDO.  Line 148", '0', '0')
                    manualSAP.append(reason)


    except:
        print ("An error Occured with Parsing the Data.")
        raise

#	print(manualSAP, "\n")
#	print(insertSAPComp, "\n")
#	input()
#	print((x for x in insertSAPCreate), "\n")
#	input()
    print("Data Parsing Complete.")
	
    print("\n -----{} seconds------".format(time.time()-start_time))
    cursor.close()
    connection.close()
    cursor1.close()
    mysqlcon.close()
	

def createPRDOTbl ():
	print("Starting PRDO Create Data insertion....")
	connection = pypyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
	cursor = connection.cursor()
	try:
		if insertSAPCreate != False:
			for x in insertSAPCreate:
				abi, ponum, finitem, startqty, whsfinish, ewsitem, whsstart, waferlot = x
				query1 = ("select * from VALIDATION.dbo.PROCESSED_CREATE_PRDO_ASSEM with(nolock) where  SpinwebABI= '{0}' and SAPPONo = '{1}' and PlannedQty = '{2}'".format(abi, ponum, startqty))
				cursor.execute(query1)
				result = cursor.fetchone()
				if result == None:
					query2 = ("Select * from VALIDATION.dbo.CREATE_PRDO_ASSEM with(nolock) where SpinwebABI = '{0}' and SAPPONo = '{1}' and PlannedQty = '{2}'".format(abi, ponum, startqty))
					cursor.execute(query2)
					result2 = cursor.fetchone()
					if result2 == None:
						query3 = ("Insert into VALIDATION.dbo.CREATE_PRDO_ASSEM (SpinwebABI, SAPPONo, ItemCodeFinish, PlannedQty, WhseFinish, ItemCodeStart, StartQty, WhseStart, LotType, TraceCode, ParentLotNo) values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}','{8}', '{9}', '{10}')".format(abi, ponum, finitem, startqty, whsfinish, ewsitem, startqty, whsstart, 'Production', '0', waferlot )) 
						cursor.execute(query3)
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
		

	print ("Insert of Create PRDO Data is Complete.")
	
	print("\n -----{} seconds------".format(time.time()-start_time))	
	
	
def reportCompTbl():
	print("Starting Report Complete Data insert preparation....")

	
	#### - Start MSSQL cursor connection - ####
	connection = pypyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
	cursor = connection.cursor()
#	print("Starting Report Completion Data Insertion...")
	mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', port=3306, database='mtsdb')
	cursor1 = mysqlcon.cursor()
	try:
		for x in insertSAPComp:
			if x != None:
				abi, prdo, ponum, ewsitem, finitem, assylot, waferlot, assystqty, compqty, whsstart, whsfinish, comptype, scrpqty = x
				if compqty == 0:
					pass
				else:
					query=("select T1.ABI, T2.waferLot, cast(round((T2.dieQty - sum(distinct T1.assyInQty))/Count(distinct T1.assyLot),2) as unsigned) 'PreScrap'  from mtsdb.tblAssyLotInfo T1 left join mtsdb.tblABILog T2 on Case when T1.ABI like 'EABI%' then left(replace(replace(T1.ABI,'_',''),'-',''),18) else left(replace(replace(T1.ABI,'_',''),'-',''),17) end = replace(T2.ABI,'_','') where T1.ABI ='{0}' and T2.waferLot = '{1}' Group by waferLotAll, dieQty, T2.waferLot, T1.ABI having sum(distinct T1.assyInQty)!= T2.dieQty".format(abi,waferlot))
					cursor1.execute(query)
					result = cursor1.fetchone()
					if result != None:
						prescrp = result[2] + scrpqty
						verify = (abi, prdo, ponum, ewsitem, finitem, assylot, waferlot, assystqty, compqty, whsstart, whsfinish, comptype, prescrp)
						insertSAPComp2.append(verify)
					else:
						verify = (abi, prdo, ponum, ewsitem, finitem, assylot, waferlot, assystqty, compqty, whsstart, whsfinish, comptype, scrpqty)
						insertSAPComp2.append(verify)
		
		for x in insertSAPComp2:
			if x !=None:
				abi, prdo, ponum, ewsitem, finitem, assylot, waferlot, assystqty, compqty, whsstart, whsfinish, comptype, prescrap = x
				#query = ("Select isnull(IBT1.Quantity,0) 'Quantity' from EverspinTech.dbo.IBT1 where BsDocEntry = '{0}' and BsdocType = 202 and BatchNum = '{1}' and quantity = '{2}'".format(prdo, waferlot, startqty))
				#cursor.execute(query)
				#result = cursor.fetchone()
				compamounts = int(assystqty) - int(compqty)
				if compamounts <= 100:
					scrap = int(compamounts) + int(prescrap)
					verify = (abi, prdo, ponum, ewsitem, finitem, assylot, waferlot, compqty, whsstart, whsfinish, comptype, scrap)
					insertSAPComp3.append(verify)
				else:
					verify = (abi, prdo, ponum, ewsitem, finitem, assylot, waferlot, compqty, whsstart, whsfinish, comptype, prescrap)
					insertSAPComp3.append(verify)
		
		for x in insertSAPComp3:
			if x != None:
				abi, prdo, ponum, ewsitem, finitem, assylot, waferlot, compqty, whsstart, whsfinish, comptype, scrapqty = x
				query2 = ("select * from VALIDATION.dbo.PROCESSED_REPORT_COMP_ASSEM with(nolock) WHERE SpinwebABI = '{0}' and SAPPRDONo = '{1}' and Quantity = '{2}' and ParentLotNo = '{3}' and NewLotNo = '{4}' and ItemCodeFinish = '{5}'".format(abi, prdo, compqty, waferlot, assylot, finitem))
				cursor.execute(query2)
				result = cursor.fetchone()
				if result == None:
					query3 = ("Select * from VALIDATION.dbo.REPORT_COMP_ASSEM with(nolock) where SpinwebABI = '{0}' and SAPPRDONo = '{1}' and Quantity = '{2}' and ParentLotNo = '{3}' and NewLotNo = '{4}' and ItemCodeFinish = '{5}'".format(abi, prdo, compqty, waferlot, assylot, finitem))
					cursor.execute(query3)
					result2 = cursor.fetchone()
					if result2 == None:
						query4 = ("insert into VALIDATION.dbo.REPORT_COMP_ASSEM (SpinwebABI, SAPPRDONo, CompletionType, Quantity, ParentLotNo, NewLotNo, WhseFinish, ItemCodeStart, ItemCodeFinish, scrapQty) values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}')".format(abi, prdo, comptype, compqty, waferlot, assylot, whsfinish, ewsitem, finitem, scrapqty))
						cursor.execute(query4)
					else:
						pass
				else:
					pass
			else:
				pass

	except:
		print("Error in checkig for existing Report Complete Data")
		raise

	

	cursor.commit()
	cursor.close()
	connection.close()
	cursor1.close()
	mysqlcon.close()
	print ("Insert of Report Complete Data is Complete.")
	
	print("\n -----{} seconds------".format(time.time()-start_time))
	
def manualEntry():
	print("Starting Error Report Insertion...")

	
	connection = pypyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
	cursor = connection.cursor()
		
	try:
		if manualSAP != False:
			for x in manualSAP:
				abi, ponum, assyp, lot, qty, error, prdo, source = x
				query = ("Select * from VALIDATION.dbo.ERROR_ENTRY_ASSEM with(nolock) where SpinwebABI = '{0}' and PONumber = '{1}' and ItemCode = '{2}' and LotNo = '{3}' and ErrorReason = '{4}'".format(abi, ponum, assyp, lot, error))
				cursor.execute(query)
				result = cursor.fetchone()
				if result == None:
					query1= ("insert into VALIDATION.dbo.ERROR_ENTRY_ASSEM (SpinwebABI, PONumber, ItemCode, LotNo, Qty, ErrorReason, PRDONo, SourceTransID) values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}')".format(abi, ponum, assyp, lot, qty, error, prdo, source))
					cursor.execute(query1)

		else:
			pass

	except:
		print ("An error occured Inserting Error Data.")
		raise
#	print ("Sample: {0} ".format(insertSAPman2[0]))

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










