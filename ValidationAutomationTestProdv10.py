import sys
import pyodbc
import csv
import pymysql
import re 
import itertools
import time
#import progressbar
import mysqllogin

start_time=time.time()
#bar = progress#bar.ProgressBar(max_value=progress#bar.UnknownLength)
#timestr = time.strftime(".%Y.%m.%d-%H_%M_%S")

importSAP = []
preimportEVS = []
preimportEVS2 = []
parse1 = []
parse2 = []
parse3 = []
parse4 = []
repcompSAP = []
insertSAPCreate = []
insertSAPComp = []
insertSAPComp1 = []
insertSAPComp2 = []
insertSAPComp3 = []
insertSAPComp4 = []
manualSAPnonexist = []
insertSAPman1 = []
insertSAPman2 = []



######Data Gather handling#########
def dataGather1():
	print("Starting Spinweb Bulk Data gather...")
	mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', port=3306, database='mtsdb')
	cursor1 = mysqlcon.cursor()
#	mySQLcomVT1 = ("select distinct t2.workOrder, t1.partNumber, t1.lotID, t1.qty, t3.sourceDevice from mtsdb.tblWorkOrderItem t1 inner join mtsdb.tblWorkOrder t2 on t1.workOrderID = t2.workOrderID left join mtsdb.tblProdLotInfo t3 on (t1.lotID = t3.prodLot or t1.lotID = t3.assyLot) where t1.inputTime >= date_add(curdate(), INTERVAL -30 day) and t1.lotId != 'NA' order by t2.workOrder")
#	cursor1.execute(mySQLcomVT1)
#	results = cursor1.fetchone()
#	print("Starting import from Spinweb....")
#	while results:
#		preimportEVS.append(results)
#		results = cursor1.fetchone()
#		#bar.update()
#	#bar.finish()
	mySQLcomVT2 = ("select distinct t2.workOrder, t1.partNumber, t1.lotID, cast(sum(distinct t1.qty) as unsigned) as 'qty', ifnull(t3.sourceDevice, t1.sourceDevice) 'assyPart', ifnull(t4.assyLot,'NA') 'AssyLot' from mtsdb.tblWorkOrderItem t1 inner join mtsdb.tblWorkOrder t2 on t1.workOrderID = t2.workOrderID left join mtsdb.tblProdLotInfo t3 on (t1.lotID = t3.prodLot or t1.lotID = t3.assyLot) left join mtsdb.tblFTLotEndTrans t4 on t1.lotID = t4.startLot where t1.inputTime >= date_add(curdate(), INTERVAL -260 day) and t1.lotID != 'NA' and t1.partNumber != 'NA' and t2.workOrder = 'UTC2147-10' group by t2.workOrder, t1.partNumber, t1.lotID, t3.sourceDevice order by t2.workOrder")
	cursor1.execute(mySQLcomVT2)
	results = cursor1.fetchone()
	while results:
		preimportEVS2.append(results)
		results = cursor1.fetchone()
		#bar.update()
	#bar.finish()

	
#	print (preimportEVS)
#	input()
	print("Spinweb Bulk data gathering complete.")
#	input()
#	for x in preimportEVS2:
#		print(x)
#	input()
#	print ()
#	print ("\n\n\n")
#	print (parse4)
	print("\n -----{} seconds------".format(time.time()-start_time))
	cursor1.close()
	mysqlcon.close()


#####Data Parse handling#########		
def dataParse2():
    print("Starting data parse for combined Spinweb UTC orders and gather from SAP...")
    connection = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor2 = connection.cursor()
    print("Parse 1....")
    for x in preimportEVS2:
#		print (x)
        test1 = ("select T4.U_SpinwebNo, t3.BaseEntry, t4.PlannedQty, t0.Itemcode, t0.DistNumber, t1.location, t0.notes, t1.whscode, t2.quantity from EverspinTech.dbo.OBTN t0 WITH(NOLOCK) inner join EverspinTech.dbo.OBTW t1 WITH(NOLOCK) on t0.absentry = t1.mdabsentry left join EverspinTech.dbo.ITL1 T2 WITH(NOLOCK) on t0.absentry = T2.mdabsEntry left join EverspinTech.dbo.OITL t3 WITH(NOLOCK) on t2.LogEntry = t3.LogEntry left join EverspinTech.dbo.OWOR t4 WITH(NOLOCK) on t3.BaseEntry = t4.DocNum where t4.U_spinwebno like '{0}%' and t0.ItemCode like '{1}%' and (t0.notes like '{2}' or t1.location like '{3}' or t0.DistNumber like '{4}%') and abs(t2.quantity) = '{5}' and t3.BaseType = '202' and t4.status = 'R'".format(x[0], x[4], x[2], x[2], x[2], x[3]))
        cursor2.execute(test1)
        testresult = cursor2.fetchone()
        if testresult != None:
#			print(testresult, " Test1 \n")
            verify = (x[0], x[1], x[2], x[3], testresult[1], x[4], testresult[4])
#			print("To check for Prior Completion:  ", verify, "\n")
#			input()
            repcompSAP.append(verify)
        else:
            test3 = ("select T4.U_SpinwebNo, t3.BaseEntry, t4.PlannedQty, t0.Itemcode, t0.DistNumber, t1.location, t0.notes, t1.whscode, t2.quantity from EverspinTech.dbo.OBTN t0 WITH(NOLOCK) inner join EverspinTech.dbo.OBTW t1 WITH(NOLOCK) on t0.absentry = t1.mdabsentry left join EverspinTech.dbo.ITL1 T2 WITH(NOLOCK) on t0.absentry = T2.mdabsEntry left join EverspinTech.dbo.OITL t3 WITH(NOLOCK) on t2.LogEntry = t3.LogEntry left join EverspinTech.dbo.OWOR t4 WITH(NOLOCK) on t3.BaseEntry = t4.DocNum where t4.U_spinwebno like '{0}%' and t0.ItemCode like '{1}%' and (t0.notes like '{2}' or t1.location like '{3}' or t0.DistNumber like '{4}%') and t3.BaseType = '202' and t4.status = 'R'".format(x[0], x[4], x[2], x[2], x[2]))
            cursor2.execute(test3)
            testresult11 = cursor2.fetchone()
            if testresult11 != None:
#				print(testresult, " Test 3 \n")
                verify = (x[0], x[1], x[2], testresult11[2], testresult11[1], x[4], testresult11[4])
#				print("To check for Prior Completion:  ", verify, "\n")
#				input()
                repcompSAP.append(verify)
            else:
                test4 = ("select T4.U_SpinwebNo, t3.BaseEntry, t4.PlannedQty, t0.Itemcode, t0.DistNumber, t1.location, t0.notes, t1.whscode, t2.quantity from EverspinTech.dbo.OBTN t0 WITH(NOLOCK) inner join EverspinTech.dbo.OBTW t1 WITH(NOLOCK) on t0.absentry = t1.mdabsentry left join EverspinTech.dbo.ITL1 T2 WITH(NOLOCK) on t0.absentry = T2.mdabsEntry left join EverspinTech.dbo.OITL t3 WITH(NOLOCK) on t2.LogEntry = t3.LogEntry left join EverspinTech.dbo.OWOR t4 WITH(NOLOCK) on t3.BaseEntry = t4.DocNum where t4.U_spinwebno like '{0}%' and t0.ItemCode like '{1}%' and (t0.notes like '{2}' or t1.location like '{3}' or t0.DistNumber like '{4}%') and t3.BaseType = '202' and t4.status = 'R'".format(x[0], x[4], x[5], x[5], x[5]))
                cursor2.execute(test4)
                testresult12 = cursor2.fetchone()
                if testresult12 != None:
#					print(testresult12, " Test 4 \n")
                    verify = (x[0], x[1], x[2], testresult12[2], testresult12[1], x[4], x[5])
                    repcompSAP.append(verify)
                else:
                    test2 = ("select T4.U_SpinwebNo, t3.BaseEntry, t4.PlannedQty, t0.Itemcode, t0.DistNumber, t1.location, t0.notes, t1.whscode, t2.quantity from EverspinTech.dbo.OBTN t0 WITH(NOLOCK) inner join EverspinTech.dbo.OBTW t1 WITH(NOLOCK) on t0.absentry = t1.mdabsentry left join EverspinTech.dbo.ITL1 T2 WITH(NOLOCK) on t0.absentry = T2.mdabsEntry left join EverspinTech.dbo.OITL t3 WITH(NOLOCK) on t2.LogEntry = t3.LogEntry left join EverspinTech.dbo.OWOR t4 WITH(NOLOCK) on t3.BaseEntry = t4.DocNum where t4.U_spinwebno like '{0}%' and t0.ItemCode like '{1}%' and (t0.notes like '{2}' or t1.location like '{3}' or t0.DistNumber like '{4}%') and abs(t2.quantity) = '{5}' and t3.BaseType = '202' and t4.status = 'L'".format(x[0], x[4], x[2], x[2], x[2], x[3]))
                    cursor2.execute(test2)
                    testresult2 = cursor2.fetchone()
                    if testresult2 != None:
                        pass
                    else:
#						print("Check for Lot number exist:  ", x)
	#					input()
                        parse1.append(x)
		#bar.update()
	#bar.finish()
#	for x in parse1:
#		print("Parse 1", x)
#	input()
#	for x in parse2:
#		print("Parse 2", x)
#	input()
    print("\n -----{} seconds------".format(time.time()-start_time))
    print("Parse 2-1....")
    for x in parse1:
        test3 = ("Select Distinct OWOR.DocNum, OWOR.U_SpinwebNo, OWOR.ItemCode, WOR1.ItemCode, cast(OWOR.PlannedQty as int) as PlannedQty, OWOR.Status from EverspinTech.dbo.OWOR WITH(NOLOCK) inner join EverspinTech.dbo.WOR1 WITH(NOLOCK) on OWOR.DocEntry = WOR1.DocEntry inner join EverspinTech.dbo.OITL With(NOLOCK) ON OWOR.DocEntry = OITL.BaseEntry and OITL.StockEff = 1 inner join EverspinTech.dbo.ITL1 with(nolock) on ITL1.LogEntry = OITL.LogEntry inner join EverspinTech.dbo.OBTN with(nolock) on ITL1.ItemCode = OBTN.ItemCode and ITL1.SysNumber = OBTN.SysNumber where U_SpinwebNo like '{0}%' and owor.ItemCode = '{1}' and OBTN.DistNumber = '{2}' and WOR1.ItemType = 4".format(x[0], x[1], x[2]))
        cursor2.execute(test3)
        testresult3 = cursor2.fetchone()
        if testresult3 != None:
#			print("Test Result 3: ", testresult3)
            if testresult3[5] == 'L':
                pass
            else:
                test6 = ("select Cast(sum(abs(itl1.quantity)) as int) as Issued, oitl.BaseEntry, OBTN.Distnumber, convert(varchar, OBTN.notes) as ParentLot from OBTN WITH(NOLOCK) inner join itl1 on obtn.SysNumber = itl1.SysNumber and OBTN.ItemCode = ITL1.ItemCode inner join oitl WITH(NOLOCK) on itl1.logentry = oitl.logentry where oitl.baseentry = '{0}' and OBTN.Distnumber = '{1}' group by OITL.BaseEntry, CONVERT(varchar, OBTN.Notes), OBTN.Distnumber".format(testresult3[0], x[2]))
				#test6 = ("select T4.U_SpinwebNo, T3.BaseEntry, T4.PlannedQty, T0.ItemCode, T0.DistNumber, T0.Notes, T1.Location, T1.WhsCode, cast((select abs(sum(S0.quantity)) as 'Receipt'from EverspinTech.dbo.b1_snbopenqtyinnerview S0 where S0.itemcode = T0.ItemCode and S0.SysNumber = T0.SysNumber and S0.applytype = 59 group by S0.applytype) - (select Case When (select abs(sum(S1.quantity)) as 'issued'from EverspinTech.dbo.b1_snbopenqtyinnerview S1 where S1.itemcode = T0.ItemCode and S1.SysNumber = T0.SysNumber and S1.applytype = 60 group by S1.applytype) is not null then (select abs(sum(S1.quantity)) as 'issued'from EverspinTech.dbo.b1_snbopenqtyinnerview S1 where S1.itemcode = T0.ItemCode and S1.SysNumber = T0.SysNumber and S1.applytype = 60 group by S1.applytype) else abs(0) end as 'Issued')as INT) as 'On Hand', t3.DocEntry from EverspinTech.dbo.OBTN t0 inner join EverspinTech.dbo.OBTW t1 on t0.absentry = t1.mdabsentry inner join EverspinTech.dbo.ITL1 T2 on t0.absentry = T2.mdabsEntry inner join EverspinTech.dbo.OITL t3 on t2.LogEntry = t3.LogEntry inner join EverspinTech.dbo.OWOR t4 on t3.BaseEntry = t4.DocNum where T1.Location like '{0}'and T0.ItemCode like '{1}%' and T4.PlannedQty = '{2}' and t3.basetype = '202' and t3.DocType = '59'".format(x[2], x[4], x[3]))
                cursor2.execute(test6)
                testresult5 = cursor2.fetchone()
                if testresult5 != None:
                    while testresult5:
                        if testresult5[2] == x[2]:
                            # print("Test Result 5: ", testresult5)
                            verify = (x[0], x[1], x[2], testresult5[0], testresult5[1], x[4], testresult5[3])
                            repcompSAP.append(verify)
                            testresult5 = cursor2.fetchone()
                        else:
                            pass
                            testresult5 = cursor2.fetchone()
				
                else:
                    #	print("\n" + x + " Error to Existing PRDO, but LotNumber/ItemCode pair not found.  Line 117 python query.\n")
                    reason = (x[0], x[1], x[2], x[3], 'PRDO Exists, but Lot Number/ItemCode pair in Spinweb does not match Lot Number/ItemCode pair in SAP.  Line 133 Query Result.', testresult3[0])
                    manualSAPnonexist.append(reason)
        else:
            test8=("Select '' 'spinweb', '' 'baseentry', '' 'planned', T1.ItemCode, T2.Distnumber, Cast(T2.Notes as nvarchar) 'Notes', T4.WhsCode, Cast(Sum(T0.Quantity) as INT) 'OnHand',  '' 'docentry' from EverspinTech.dbo.ITL1 T0 with(nolock) Inner join EverspinTech.dbo.OITL T1 with(nolock) on T0.Logentry = T1.LogEntry and T1.Stockeff = 1 Inner join EverspinTech.dbo.OBTN T2 with(nolock) on T0.Sysnumber = T2.Sysnumber and T0.ItemCode = T2.ItemCode inner Join EverspinTech.dbo.OBTQ T4 with(nolock) on T2.AbsEntry = T4.MDAbsEntry Where T2.DistNumber = '{0}' and T4.Quantity <> 0 Group by T2.DistNumber, T4.WhsCode, Cast(T2.Notes as nvarchar), T1.ItemCode Having Sum(T0.Quantity) <> 0".format(x[2]))
            cursor2.execute(test8)
            testresult7 = cursor2.fetchone()
            if testresult7 != None:
                print("Test Result 7: ", testresult7)
                verify = (x[0], x[1], x[2], x[3], testresult7[4], testresult7[7], testresult7[3], testresult7[6])
                #	print(verify)
                importSAP.append(verify)
            else:
#				reason = (x[0], x[1], x[2], x[3], 'Parent Lot No not found for Lot Number/Itemcode pair in SAP, Cannot Create PRDO.  Line 105 Query Result.', '0')
#				manualSAPnonexist.append(reason)
				# test4 = ("select T4.U_SpinwebNo, T3.BaseEntry, T4.PlannedQty, T0.ItemCode, T0.DistNumber, T0.Notes, T1.WhsCode, cast((select abs(sum(S0.quantity)) as 'Receipt'from EverspinTech.dbo.b1_snbopenqtyinnerview S0 where S0.itemcode = T0.ItemCode and S0.SysNumber = T0.SysNumber and S0.applytype = 59 group by S0.applytype) - isnull((select abs(sum(S1.quantity)) as 'issued' from EverspinTech.dbo.b1_snbopenqtyinnerview S1 where S1.itemcode = T0.ItemCode and S1.SysNumber = T0.SysNumber and S1.applytype = 60 group by S1.applytype),0)As INT) as 'On Hand', t3.DocEntry from EverspinTech.dbo.OBTN t0 WITH(NOLOCK) inner join EverspinTech.dbo.OBTW t1 WITH(NOLOCK) on t0.absentry = t1.mdabsentry left join EverspinTech.dbo.ITL1 T2 WITH(NOLOCK) on t0.absentry = T2.mdabsEntry left join EverspinTech.dbo.OITL t3 WITH(NOLOCK) on t2.LogEntry = t3.LogEntry left join EverspinTech.dbo.OWOR t4 WITH(NOLOCK) on t3.BaseEntry = t4.DocNum left join EverspinTech.dbo.B1_SnBOpenQtyInnerView t5 on t0.SysNumber = t5.SysNumber and t0.ItemCode = T5.Itemcode where (T0.DistNumber like '{0}%' or T0.Notes like '{1}') and T0.ItemCode like '{2}%' and t3.basetype = '202' and t3.DocType = '59' and t5.ApplyType = '59' and (cast((select abs(sum(S0.quantity)) as 'Receipt'from EverspinTech.dbo.b1_snbopenqtyinnerview S0 where S0.itemcode = T0.ItemCode and S0.SysNumber = T0.SysNumber and S0.applytype = 59 group by S0.applytype) - isnull((select abs(sum(S1.quantity)) as 'issued' from EverspinTech.dbo.b1_snbopenqtyinnerview S1 where S1.itemcode = T0.ItemCode and S1.SysNumber = T0.SysNumber and S1.applytype = 60 group by S1.applytype),0)As INT)) <> 0".format(x[2], x[2], x[4]))
				# cursor2.execute(test4)
				# testresult4 = cursor2.fetchone()
				# if testresult4 != None:
					# print("Test result 4: ", testresult4)
					# verify = (x[0], x[1], x[2], x[3], testresult4[4], testresult4[7], testresult4[3], testresult4[6])
					# importSAP.append(verify)
				# else:
                test9=("""Select T0.ItemCode, CAST(T2.Notes as nvarchar) 'AssyLotNo' from ITL1 T0 with(nolock) inner join OITL T1 with(nolock) on T0.LogEntry = T1.LogEntry and T1.StockEff = 1 inner join OBTN T2 with(nolock) on T0.ItemCode = T2.ItemCode and T0.SysNumber = T2.SysNumber Left JOIN OWOR T3 with(nolock) on T1.BaseEntry = T3.DocEntry and T1.BaseType = 202 Left JOIN WOR1 T4 with(nolock) on T3.DocEntry = T4.DocEntry and T4.ItemType = 4 Where T2.DistNumber = '{0}' and T3.Type = 'D' Group by T0.ItemCode, Cast(T2.Notes as nvarchar), T2.DistNumber, T4.wareHouse""".format(x[2]))
                cursor2.execute(test9)
                testresult10 = cursor2.fetchone()
                if testresult10 != None:
                    test10 = ("""Select T0.ItemCode, T0.DistNumber, T1.WhsCode, Cast(T1.Quantity as int) 'Quantity'
                                from OBTN T0 with(nolock) 
                                inner Join OBTQ T1 with(nolock) on T0.ItemCode = T1.ItemCode and T0.SysNumber = T1.SysNumber and T1.Quantity <> 0
                                Where T0.ItemCode like '{0}%' and T0.DistNumber = '{1}' """.format(testresult10[0], testresult10[1]))
                    cursor2.execute(test10)
                    testresult13 = cursor2.fetchone()
                    if testresult13 != None:
                        # print(testresult13)
                        if testresult13[3] == 0:
                            # print('On hand is 0')
                            pass
                        else:
                            if testresult13[3] >= x[3]:
                                verify = (x[0], x[1], x[2], x[3], testresult13[1], x[3], testresult13[0], testresult13[2])
                                print("Line 162", verify)
                                importSAP.append(verify)
                            else:
                                reason = (x[0], x[1], x[2], x[3], "SAP On Hand Quantity for " + testresult13[0] + " less than planned Qty.  Cannot Create PRDO.  Line 164 query result.", '0')
                                manualSAPnonexist.append(reason)
                else:
                    # print("\n" + x + " Error, did not find Lot number in SAP.  Line 146 Python Query.\n")
                    reason = (x[0], x[1], x[2], x[3], 'Parent Lot No not found for Lot Number/Itemcode pair in SAP, Cannot Create PRDO.  Line 194 Query Result.', '0')
                    manualSAPnonexist.append(reason)
					
    print("\n -----{} seconds------".format(time.time()-start_time))
		#bar.update()
	#bar.finish()
	

	
#	for x in importSAP:
#		print("CreatePRDO: ",x)
#	input()
	
    cursor2.close()
    connection.close()
	
	
#####Create PRDO handling#########
def createPRDOTbl ():
	print("Starting Create PRDO table population....")
	mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', database='mtsdb')
	cursor = mysqlcon.cursor()
	
	try:
		if importSAP != False:
			for x in importSAP:
				a, b, c, d, e, f, g, h = x
				query1 = ("select distinct t2.workOrder, '0' as PONumber, Case When t1.partNumber = 'CONDORTS' then 'CondorTS' when t1.partNumber = 'LYNX16TS' then 'Lynx16TS' when t1.partNumber = 'PANTHER16TS' then 'Panther16TS' When t1.partNumber = 'PANTHER16BG' then 'Panther16BG' when t1.partNumber = 'LYNX16BG' then 'Lynx16BG' when t1.partNumber = 'CONDORBG' then 'CondorBG' when t1.partNumber = 'CONDORDV' then 'CondorDV' When t1.partNumber = 'LYNX08TS' then 'Lynx08TS' when t1.partNumber = 'LYNX08BG' then 'Lynx08BG' When t1.partNumber = 'PANTHER08TS' then 'Panther08TS' When t1.partNumber = 'PANTHER08BG' then 'Panther08BG' else t1.partNumber end 'partNumber', sum(t1.qty) as 'PlannedQty', '{0}' as 'FinishWhse', t1.flow, t1.lotType, t1.traceCode, t1.lotID, '{1}' as 'On Hand', '{2}' as 'SourceDevice' from mtsdb.tblWorkOrderItem t1 inner join mtsdb.tblWorkOrder t2 on t1.workOrderID = t2.workOrderID left join mtsdb.tblProdLotInfo t3 on t1.lotID = t3.prodLot where t2.workOrder = '{3}' and t1.partNumber = '{4}' and t1.lotID = '{5}' Group by t2.workOrder, t1.partNumber, t2.vendor, t1.flow, t1.lotType, t1.traceCode, t1.lotID Having sum(t1.qty) <= '{6}' or cast(sum(distinct t1.qty) as unsigned) = '{7}'".format(h, f, g, a, b, c, d, d))
				cursor.execute(query1)
				result = cursor.fetchone()
#				print(result)
				if result != None:
					if result[10].lower() != result[2].lower():
						if result[3] <= f:
							verify = (result[0], result[1], result[2], result[3], result[4], result[5], result[6], result[7], e, result[9], result[10])
#							print(verify)
							insertSAPCreate.append(verify)
						else:
							reason = (result[0], result[2], result[8], result[3], "SAP On Hand Quantity for " + g + " less than Planned Quantity for PRDO.  Cannot Create PRDO. Line 165 Query Result.", '0')
							manualSAPnonexist.append(reason)
					else:
						reason = (result[0], result[2], result[8], result[3], "Component Part is same as FG, Cannot Create PRDO.  Line 165 Query Result.", '0')
						manualSAPnonexist.append(reason)
				else:
					reason = (a, b, c, d, "Returned None Value from Spinweb Query to Create PRDO. Check Data, Line 165 Query Result.", '0')
				#time.sleep(.0001)
				#bar.update()
					
		else:
			print ("NoneType value, cannot insert into Create PRDO Table.\n")
			pass
	except:
		print ("An error occured finding WorkOrder Number.")
		raise
	#bar.finish()	
	print ("Insert Information Preparation for Create PRDO Table Complete")
#	for x in manualSAPnonexist:
#		print(x)
#	input()
#	for x in insertSAPCreate:
#		print (x)
#	input()
	print("\n -----{} seconds------".format(time.time()-start_time))
	cursor.close()
	mysqlcon.close()
	
	sapsqlcon = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))			
	cursor = sapsqlcon.cursor()
	
	try:
		for x in insertSAPCreate:
			if x != None:
				a, b, c, d, e, f, g, h, i, j, k = x
				query3 = ("Select * from TEST_VALIDATION.dbo.CREATE_PRDO_TEST WITH(NOLOCK) where SpinwebABI = '{0}' and SAPPONo = '{1}' and ItemCodeFinish = '{2}' and PlannedQty = '{3}' and WhseFinish = '{4}' and TestCode ='{5}' and LotType = '{6}' and TraceCode = '{7}' and ParentLotNo = '{8}' and SAPOnHand = '{9}' and ItemCodeStart = '{10}'".format(a, b, c, d, e, f, g, h, i, j, k))
				cursor.execute(query3)
				result = cursor.fetchone()
				if result == None:
					query4 = ("Select * from TEST_VALIDATION.dbo.PROCESSED_CREATE_PRDO_TEST WITH(NOLOCK) where SpinwebABI = '{0}' and ItemCodeFinish = '{1}' and PlannedQty = '{2}' and WhseFinish = '{3}' and TestCode ='{4}' and LotType = '{5}' and TraceCode = '{6}' and ParentLotNo = '{7}' and ItemCodeStart = '{8}'".format(a, c, d, e, f, g, h, i, k))
					cursor.execute(query4)
					result = cursor.fetchone()
					if result == None:
						query2 = ("INSERT INTO TEST_VALIDATION.dbo.CREATE_PRDO_TEST (SpinwebABI, SAPPONo, ItemCodeFinish, PlannedQty, WhseFinish, TestCode, LotType, TraceCode, ParentLotNo, SAPOnHand, ItemCodeStart) values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}')".format(a, b, c, d, e, f, g, h, i, j, k))
						cursor.execute(query2)
					else:
						pass
				else:
					pass
				#bar.update()
			else:
				pass
		
	except:
		print ("\nAn error Occured in the Insert To Create PRDO Table.")
		raise
	#bar.finish()	
	
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	print ("Insert of Create PRDO Data is Complete.")
	print("\n -----{} seconds------".format(time.time()-start_time))

#####Report Comlete handling#########	
def reportCompTbl():
    print ("Starting Report Complete Table population...")
    mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', database='mtsdb')
    cursor = mysqlcon.cursor()
	
    try:
        if repcompSAP != False:
            for x in repcompSAP:
#				print(x)
#				input()
                a, b, c, d, e, f, g = x
                query1 = ("select workOrder, '{0}' as 'PRDO No.', case when opnCodeEnd in ('C600', 'C6B1', 'C6B2', 'C6B3', 'C6B4') and flowCode in ('TF9A', 'TF2D', 'TF4D') then 'Complete' when opnCodeEnd in ('C600', 'C6B1', 'C6B2', 'C601') then 'Complete' when opnCodeEnd in ('LOST', 'C699') then 'Rejected' when opnCodeEnd in ('C6B3', 'C6B4', 'C6B5', 'C6B6', 'C6B7', 'C690','C69H', 'C69C', 'C6BC') then 'Rework' when opnCodeEnd like 'C5%' then 'Engineering' else opnCodeEnd end as 'completion Type', endQty, assyLot, assyLot, endLot, Case when opnCodeEnd in ('C600', 'C6B1', 'C6B2', 'C6B3', 'C6B4') and flowCode in ('TF9A', 'TF2D', 'TF4D') THEN 'T_UTC' When opnCodeEnd in ('C600', 'C6B1', 'C6B2', 'C601') THEN 'T_UTC' When opnCodeEnd in ('LOST', 'C699') THEN 'T_UTC_S' When opnCodeEnd in ('C6B3', 'C6B4', 'C6B5', 'C6B6', 'C6B7', 'C690','C69H', 'C69C', 'C6BC') THEN 'T_UTC_R' Else endLoc END as 'WhseFinish', assyDevice, targetDevice from mtsdb.tblFTLotEndTrans where workOrder = '{1}' and assyLot = '{2}' #and assyDevice like '{3}%'".format(e, a, c, f))
                cursor.execute(query1)
                result = cursor.fetchone()
                #				print("Result line 324: ", result)
                if result != None:
                    while result:
#						print('Assy Lot find ', result)
#						input()
                        verify = (result[0], result[1], result[2], result[3], result[4], result[5], result[6], result[7], result[8], result[9])
                        insertSAPComp.append(verify)
                        result = cursor.fetchone()
                else:
                    query2 = ("select workOrder, '{0}' as 'PRDO No.',  case when opnCodeEnd in ('C600', 'C6B1', 'C6B2', 'C6B3', 'C6B4') and flowCode in ('TF9A', 'TF2D', 'TF4D') then 'Complete' when opnCodeEnd in ('C600', 'C6B1', 'C6B2', 'C601') then 'Complete' when opnCodeEnd in ('LOST', 'C699') then 'Rejected' when opnCodeEnd in ('C6B3', 'C6B4', 'C6B5', 'C6B6', 'C6B7', 'C690','C69H', 'C69C', 'C6BC') then 'Rework' when opnCodeEnd like 'C5%' then 'Engineering' else opnCodeEnd end as 'completion Type', endQty, assyLot, assyLot, endLot, Case when opnCodeEnd in ('C600', 'C6B1', 'C6B2', 'C6B3', 'C6B4') and flowCode in ('TF9A', 'TF2D', 'TF4D') THEN 'T_UTC' When opnCodeEnd in ('C600', 'C6B1', 'C6B2', 'C601') THEN 'T_UTC' When opnCodeEnd in ('LOST', 'C699') THEN 'T_UTC_S' When opnCodeEnd in ('C6B3', 'C6B4', 'C6B5', 'C6B6', 'C6B7', 'C690','C69H', 'C69C', 'C6BC') THEN 'T_UTC_R' Else endLoc END as 'WhseFinish', assyDevice, targetDevice from mtsdb.tblFTLotEndTrans where workOrder = '{1}' and startLot = '{2}' #and assyDevice like '{3}%'".format(e, a, c, f))
                    cursor.execute(query2)
                    result2 = cursor.fetchone()
                    #					print("Result line 336: ", result2)
                    if result2 != None:
                        while result2:
#							print('Start Lot Find ', result2)
#							input()
                            verify = (result2[0], result2[1], result2[2], result2[3], result2[4], result2[5], result2[6], result2[7], result2[8], result2[9])
                            insertSAPComp.append(verify)
                            result2 = cursor.fetchone()
                    else:
                        reason = (a, b, c, d, 'No Record Found in tblFTLotEndTrans, cannot report complete.  Line 331 Query Result.', e)
                        manualSAPnonexist.append(reason)
#						result2 = cursor.fetchone()
					#bar.update()
        else:
            print ("NoneType value, cannot insert into Report Complete Table.\n")
    except:
        print ("An error occured processing Test DATA.")
        raise
	#bar.finish()
#	print(insertSAPComp1)
#	input()
    print ("Insert Information Preparation Complete for Report Complete Table.")
    print("\n -----{} seconds------".format(time.time()-start_time))
    cursor.close()
    mysqlcon.close()
	
	
    sapsqlcon = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))			
    cursor = sapsqlcon.cursor()
#####Verify If data should exist in SAP######	
    print("Verifying Data...")
    try:

        for x in insertSAPComp:
            a, b, c, d, e, f, g, h, i, j = x
            test = ("Select T0.ItemCode, T2.DistNumber, sum(T0.Quantity) 'Quantity', T1.BaseEntry from EverspinTech.dbo.ITL1 T0 with(nolock) inner join EverspinTech.dbo.OITL T1 with(nolock) on T0.LogEntry = T1.LogEntry and T1.StockEff =1 inner join EverspinTech.dbo.OBTN T2 with(nolock) on T0.ItemCode = T2.ItemCode and T0.SysNumber = T2.SysNumber Where T2.DistNumber = '{0}' and T1.BaseType = 202 Group by T0.ItemCode, T2.DistNumber, T1.BaseEntry".format(g))
            cursor.execute(test)
            result = cursor.fetchone()
            if result != None:
                pass
				# reason = (a, i, g, d, "Lot Number Already exists.  Line 352 Query Result.", b)
				# manualSAPnonexist.append(reason)
            else:
                itemupdate = ("Select ItemCode from EverspinTech.dbo.OITM with(nolock) where ItemCode = '{0}'".format(j))
                cursor.execute(itemupdate)
                itmupd = cursor.fetchone()
                verify = (a,b,c,d,e,f,g,h,i,itmupd[0])
                insertSAPComp1.append(verify)
	
        for x in insertSAPComp1:
            a, b, c, d, e, f, g, h, i, j = x
            if c.lower() == 'lost':
                reason = (a, i, e, d, ""+c+" Transaction per UTC Report, Cannot report Complete", b)
                manualSAPnonexist.append(reason)
#				print(reason)
            elif c.lower() == 'complete':
                test1 = ("select cast(sum(T0.quantity) as int) as 'Total' from EverspinTech.dbo.B1_SnBOpenQtyInnerView T0 With(nolock) left join EverspinTech.dbo.OBTN T1 WITH(NOLOCK) on T0.SysNumber = T1.SysNumber left join EverspinTech.dbo.IGN1 T2 WITH(NOLOCK) on T0.ApplyEntry = T2.DocEntry where T2.BaseRef = '{0}' and (T1.Notes like '{1}' or T1.Notes like '{2}' or T1.DistNumber like '{3}' or T1.DistNumber like '{4}') and T2.BaseType = '202' and T2.TranType = 'C'".format(b, f, e, f, e))
                cursor.execute(test1)
                result = cursor.fetchone()
                if result != None:
#					print("Complete 1 ", result, "\n")
#					input()
                    if result[0] == d:
                        pass
						# reason = (a, i, e, d, "Already Reported Complete, Verify Data.  Line 270 Query Result.", b)
						# manualSAPnonexist.append(reason)
                    else:
                        test8 = ("select cast(T0.quantity as int) as 'Total' from EverspinTech.dbo.B1_SnBOpenQtyInnerView T0 With(nolock) left join EverspinTech.dbo.OBTN T1 WITH(NOLOCK) on T0.SysNumber = T1.SysNumber left join EverspinTech.dbo.IGN1 T2 WITH(NOLOCK) on T0.ApplyEntry = T2.DocEntry where (T1.Notes like '{0}' or T1.Notes like '{1}' or T1.DistNumber like '{2}' or T1.DistNumber like '{3}') and T1.Distnumber = '{4}' and T2.BaseType = '202' and T2.TranType = 'C' and T0.Quantity = '{5}'".format(f, e, f, e, g, d))
                        cursor.execute(test8)
                        result = cursor.fetchone()
                        if result != None:
#							print("Complete Quantity 1", result, "\n")
#							input()
                            if result[0] == d:
                                pass
								# reason = (a, i, e, d, "Already Reported Complete, Verify Data.  Line 278 Query Result.", b)
								# manualSAPnonexist.append(reason)
                            else:
                                verify = (a, b, c, d, e, f, g, h, i, j)
                                insertSAPComp2.append(verify)
                        else:
                            verify = (a, b, c, d, e, f, g, h, i, j)
                            insertSAPComp2.append(verify)
                else:
                    test6 = ("select cast(sum(T0.quantity) as int) as 'Total' from EverspinTech.dbo.B1_SnBOpenQtyInnerView T0 With(nolock) left join EverspinTech.dbo.OBTN T1 WITH(NOLOCK) on T0.SysNumber = T1.SysNumber left join EverspinTech.dbo.IGN1 T2 WITH(NOLOCK) on T0.ApplyEntry = T2.DocEntry and T0.ApplyLine = T2.LineNum inner join EverspinTech.dbo.OBTW T3 on T1.absentry = T3.mdabsentry where T2.BaseRef = '{0}' and (T3.Location like '{1}' or T3.Location like '{2}') and T2.TranType = 'C'".format(b, e, f))
                    cursor.execute(test6)
                    result = cursor.fetchone()
                    if result[0] == d:
                        pass
						# reason = (a, i, e, d, "Already Reported Complete, Verify Data. Line 292 Query Result.", b)
						# manualSAPnonexist.append(reason)
                    else:
                        test9 = ("select cast(T0.quantity as int) as 'Total' from EverspinTech.dbo.B1_SnBOpenQtyInnerView T0 With(nolock) left join EverspinTech.dbo.OBTN T1 WITH(NOLOCK) on T0.SysNumber = T1.SysNumber left join EverspinTech.dbo.IGN1 T2 WITH(NOLOCK) on T0.ApplyEntry = T2.DocEntry and T0.ApplyLine = T2.LineNum inner join EverspinTech.dbo.OBTW T3 WITH(NOLOCK) on T1.absentry = T3.mdabsentry where (T3.Location like '{0}' or T3.Location like '{1}') and T2.TranType = 'C' and T0.Quantity = '{2}'".format(e, f, d))
                        cursor.execute(test9)
                        result = cursor.fetchone()
                        if result != None:
                            if result[0] == d:
#								print("Complete Quantity 2",result, "\n")
#								input()
                                pass
								# reason = (a, i, e, d, "Already Reported Complete, Verify Data. Line 299 Query Result.", b)
								# manualSAPnonexist.append(reason)
                            else:
                                verify = (a, b, c, d, e, f, g, h, i, j)
                                insertSAPComp2.append(verify)
								
                        else:
                            verify = (a, b, c, d, e, f, g, h, i, j)
                            insertSAPComp2.append(verify)
            elif c.lower() == 'rejected':
                test2 = ("select cast(sum(T0.quantity) as int) as 'Total' from EverspinTech.dbo.B1_SnBOpenQtyInnerView T0 With(nolock) left join EverspinTech.dbo.OBTN T1 WITH(NOLOCK) on T0.SysNumber = T1.SysNumber left join EverspinTech.dbo.IGN1 T2 WITH(NOLOCK) on T0.ApplyEntry = T2.DocEntry where T2.BaseRef = '{0}' and (T1.Notes like '{1}' or T1.Notes like '{2}' or T1.DistNumber like '{3}' or T1.DistNumber like '{4}') and T2.BaseType = '202' and T2.TranType = 'R'".format(b, f, e, f, e))
                cursor.execute(test2)
                result = cursor.fetchone()
                if result != None:
#					print(" Rejected line 428: ", result)
#					input()
                    if result[0] == d:
                        pass
						# reason = (a, i, e, d, "Already Reported Complete, Verify Data.  Line 314 Query Result.", b)
						# manualSAPnonexist.append(reason)
                    else:
                        test10 = ("select cast(T0.quantity as int) as 'Total' from EverspinTech.dbo.B1_SnBOpenQtyInnerView T0 With(nolock) left join EverspinTech.dbo.OBTN T1 WITH(NOLOCK) on T0.SysNumber = T1.SysNumber left join EverspinTech.dbo.IGN1 T2 WITH(NOLOCK) on T0.ApplyEntry = T2.DocEntry where (T1.Notes like '{0}' or T1.Notes like '{1}' or T1.DistNumber like '{2}' or T1.DistNumber like '{3}') and T1.Distnumber = '{4}' and T2.BaseType = '202' and T2.TranType = 'R' and T0.Quantity = '{5}'".format(f, e, f, e, g, d))
                        cursor.execute(test10)
                        result = cursor.fetchone()
                        if result != None:
                            if result[0] == d:
#								print(" Rejected line 439: ",result)
#								input()
                                pass
								# reason = (a, i, e, d, "Already Reported Complete, Verify Data.  Line 322 Query Result.", b)
								# manualSAPnonexist.append(reason)
                            else:
                                verify = (a, b, c, d, e, f, g, h, i, j)
#								print("Verify line 445: ", verify)
                                insertSAPComp2.append(verify)
                        else:
                            verify = (a, b, c, d, e, f, g, h, i, j)
                            insertSAPComp2.append(verify)
                else:
                    test11 = ("select cast(sum(T0.quantity) as int) as 'Total' from EverspinTech.dbo.B1_SnBOpenQtyInnerView T0 With(nolock) left join EverspinTech.dbo.OBTN T1 WITH(NOLOCK) on T0.SysNumber = T1.SysNumber left join EverspinTech.dbo.IGN1 T2 WITH(NOLOCK) on T0.ApplyEntry = T2.DocEntry and T0.ApplyLine = T2.LineNum inner join EverspinTech.dbo.OBTW T3 WITH(NOLOCK) on T1.absentry = T3.mdabsentry where T2.BaseRef = '{0}' and (T3.Location like '{1}' or T3.Location like '{2}') and T2.TranType = 'R'".format(b, e, f))
                    cursor.execute(test11)
                    result = cursor.fetchone()
                    if result[0] == d:
                        pass
						# reason = (a, i, e, d, "Already Reported Complete, Verify Data. Line 336 Query Result.", b)
						# manualSAPnonexist.append(reason)
                    else:
                        test7 = ("select cast(T0.quantity as int) as 'Total' from EverspinTech.dbo.B1_SnBOpenQtyInnerView T0 With(nolock) left join EverspinTech.dbo.OBTN T1 WITH(NOLOCK) on T0.SysNumber = T1.SysNumber left join EverspinTech.dbo.IGN1 T2 WITH(NOLOCK) on T0.ApplyEntry = T2.DocEntry and T0.ApplyLine = T2.LineNum inner join EverspinTech.dbo.OBTW T3 WITH(NOLOCK) on T1.absentry = T3.mdabsentry where (T3.Location like '{0}' or T3.Location like '{1}') and T2.TranType = 'R' and T0.Quantity = '{2}'".format(e, f, d))
                        cursor.execute(test7)
                        result = cursor.fetchone()
                        if result != None:
#							print("Rejected Quantity 2", result, "\n")
#							input()
                            if result[0] == d:
                                pass
								# reason = (a, i, e, d, "Already Reported Complete, Verify Data. Line 343 Query Result.", b)
								# manualSAPnonexist.append(reason)
                            else:
                                verify = (a, b, c, d, e, f, g, h, i, j)
                                insertSAPComp2.append(verify)
                        else:
                            verify = (a, b, c, d, e, f, g, h, i, j)
                            insertSAPComp2.append(verify)
            elif c.lower() == 'rework':
                test3 = ("select cast(sum(T0.quantity) as int) as 'Total' from EverspinTech.dbo.B1_SnBOpenQtyInnerView T0 With(nolock) left join EverspinTech.dbo.OBTN T1 WITH(NOLOCK) on T0.SysNumber = T1.SysNumber left join EverspinTech.dbo.IGN1 T2 WITH(NOLOCK) on T0.ApplyEntry = T2.DocEntry where T2.BaseRef = '{0}' and (T1.Notes like '{1}' or T1.Notes like '{2}' or T1.DistNumber like '{3}' or T1.DistNumber like '{4}') and T2.BaseType = '202' and T2.TranType = 'C'".format(b, f, e, f, e))
                cursor.execute(test3)
                result = cursor.fetchone()
                if result != None:
                    if result[0] == d:
                        pass
						# reason = (a, i, e, d, "Already Reported Complete, Verify Data. Line 357 Query Result.", b)
						# manualSAPnonexist.append(reason)
                    else:
                        test12 = ("select cast(T0.quantity as int) as 'Total' from EverspinTech.dbo.B1_SnBOpenQtyInnerView T0 With(nolock) left join EverspinTech.dbo.OBTN T1 WITH(NOLOCK) on T0.SysNumber = T1.SysNumber left join EverspinTech.dbo.IGN1 T2 WITH(NOLOCK) on T0.ApplyEntry = T2.DocEntry where (T1.Notes like '{0}' or T1.Notes like '{1}' or T1.DistNumber like '{2}' or T1.DistNumber like '{3}') and T1.Distnumber = '{4}' and T2.BaseType = '202' and T2.TranType = 'C' and T0.Quantity = '{5}'".format(f, e, f, e, g, d))
                        cursor.execute(test12)
                        result = cursor.fetchone()
                        if result != None:
                            if result[0] == d:
                                pass
								# reason = (a, i, e, d, "Already Reported Complete, Verify Data. Line 365 Query Result.", b)
								# manualSAPnonexist.append(reason)
                            else:
                                verify = (a, b, c, d, e, f, g, h, i, j)
                                insertSAPComp2.append(verify)
                        else:
                            verify = (a, b, c, d, e, f, g, h, i, j)
                            insertSAPComp2.append(verify)
                else:
                    test4 = ("select cast(sum(T0.quantity) as int) as 'Total' from EverspinTech.dbo.B1_SnBOpenQtyInnerView T0 With(nolock) left join EverspinTech.dbo.OBTN T1 WITH(NOLOCK) on T0.SysNumber = T1.SysNumber left join EverspinTech.dbo.IGN1 T2 WITH(NOLOCK) on T0.ApplyEntry = T2.DocEntry and T0.ApplyLine = T2.LineNum inner join EverspinTech.dbo.OBTW T3 WITH(NOLOCK) on T1.absentry = T3.mdabsentry where T2.BaseRef = '{0}' and (T3.Location like '{1}' or T3.Location like '{2}') and T2.TranType = 'C'".format(b, e, f))
                    cursor.execute(test4)
                    result = cursor.fetchone()
                    if result[0] == d:
                        pass
						# reason = (a, i, e, d, "Already Reported Complete, Verify Data. Line 379 Query Result.", b)
						# manualSAPnonexist.append(reason)
                    else:
                        test13 = ("select cast(T0.quantity as int) as 'Total' from EverspinTech.dbo.B1_SnBOpenQtyInnerView T0 With(nolock) left join EverspinTech.dbo.OBTN T1 WITH(NOLOCK) on T0.SysNumber = T1.SysNumber left join EverspinTech.dbo.IGN1 T2 WITH(NOLOCK) on T0.ApplyEntry = T2.DocEntry and T0.ApplyLine = T2.LineNum inner join EverspinTech.dbo.OBTW T3 WITH(NOLOCK) on T1.absentry = T3.mdabsentry where (T3.Location like '{0}' or T3.Location like '{1}') and T2.TranType = 'C' and T0.Quantity = '{2}'".format(e, f, d))
                        cursor.execute(test13)
                        result = cursor.fetchone()
                        if result != None:
                            if result[0] == d:
                                pass
								# reason = (a, i, e, d, "Already Reported Complete, Verify Data. Line 386 Query Result.", b)
								# manualSAPnonexist.append(reason)
                            else:
                                verify = (a, b, c, d, e, f, g, h, i, j)
                                insertSAPComp2.append(verify)
                        else:
                            verify = (a, b, c, d, e, f, g, h, i, j)
                            insertSAPComp2.append(verify)
            elif c.lower() == 'engineering':
				### This data will need to be parsed at a later time. ###
                pass
            else:
                pass
    except:
        print("Error in Verifying Report Complete Data")
        raise
#	print(insertSAPComp2)
#	for x in manualSAPnonexist:
#		print(x)
#	input()
    print("Report Complete Data verified.")
    print("\n -----{} seconds------".format(time.time()-start_time))
	
#####Test for already exisiting Data in Processed Table#######	
    print("Checking for Prior Report Completion.....")
    try:
        for x in insertSAPComp2:
            if x != None:
                a, b, c, d, e, f, g, h, i, j = x
                query3 = ("select * from TEST_VALIDATION.dbo.PROCESSED_REPORT_COMP_TEST WITH(NOLOCK) where SpinwebABI = '{0}' and SAPPRDONo = '{1}' and Quantity = '{2}' and ParentLotNo = '{3}' and NewLotNo = '{4}'".format(a, b, d, f, g))
                cursor.execute(query3)
                result = cursor.fetchone()
                if result != None:
                    pass
					# reason = (a, i, g, d, "Already Processed in TEST_VALIDATION Database, Verify Data. Line 555 Query Result.", b)
					# manualSAPnonexist.append(reason)
                else:
                    verify = (a, b, c, d, e, f, g, h, i, j)
                    insertSAPComp3.append(verify)
				#bar.update()
    except:
        print("Error in verifying if reported complete previously.")
        raise
	#bar.finish()
#	print(insertSAPComp3)
    print ("Verified Report Complete Information has not been used before.")
    print("\n -----{} seconds------".format(time.time()-start_time))

    print ("Updating Item Code for Production order rework items....")
    try:
        for x in insertSAPComp3:
            if x != None:
                a, b, c, d, e, f, g, h, i, j = x
                query4 = ("select OWOR.ItemCode, WOR1.ItemCode from EverspinTech.dbo.OWOR WITH(NOLOCK) INNER JOIN EverspinTech.dbo.WOR1 WITH(NOLOCK) on OWOR.DocEntry = WOR1.DocEntry where OWOR.U_SpinwebNo = '{0}'".format(a))
                cursor.execute(query4)
                result = cursor.fetchone()
                if result[0].lower() == j.lower():
                    if result[1].lower() == i.lower():
                        verify = (a, b, c, d, e, f, g, h, i, j)
#						print("line 580 ", verify)
                        insertSAPComp4.append(verify)
                    else:
                        verify = (a, b, c, d, e, f, g, h, result[1], j)
#						print("line 584 ", verify)
                        insertSAPComp4.append(verify)
                else:
                    verify = (a, b, c, d, e, f, g, h, i, result[0])
#					print("line 588 ", verify)
                    insertSAPComp4.append(verify)
			#bar.update()
    except:
        print("Error in finding Rework Finished Good Item Code.")
        raise
	#bar.finish()	
    print("Finished updating Rework Item Code Data.")
    print("\n -----{} seconds------".format(time.time()-start_time))
					
#####Insertion of Data########
    print("Inserting verified Data....")
    try:
        for x in insertSAPComp4:
            if x != None:
                a, b, c, d, e, f, g, h, i, j = x
                query5 = ("select * from TEST_VALIDATION.dbo.REPORT_COMP_TEST WITH(NOLOCK) where SpinwebABI = '{0}' and SAPPRDONo = '{1}' and Quantity = '{2}' and ParentLotNo = '{3}' and NewLotNo = '{4}'".format(a, b, d, f, g))
                query2 = ("insert into TEST_VALIDATION.dbo.REPORT_COMP_TEST (SpinwebABI, SAPPRDONo, CompletionType, Quantity, ParentLotNo, NewLotNo, WhseFinish, ItemCodeStart, ItemCodeFinish, AssyLotNo) values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}')".format(a, b, c, d, f, g, h, i, j, e))
                cursor.execute(query5)
                result = cursor.fetchone()
                if result == None:
                    cursor.execute(query2)
                else:
                    pass
            else:
                pass
			#time.sleep(.0001)
			#bar.update()
    except:
        print ("\nAn error Occured in the Insert to Report Complete Table.")
        raise
	
	#bar.finish()
	
    cursor.commit()
    cursor.close()
    sapsqlcon.close()
    print ("Insert of Report Complete Data is Complete.")
    print("\n -----{} seconds------".format(time.time()-start_time))

######Error Entry handling#########	
def errorEntry1():
	print("Starting Error Entry Table population....")

	
	sapsqlcon = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))			
	cursor = sapsqlcon.cursor()
		
	try:
		if manualSAPnonexist != None:
			for x in manualSAPnonexist:
				a, b, c, d, e, f = x
#				print(x)
				query2= ("select t1.workOrder, t1.ItemCode, t1.LotNo, t1.ItemQty, t1.ErrorReason, t1.SAPPRDONo from TEST_VALIDATION.dbo.PROCESSED_ERROR_ENTRY_TEST T1 WITH(NOLOCK) where t1.workOrder = '{0}' and t1.ItemCode = '{1}' and t1.LotNo = '{2}' and t1.ItemQty = '{3}' and t1.ErrorReason = '{4}' and t1.SAPPRDONo = '{5}'".format(a, b, c, d, e, f))
				cursor.execute(query2)
				result = cursor.fetchone()
				if result != None:
					pass
				else:
					verify = (a, b, c, d, e, f)
					insertSAPman1.append(verify)
			#bar.update()	
		else:
			print ("NoneType value, cannot insert into Manual Table part 2.\n")
	except:
		print ("An error occured finding Work Order Number in Processed Error Report table.")
		raise
	#bar.finish()
#	print ("Sample: {0} ".format(insertSAPman2[0]))
	
	print ("Verified Information has not been previously reported.")
	print("\n -----{} seconds------".format(time.time()-start_time))
	
	try:
		if insertSAPman1 != None:
			for x in insertSAPman1:
				a, b, c, d, e, f = x
				query2 = ("insert into TEST_VALIDATION.dbo.ERROR_ENTRY_TEST (workOrder, ItemCode, LotNo, ItemQty, ErrorReason, SAPPRDONo) values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}')".format(a, b, c, d, e, f))
				cursor.execute(query2)
				#time.sleep(.0001)
				#bar.update()
		else:
			print ("NoneType value, cannot insert into Error Table. \n")
	except:
		print ("\nAn error Occurred in the Insert to Error Entry Table.\n")
		raise
		
	#bar.finish()
	cursor.commit()
	cursor.close()
	sapsqlcon.close()
	print ("Insert of Error Entry Data is Complete.")
	print("\n -----{} seconds------".format(time.time()-start_time))

	
###### Program Start #######

dataGather1()
dataParse2()
createPRDOTbl()
reportCompTbl()
errorEntry1()



