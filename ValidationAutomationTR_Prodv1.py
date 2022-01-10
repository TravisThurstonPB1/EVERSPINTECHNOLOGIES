import sys
import pyodbc
import csv
import pymysql
import re 
import itertools
import time
import mysqllogin
import cred
import requests
import TRProcessReport as ftpfile

username = cred.sluid
password = cred.slpwd

spinwebdata = []
sapimport = []
sapcomp = []
manerror = []
success_create_prdo = []
httpsuccess=[200,201,204]
processerror = []
processcreate = []
processcomp = []
byprod = []
## -- Added these lists 12/28/2021 for Active Byproduct handling -- ##
byprod_active = []
byprod_active_update = []
## ---------------------------------------------------------------- ##

logindict = {"UserName": username, "Password": password, "CompanyDB": "EverspinTech"}
x = requests.post('https://everspinsap2:50000/b1s/v1/Login', json=logindict, verify=False)
logcookies = x.cookies.get_dict()

def datagather():
    print("Starting Spinweb Bulk Data gather...")
    mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', port=3306, database='mtsdb')
    cursor1 = mysqlcon.cursor()
    
    mySQLquery=("""Select T0.workOrder, T0.orderType, T0.startDate, T0.completeDate, T0.shipDate
                , T0.targetDevice, T1.sourceDevice,  T0.targetLotId, IFNULL(T0.creditLotID,'') 'creditLotID'
                , IFNULL(T1.sourceLotID,'') 'sourceLotID'
                , T0.planQty, IFNULL(T0.completeQty,0)  'completeQty', IFNULL(T0.shipQty,0) 'shipQty'
                , IFNULL(T0.creditQty,0) 'creditQty', CAST(IFNULL(T0.scrapQty,0) as int) 'ScrapQty'
                , Case When SUM(T1.issueQty) > T0.planQty then T0.planQty else SUM(T1.issueQty) end 'issueQty'
                , T0.orderStatus

                from mtsdb.tblWorkOrderTR T0
                inner join mtsdb.tblWorkOrderTRItem T1 on T0.workOrder = T1.workOrder

                Where T0.workWeek >= '2151' and T0.startDate between DATE_ADD(CURDATE(), INTERVAL -60 day) and DATE_ADD(CURDATE(), INTERVAL 1 day)

                GROUP BY T0.workOrder, T0.orderType, T0.startDate, T0.completeDate, T0.shipDate
                , T0.targetDevice, T1.sourceDevice,  T0.targetLotId, T0.creditLotID, T1.sourceLotID
                , T0.planQty, T0.completeQty, T0.shipQty, T0.creditQty, T0.scrapQty""")
    cursor1.execute(mySQLquery)
    result = cursor1.fetchone()
    
    while result:
        spinwebdata.append(result)
        result = cursor1.fetchone()
        
    print("Finished Spinweb bulk Data gather")
    cursor1.close()
    mysqlcon.close()
    
def parse():
    print("Starting data parse for Spinweb TR records gathered for SAP handling...")
    connection = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor = connection.cursor()
    
    for x in spinwebdata:
        workorder, otype, sd, ed, shpd, finitem, startitem, finlot, credlot, startlot, pqty, compqty, shpqty, credqty, scrapqty, issueqty, status = x
        
        query1 = ("""Select T0.ItemCode, T0.DistNumber

                    from OBTN T0

                    Where T0.ItemCode = '{0}' and T0.DistNumber = '{1}'""".format(finitem, finlot))
        cursor.execute(query1)
        result1 = cursor.fetchone()
        
        if result1 != None:
            pass
        else:
            query4 = ("""Select OWOR.DocEntry 
                        from OWOR with(nolock) 
                        left join IGE1 with(nolock) on IGE1.BaseEntry = OWOR.DocEntry and IGE1.BaseType = 202
                        where OWOR.U_SpinwebNo  = '{0}' and ISNULL(IGE1.DocEntry,'')='' and OWOR.Status = 'R'""".format(workorder))
            cursor.execute(query4)
            result4 = cursor.fetchone()
            if result4 != None:
                toissue = (result4[0], workorder)
                success_create_prdo.append(toissue)
            else:
                query2 = ("""Select T3.Docentry, T1.ItemCode, T2.DistNumber, T1.Quantity

                            from OITL T0 with(nolock)
                            inner join ITL1 T1 with(nolock) on T0.LogEntry = T1.LogEntry and T0.StockEff = 1 and T0.DocType = 60
                            inner join OBTN T2 with(nolock) on T1.ItemCode = T2.ItemCode and T1.SysNumber = T2.SysNumber
                            inner join OWOR T3 with(nolock) on T0.BaseEntry = T3.DocEntry and T0.Basetype = 202

                            Where T2.DistNumber = '{0}' and T1.ItemCode = '{1}' and T3.U_Spinwebno = '{2}' and T3.Status = 'R'""".format(startlot, startitem, workorder))
                cursor.execute(query2)
                result2 = cursor.fetchone()
                
                if result2 != None:
                    if status.lower() == 'active':
                        ## -- Added Code 12/28/2021 for Active Byproduct handling -- ##
                        if credqty > 0:
                            byprodactive = (workorder, result2[0], startitem, credlot, credqty)
                            byprod_active.append(byprodactive)
                        ## --------------------------------------------------------- ##
                        else:
                            errreason = (workorder, finitem, finlot, compqty, "TR work order is currently Active and not Complete.  Did not process receipt of Production", result2[0])
                            manerror.append(errreason)
                    elif status.lower() == 'complete':
                        tocomplete = (workorder, result2[0], finitem, startitem, finlot, credlot, compqty, credqty, scrapqty)
                        sapcomp.append(tocomplete)
                    elif status.lower() == 'qty error':
                        errreason = (workorder, finitem, finlot, compqty, "TR work order is currently Qty Error.  Did not process receipt of Production", result2[0])
                        manerror.append(errreason)
                else:
                    # tostart = (workorder, finitem, startitem, startlot, pqty, issueqty, result3[2], result3[3])
                    # sapimport.append(tostart)
                    query3 = ("""Select T1.ItemCode, T2.DistNumber, CAST(SUM(T1.Quantity) as INT) 'OnHand', T3.WhsCode

                                from OITL T0 with(nolock)
                                inner join ITL1 T1 with(nolock) on T0.LogEntry = T1.LogEntry and T0.StockEff = 1 
                                inner join OBTN T2 with(nolock) on T1.ItemCode = T2.ItemCode and T1.SysNumber = T2.SysNumber
                                inner join OBTW T3 with(nolock) on T2.AbsEntry = T3.MdAbsEntry

                                Where T2.DistNumber = '{0}' and T1.ItemCode = '{1}'

                                Group by T1.ItemCode, T2.DistNumber, T3.WhsCode""".format(startlot, startitem))
                    cursor.execute(query3)
                    result3 = cursor.fetchone()
                    
                    if result3 != None:
                        # if result3[2] >= issueqty:
                        tostart = (workorder, finitem, startitem, startlot, pqty, issueqty, result3[2], result3[3])
                        sapimport.append(tostart)
                        # else:
                            # errreason = (workorder, startitem, startlot, issueqty, "Cannot start TR work order, not enough on hand in SAP.  SAP on Hand qty is {0}".format(result3[2]), '0')
                            # manerror.append(errreason)
                    else:
                        tostart = (workorder, finitem, startitem, startlot, pqty, issueqty, '0', 'T_UTC')
                        sapimport.append(tostart)
                        # errreason = (workorder, startitem, startlot, '0', "Cannot start TR work order, Lot to issue does not exist in SAP", '0')
                        # manerror.append(errreason)
    
    print("Completed Data parse")
    cursor.close()
    connection.close()
    
def createPRDO():
    print("Starting process for creating production order in SAP...")
    connection = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor = connection.cursor()
    
    for x in sapimport:
        workorder, finitem, startitem, startlot, pqty, issueqty, onhand, whse = x 
        # wocheck = [i[0] for i in manerror if i[0] == workorder]
        # print(wocheck)
        # if workorder in wocheck:
            # errreason = (workorder, startitem, startlot, issueqty, "One or More records for this workorder in parsing were errored due to lot not exist or SAP on Hand Qty less than issued Qty",'0')
            # manerror.append(errreason)
        # else:
        query1 = ("""Select * from VALIDATION.dbo.CREATE_PRDO_TR
                    Where SpinwebABI = '{0}' and ParentLotNo = '{1}' and IssueQty = '{2}'""".format(workorder, startlot, issueqty))
        cursor.execute(query1)
        result = cursor.fetchone()
        
        if result == None:
            query2 = ("""Insert Into VALIDATION.dbo.CREATE_PRDO_TR 
                        (SpinwebABI, SAPPONo, ItemCodeFinish, PlannedQty, WhseFinish, ParentLotno, IssueQty, SAPOnHand, ItemCodeStart)
                        values
                        ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}')""".format(workorder, '0', finitem, pqty, whse, startlot, issueqty, onhand, startitem))
            cursor.execute(query2)
        else:
            pass
    
    
    print("Completed insert of records to {0} Database Create PRDO table".format('VALIDATION'))
    cursor.commit()
    cursor.close()
    connection.close()
    
## -- Added code 12/28/2021 for Active Byproduct handling -- ##
def byProd_Active():
    print("Starting process for byproduct active on order in SAP...")
    connection = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor = connection.cursor()
    
    byprod_active2 = list(set(byprod_active))
    
    for x in byprod_active2:
        workorder, prdo, startitem, credlot, credqty = x
        query1 = ("""Select * from VALIDATION.dbo.BY_PROD_ACTIVE_TR
                    Where workOrder = '{0}' and CreditLot = '{1}'""".format(workorder, credlot))
                    
        cursor.execute(query1)
        result1 = cursor.fetchone()
        
        if result1 != None:
            pass
        else:
            query2=("""Insert Into VALIDATION.dbo.BY_PROD_ACTIVE_TR
                        (workOrder, SAPPRDONo, CreditItem, CreditLot, CreditQty) Values
                        ('{0}', '{1}', '{2}', '{3}', '{4}')""".format(workorder, prdo, startitem, credlot, credqty))
            cursor.execute(query2)
    
    print("Completed insert of records into {0} Database ByProduct Active table".format('VALIDATION'))
    cursor.commit()
    cursor.close()
    connection.close()
    
def reportcomp():
    print("Starting process for completeing production order in SAP...")
    connection = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor = connection.cursor()
    
    sapcomp2 = list(set(sapcomp))
    
    for x in sapcomp2:
        # print(x)
        workorder, prdo, finitem, startitem, finlot, credlot, compqty, credqty, scrapqty = x
        query1 = ("""Select T0.TransID, T0.SpinwebABI, T0.SAPPRDONo, T0.CompQty, ISNULL(T1.byprodAdd,'E') 'byprodAdd' 
					from VALIDATION.dbo.REPORT_COMP_TR T0
					LEFT JOIN VALIDATION.dbo.BY_PROD_ACTIVE_TR T1 on T0.SpinwebABI = T1.workOrder and T0.SAPPRDONo = T1.SAPPRDONo
                    Where T0.SpinwebABI = '{0}' and T0.NewLotNo = '{1}'""".format(workorder, finlot))
        cursor.execute(query1)
        result1 = cursor.fetchone()

        if result1 != None:
            if result1[3] == 0:
                if result1[4] == 'E':
                    query3=("""UPDATE VALIDATION.dbo.REPORT_COMP_TR SET CompQty = '{0}', CredQty = '{1}', ScrapQty = '{2}' WHERE TransID = '{3}'""".format(compqty, credqty, scrapqty, result1[0]))
                    cursor.execute(query3)
                else:
                    query7 = ("""UPDATE VALIDATION.dbo.REPORT_COMP_TR SET CompQty = '{0}', CredQty = '{1}', ScrapQty = '{2}', byprodAdd = 'Y' WHERE TransID = '{3}'""".format(compqty, credqty, scrapqty, result1[0]))
                    cursor.execute(query7)
            else:
                pass
        else:
            query8=("""Select * from VALIDATION.dbo.BY_PROD_ACTIVE_TR where workOrder = '{0}' and SAPPRDONo = '{1}'""".format(workorder,prdo))
            cursor.execute(query8)
            result2 = cursor.fetchone()
            if result2 != None:
                query9=("""Insert Into VALIDATION.dbo.REPORT_COMP_TR
                        (SpinwebABI, SAPPRDONo, CompQty, CredQty, ScrapQty, ParentLotNo, NewLotNo, WhseFinish, ItemCodeStart, ItemCodeFinish, byprodAdd)
                        values
                        ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', 'Y')""".format(workorder, prdo, compqty, credqty, scrapqty, credlot, finlot, 'T_UTC', startitem, finitem))
                cursor.execute(query9)
            else:
                query2=("""Insert Into VALIDATION.dbo.REPORT_COMP_TR
                        (SpinwebABI, SAPPRDONo, CompQty, CredQty, ScrapQty, ParentLotNo, NewLotNo, WhseFinish, ItemCodeStart, ItemCodeFinish, byprodAdd)
                        values
                        ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', 'N')""".format(workorder, prdo, compqty, credqty, scrapqty, credlot, finlot, 'T_UTC', startitem, finitem))
                cursor.execute(query2)
    
    
    print("Completed insert of records into {0} Database Report comp table".format('VALIDATION'))
    cursor.commit()
    cursor.close()
    connection.close()
    
def errortable():
    print("Starting process for Error Table population...")
    connection = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor = connection.cursor()
    
    for x in manerror:
        workorder, item, lot, qty, errreason, prdo = x
        query2 = ("""Select * from VALIDATION.dbo.ERROR_ENTRY_TR
                    WHERE workOrder = '{0}' and ItemCode = '{1}' and LotNo = '{2}'""".format(workorder, item, lot))
        cursor.execute(query2)
        result = cursor.fetchone()
        if result != None:
            if errreason.lower() == result[1].lower():
                pass
            else:
                query3=("""UPDATE VALIDATION.dbo.ERROR_ENTRY_TR SET ErrorReason = '{0}', Qty = '{1}' WHERE TransID = '{2}'""".format(errreason, qty, result[0]))
                cursor.execute(query3)
        else:
            query1 = ("""Insert Into VALIDATION.dbo.ERROR_ENTRY_TR
                        (workOrder, ItemCode, LotNo, Qty, ErrorReason, PRDONo, CreateDate)
                        values
                        ('{0}','{1}','{2}','{3}','{4}','{5}', GETDATE())""".format(workorder, item, lot, qty, errreason, prdo))
            cursor.execute(query1)
        
    print("Completed insert of records into {0} Database Error Table".format('VALIDATION'))
    cursor.commit()
    cursor.close()
    connection.close()


####################################
## -- Processing Valid records -- ##

def createPRDO_inSAP():
    print("Starting process for Creating Production Orders in SAP from Validation Data...")
    connection = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor = connection.cursor()
    
       
    prodheader={}
    prodlines = []
    
    query1 = ("""Select DISTINCT T0.SpinwebABI, T0.SAPPONo, T0.ItemCodeFinish, T0.PlannedQty, T0.WhseFinish, REPLACE(Convert(date,GETDATE(),112),'-','') 'Date'
                
                from VALIDATION.dbo.CREATE_PRDO_TR T0 with(nolock)
                LEFT Join OWOR with(nolock) on T0.SpinwebABI collate database_default = OWOR.U_SpinwebNo collate database_default
				WHERE ISNULL(OWOR.DocEntry,'') = ''""")
    cursor.execute(query1)
    result1 = cursor.fetchone()
    
    
    while result1:
        prodheader["DueDate"] = result1[5]
        prodheader["ItemNo"] = result1[2]
        prodheader["PlannedQuantity"] = result1[3]
        prodheader["Warehouse"] = result1[4]
        prodheader["U_SpinwebNo"] = result1[0]
        prodheader["U_PONum"] = result1[1]
        prodheader["Remarks"] = "PIO Auto TR Process via ServiceLayer"
        
        
        # print(prodheader)
        try:
            crprdo = requests.post("https://everspinsap2:50000/b1s/v1/ProductionOrders", json=prodheader, cookies=logcookies, verify=False)
            # print(crprdo.json())
            if crprdo.status_code in httpsuccess:
                crprdolist = (crprdo.json()["AbsoluteEntry"],crprdo.json()["U_SpinwebNo"])
                success_create_prdo.append(crprdolist)
            else:
                print(result1)
                createPRDOerror = (result1[0], result1[2], 'CreatePRDOFail', result1[3], crprdo.json()["error"]["message"]["value"], '0')
                processerror.append(createPRDOerror)
        except:
            raise

        result1 = cursor.fetchone()
            
        # print(success_create_prdo)
    for x in success_create_prdo:
        docentry, workorder = x
        query4 = ("""Select DocEntry from IGE1 with(nolock) where BaseEntry = '{0}' and BaseType = 202""".format(docentry))
        cursor.execute(query4)
        result4 = cursor.fetchone()
        # print(result4)
        if result4 != None:
            pass
        else:
            issueheader = {}
            issuelines = []
            batchlines = []
            
            query2 = ("""Select T0.PlannedQty, T0.ParentLotNo, T0.IssueQty, T0.ItemCodeStart, MAX(T2.LineNum) 'Line'
                        from VALIDATION.dbo.CREATE_PRDO_TR T0 with(nolock)
                        INNER JOIN EverspinTech.dbo.OWOR T1 with(nolock) on T0.SpinwebABI collate database_default = T1.U_SpinwebNo collate database_default
                        INNER JOIN EverspinTech.dbo.WOR1 T2 with(nolock) on T1.DocEntry = T2.DocEntry
                        where SpinwebABI = '{0}'
                        Group By T0.PlannedQty, T0.ParentLotNo, T0.IssueQty, T0.ItemCodeStart """.format(workorder))
            cursor.execute(query2)
            result2 = cursor.fetchone()
            # print(result2)
                        
            issueheader["Comments"] = "PIO Auto TR Process Via ServiceLayer"
            issuelines.append({"BaseEntry": docentry,
                                "BaseType": "202",
                                "Quantity": result2[0],
                                "BaseLine": '0'})
            issuelines.append({"BaseEntry": docentry,
                                "BaseType": "202",
                                "Quantity": result2[0],
                                "BaseLine": result2[4]})
            while result2:
                batchlines.append({"BatchNumber": result2[1],
                                    "Quantity": result2[2],
                                    "ItemCode": result2[3]})
                result2 = cursor.fetchone()
                
            issuelines[0]["BatchNumbers"] = batchlines
            issueheader["DocumentLines"] = issuelines
            
            # print(issueheader)
            # print(result2)
            
            try:
                crissue = requests.post("https://everspinsap2:50000/b1s/v1/InventoryGenExits", json=issueheader, cookies=logcookies, verify=False)
                # print(crissue.json()["DocumentLines"][0]["BaseEntry"])
                if crissue.status_code in httpsuccess:
                    records = (workorder, crissue.json()["DocumentLines"][0]["BaseEntry"])
                    processcreate.append(records)
                else:
                    createPRDOerror = (workorder, 'N/A', 'N/A', '0', crissue.json()["error"]["message"]["value"], '0')
                    processerror.append(createPRDOerror)
            except:
                raise
    for x in processcreate:
        workorder, prdo = x
        # print(x)
        query5 = ("""INSERT into VALIDATION.dbo.PROCESSED_CREATE_PRDO_TR 
					Select SpinwebABI, SAPPONo, ItemCodeFinish, PlannedQty, WhseFinish, ParentLotNo, IssueQty, SAPOnHand, ItemCodeStart, GETDATE() 'CreateDate', '{0}' 'PRDONo' 
                    FROM VALIDATION.dbo.CREATE_PRDO_TR 
                    WHERE SpinwebABI = '{1}'""".format(prdo, workorder))
        cursor.execute(query5)
        
        query6 = ("""DELETE FROM VALIDATION.dbo.CREATE_PRDO_TR WHERE SpinwebABI = '{0}'""".format(workorder))
        cursor.execute(query6)
    
    print("Completed Production order Creation")
    cursor.commit()
    cursor.close()
    connection.close()

def byproduct_updateSAP():
    print("Starting process for adding byproduct to Production Orders in SAP from Validation Data...")
    connection = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor = connection.cursor()
    
    woheader={}
    wolines=[]

    query1=("""Select SpinwebABI, SAPPRDONo, CompQty, CredQty, ScrapQty, ParentlotNo, NewLotNo, WhseFinish, ItemCodeStart, ItemCodeFinish
            , (Select Top 1 WOR1.OcrCode2 from EverspinTech.dbo.WOR1 where WOR1.DocEntry = SAPPRDONo) 'Fam'
            , MAX(WOR1.LineNum) + 1 'LineNum'
            from VALIDATION.dbo.REPORT_COMP_TR with(nolock)
            INNER JOIN EverspinTech.dbo.WOR1 with(nolock) on REPORT_COMP_TR.SAPPRDOno  = WOR1.DocEntry
			WHERE byprodAdd = 'N'
            GROUP by SpinwebABI, SAPPRDONo, CompQty, CredQty, ScrapQty, ParentlotNo, NewLotNo, WhseFinish, ItemCodeStart, ItemCodeFinish""")
    cursor.execute(query1)
    result1 = cursor.fetchone()
    
    while result1:
        planqty = result1[3]*-1
        
        woheader["AbsoluteEntry"]=result1[1]
        wolines.append({"DocumentAbsoluteEntry": result1[1],
                        "LineNumber": result1[11],
                        "ItemNo": result1[8],
                        "PlannedQuantity": planqty,
                        "ItemType": "pit_Item",
                        "DistributionRule2": result1[10],
                        "Warehouse": result1[7],
                        "ProductionOrderIssueType": "im_Manual"})
        woheader["ProductionOrderLines"]=wolines
                      
        try:
            crprdo = requests.patch("https://everspinsap2:50000/b1s/v1/ProductionOrders({0})".format(result1[1]), json=woheader, cookies=logcookies, verify=False)
            # print(crprdo.json())
            if crprdo.status_code not in httpsuccess:
                createPRDOerror = (result1[0], result1[8], result1[5], result1[3], crprdo.json()["error"]["message"]["value"], result1[1])
                processerror.append(createPRDOerror)
            else:
                byprod.append(result1[0])
        except:
            raise
            
        woheader.clear()
        wolines.clear()
        
        result1 = cursor.fetchone()
    
    for x in byprod:
        # print(x)
        query2 = ("""UPDATE VALIDATION.dbo.REPORT_COMP_TR SET byprodAdd = 'Y' WHERE SpinwebABI = '{0}'""".format(x))
        cursor.execute(query2)
        
    ## -- Added Code 12/28/2021 for Active By Product Handling -- ##
    byprod.clear()
    
    query3 = ("""Select workOrder, SAPPRDONo, CreditItem, CreditQty, CreditLot 
            , (Select Top 1 WOR1.OcrCode2 from EverspinTech.dbo.WOR1 where WOR1.DocEntry = SAPPRDONo) 'Fam'
            , MAX(WOR1.LineNum) + 1 'LineNum'
            from VALIDATION.dbo.BY_PROD_ACTIVE_TR with(nolock)
            INNER JOIN EverspinTech.dbo.WOR1 with(nolock) on BY_PROD_ACTIVE_TR.SAPPRDOno  = WOR1.DocEntry
			WHERE byprodAdd = 'N'
            GROUP by workOrder, SAPPRDONo, CreditItem, CreditQty, CreditLot""")
    cursor.execute(query3)
    result2 = cursor.fetchone()
    
    
    while result2:
        planqty = result2[3]*-1
        
        woheader["AbsoluteEntry"]=result2[1]
        wolines.append({"DocumentAbsoluteEntry": result2[1],
                        "LineNumber": result2[6],
                        "ItemNo": result2[2],
                        "PlannedQuantity": planqty,
                        "ItemType": "pit_Item",
                        "DistributionRule2": result2[5],
                        "Warehouse": 'T_UTC',
                        "ProductionOrderIssueType": "im_Manual"})
        woheader["ProductionOrderLines"]=wolines
        

        try:
            crprdo = requests.patch("https://everspinsap2:50000/b1s/v1/ProductionOrders({0})".format(result2[1]), json=woheader, cookies=logcookies, verify=False)
            # print(crprdo.json())
            if crprdo.status_code not in httpsuccess:
                createPRDOerror = (result2[0], result2[2], result2[4], result2[3], crprdo.json()["error"]["message"]["value"], result2[1])
                processerror.append(createPRDOerror)
            else:
                byprod.append(result2[0])
        except:
            raise
            
        woheader.clear()
        wolines.clear()
        
        result2 = cursor.fetchone()
        
    for x in byprod:
        # print(x)
        query4 = ("""UPDATE VALIDATION.dbo.BY_PROD_ACTIVE_TR Set byprodAdd = 'Y' WHERE workOrder = '{0}'""".format(x))
        cursor.execute(query4)
    ## ----------------------------------------------------------- ##
        
    cursor.commit()    
    cursor.close()
    connection.close()

def receipt_inSAP():
    print("Starting process for Receipt of Production in SAP from Validation Data...")
    connection = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    connection2 = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor = connection.cursor()
    cursor2 = connection2.cursor()  ## -- Added code 12/28/2021 for Active byproduct handling -- ##
    
    query1=("""Select T0.SpinwebABI, T0.SAPPRDONo, T0.CompQty, T0.CredQty, T0.ScrapQty, T0.ParentlotNo, T0.NewLotNo, T0.WhseFinish, T0.ItemCodeStart, T0.ItemCodeFinish
            , CAST(T1.StockPrice as float) 'StockPrice', Cast(T2.Notes as nvarchar) 'Notes', Max(T3.Linenum) 'LineNum'
            
            from VALIDATION.dbo.REPORT_COMP_TR T0 with(nolock)
            Inner Join EverspinTech.dbo.IGE1 T1 with(nolock) on T0.SAPPRDONo = T1.BaseEntry and T1.BaseType = 202 and T1.ItemType = 4
            INNER JOIN EverspinTech.dbo.OBTN T2 with(nolock) on T0.ParentLotNo collate database_default = T2.DistNumber collate database_default and T0.ItemCodeStart collate database_default = T2.ItemCode collate database_default
            INNER JOIN EverspinTech.dbo.WOR1 T3 with(nolock) on T0.SAPPRDONo = T3.DocEntry
            INNER JOIN EverspinTech.dbo.OWOR T4 with(nolock) on T3.DocEntry = T4.DocEntry

            Where ISNULL(T4.CmpltQty, 0) <= 0

            group by T0.SpinwebABI, T0.SAPPRDONo, T0.CompQty, T0.CredQty, T0.ScrapQty, T0.ParentlotNo, T0.NewLotNo, T0.WhseFinish, T0.ItemCodeStart, T0.ItemCodeFinish, T1.StockPrice, Cast(T2.Notes as nvarchar)""")
    cursor.execute(query1)
    result1 = cursor.fetchone()
    # print(result1)
    
    while result1:
    
        ## -- Added Code 12/28/2021 for Active Byproduct Handling -- ##
        query6 = ("""Select * from VALIDATION.dbo.BY_PROD_ACTIVE_TR Where byprodAdd = 'Y' and receiptDone = 'N' and SAPPRDONo = '{0}'""".format(result1[1]))
        cursor2.execute(query6)
        result3 = cursor2.fetchone()
        if result3 == None:
        ## --------------------------------------------------------- ##
    
            receiptheader={}
            receiptlines=[]
            receiptbacthes=[]
            
            receiptheader["Comments"]="PIO Auto TR Process Via ServiceLayer"
            receiptlines.append({"BaseEntry": result1[1],
                                "BaseType": "202",
                                "Quantity": result1[2],
                                "U_PRDOScrap": result1[4]})
            receiptlines.append({"BaseEntry": result1[1],
                                "BaseType": "202",
                                "TreeType": "iNotATree",
                                "BaseLine": result1[12],
                                "UnitPrice": result1[10],
                                "Quantity": result1[3]})
            receiptbacthes.append({"BatchNumber": result1[6],
                                "Notes": result1[5],
                                "Quantity": result1[2],
                                "ItemCode": result1[9]})
            receiptbacthes.append({"BatchNumber": result1[5],
                                "Notes": result1[11],
                                "Quantity": result1[3],
                                "ItemCode": result1[8]})
            receiptlines[0]["BatchNumbers"]=[receiptbacthes[0]]
            receiptlines[1]["BatchNumbers"]=[receiptbacthes[1]]
            receiptheader["DocumentLines"]=receiptlines
            
            # print(receiptbacthes[0])
            # print(receiptbacthes[1])
            
            # print(receiptheader)
            
            try:
                woreceipt = requests.post("https://everspinsap2:50000/b1s/v1/InventoryGenEntries", json=receiptheader, cookies=logcookies, verify=False)
                # print(woreceipt.json())
                if woreceipt.status_code in httpsuccess:
                    processcomp.append(result1[0])
                else:
                    createPRDOerror = (result1[0], result1[9], result1[6], result1[2], woreceipt.json()["error"]["message"]["value"], result1[1])
                    processerror.append(createPRDOerror)
            except:
                raise
            
            result1 = cursor.fetchone()    
        ## -- Added Code 12/28/2021 for Active Byproduct Handling -- ##
        else:
            receiptheader={}
            receiptlines=[]
            receiptbacthes=[]
            
            receiptheader["Comments"]="PIO Auto TR Process Via ServiceLayer"
            receiptlines.append({"BaseEntry": result1[1],
                                "BaseType": "202",
                                "Quantity": result1[2],
                                "U_PRDOScrap": result1[4]})
            receiptbacthes.append({"BatchNumber": result1[6],
                                "Notes": result1[5],
                                "Quantity": result1[2],
                                "ItemCode": result1[9]})
            receiptlines[0]["BatchNumbers"]= receiptbacthes
            receiptheader["DocumentLines"]=receiptlines
            
            # print(receiptbacthes[0])
            # print(receiptbacthes[1])
            
            # print(receiptheader)
            
            try:
                woreceipt = requests.post("https://everspinsap2:50000/b1s/v1/InventoryGenEntries", json=receiptheader, cookies=logcookies, verify=False)
                # print(woreceipt.json())
                if woreceipt.status_code in httpsuccess:
                    processcomp.append(result1[0])
                else:
                    createPRDOerror = (result1[0], result1[9], result1[6], result1[2], woreceipt.json()["error"]["message"]["value"], result1[1])
                    processerror.append(createPRDOerror)
            except:
                raise
            
            result1 = cursor.fetchone()
        ## ------------------------------------------------------- ##
    
    for x in processcomp:
        query2 = ("""INSERT into VALIDATION.dbo.PROCESSED_REPORT_COMP_TR
                    Select SpinwebABI, SAPPRDONo, CompQty, CredQty, ScrapQty, ParentLotNo, NewLotNo, WhseFinish, ItemCodeStart, ItemCodeFinish, GETDATE() 'CreateDate'
                    FROM VALIDATION.dbo.REPORT_COMP_TR
                    WHERE SpinwebABI = '{0}'""".format(x))
        cursor.execute(query2)
        
        query3 = ("""DELETE FROM VALIDATION.dbo.REPORT_COMP_TR WHERE SpinwebABI = '{0}'""".format(x))
        cursor.execute(query3)
        
    ## -- Added code 12/28/2021 Receipt of Credit Lot on Active Production -- ##
    query4 = ("""Select workOrder, SAPPRDONo, CreditItem, CreditQty, CreditLot 
            , (Select Top 1 WOR1.OcrCode2 from EverspinTech.dbo.WOR1 where WOR1.DocEntry = SAPPRDONo) 'Fam'
            , MAX(WOR1.LineNum) 'LineNum', CAST(T1.StockPrice as float) 'StockPrice', CAST(T2.Notes as nvarchar) 'Notes'
            from VALIDATION.dbo.BY_PROD_ACTIVE_TR with(nolock)
            INNER JOIN EverspinTech.dbo.WOR1 with(nolock) on BY_PROD_ACTIVE_TR.SAPPRDOno  = WOR1.DocEntry
			Inner Join EverspinTech.dbo.IGE1 T1 with(nolock) on SAPPRDONo = T1.BaseEntry and T1.BaseType = 202 and T1.ItemType = 4
			INNER JOIN EverspinTech.dbo.OBTN T2 with(nolock) on CreditItem collate database_default = T2.ItemCode collate database_default 
            and CreditLot collate database_default = T2.DistNumber collate database_default
			WHERE byprodAdd = 'Y' and receiptDone = 'N'
            GROUP by workOrder, SAPPRDONo, CreditItem, CreditQty, CreditLot, T1.StockPrice, CAST(T2.Notes as nvarchar)""")
    cursor.execute(query4)
    result2 = cursor.fetchone()
    
    while result2:
        receiptheader={}
        receiptlines=[]
        receiptbacthes=[]
        
        receiptheader["Comments"]="PIO Auto TR Process Via ServiceLayer"
        receiptlines.append({"BaseEntry": result2[1],
                            "BaseType": "202",
                            "TreeType": "iNotATree",
                            "BaseLine": result2[6],
                            "UnitPrice": result2[7],
                            "Quantity": result2[3]})
        receiptbacthes.append({"BatchNumber": result2[4],
                            "Notes": result2[8],
                            "Quantity": result2[3],
                            "ItemCode": result2[2]})
        receiptlines[0]["BatchNumbers"]=receiptbacthes
        receiptheader["DocumentLines"]=receiptlines
        
        # print(receiptheader)
        
        try:
            woreceipt = requests.post("https://everspinsap2:50000/b1s/v1/InventoryGenEntries", json=receiptheader, cookies=logcookies, verify=False)
            # print(woreceipt.json())
            if woreceipt.status_code in httpsuccess:
                byprod_active_update.append(result2[0])
            else:
                createPRDOerror = (result2[0], result2[2], result2[4], result2[3], woreceipt.json()["error"]["message"]["value"], result2[1])
                processerror.append(createPRDOerror)
        except:
            raise
        
        result2 = cursor.fetchone()
        
    for x in byprod_active_update:
        query5 = ("""UPDATE VALIDATION.dbo.BY_PROD_ACTIVE_TR Set receiptDone = 'Y', receiptDate = GETDATE() Where workOrder = '{0}'""".format(x))
        cursor.execute(query5)
    ## ----------------------------------------------------------------------- ##
    
    print("Completed Receipt from Production")
    cursor.commit()
    cursor.close()
    cursor2.close()  ## -- Added Code 12/28/2021 for active byproduct handling -- ##
    connection.close()
    connection2.close()
    

def close_PRDO():
    print("Starting process for Close of Production in SAP from Validation Data...")
    connection = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor = connection.cursor()
    
    query1 = ("""Select T0.DocEntry, T0.U_SpinwebNo, T0.ItemCode, T0.PlannedQty 
                from EverspinTech.dbo.OWOR T0 with(nolock)
                INNER JOIN (Select SUM(IGN1.Quantity) + SUM(IGN1.U_PRDOScrap) 'total', BaseEntry 
                            from EverspinTech.dbo.IGN1 with(nolock) 
                            where IGN1.BaseType = 202 Group by BaseEntry)T1 on T0.Docentry = T1.BaseEntry
                Where T0.PlannedQty = T1.total and T0.Status = 'R'""")
    cursor.execute(query1)
    result1 = cursor.fetchone()
    
    while result1:
        payload = {"ProductionOrderStatus": "L"}
        try:
            woclose = requests.patch("https://everspinsap2:50000/b1s/v1/ProductionOrders({0})".format(result1[0]), json=payload, cookies=logcookies, verify=False)
            if woclose.status_code not in httpsuccess:
                createPRDOerror = (result1[1], result1[2], 'FailedClosePRDO', result1[3], woclose.json()["error"]["message"]["value"], result1[0])
                processerror.append(createPRDOerror)
        except:
            raise
        
        result1 = cursor.fetchone()
        
    print("Completed Close process")
    cursor.close()
    connection.close()

def process_Error():
    print("Starting process for adding processed errors to table from processing Data...")
    connection = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor = connection.cursor()
    
    for x in processerror:
        workorder, item, lot, qty, errreason, prdo = x
        # print(x)
        query2 = ("""Select * from VALIDATION.dbo.ERROR_ENTRY_TR
                    WHERE workOrder = '{0}' and ItemCode = '{1}' and LotNo = '{2}'""".format(workorder, item, lot))
        cursor.execute(query2)
        result = cursor.fetchone()
        if result == None:
            query1 = ("""Insert Into VALIDATION.dbo.ERROR_ENTRY_TR
                    (workOrder, ItemCode, LotNo, Qty, ErrorReason, PRDONo, CreateDate)
                    values
                    ('{0}','{1}','{2}','{3}','{4}','{5}', GETDATE())""".format(workorder, item, lot, qty, errreason, prdo))
            cursor.execute(query1)
        else:
            pass
        
    print("Completed insert of records into {0} Database Error Table".format('VALIDATION'))
    cursor.commit()
    cursor.close()
    connection.close()

####################################


datagather()
parse()
createPRDO()
byProd_Active()  ## -- Added 12/28/2021 -- ##
reportcomp()
errortable()

## -- Start Production Order processing in SAP -- ##

createPRDO_inSAP()
byproduct_updateSAP()
receipt_inSAP()
close_PRDO()
process_Error()

## -- Added 12/28/2021 Clear lists for Second round run -- ##

spinwebdata.clear()
sapimport.clear()
sapcomp.clear()
manerror.clear()
success_create_prdo.clear()
processerror.clear()
processcreate.clear()
processcomp.clear()
byprod.clear()
byprod_active.clear()
byprod_active_update.clear()

## -- Second round run -- ##

datagather()
parse()
createPRDO()
byProd_Active()  ## -- Added 12/28/2021 -- ##
reportcomp()
errortable()

## -- Start Production Order processing in SAP second run -- ##

createPRDO_inSAP()
byproduct_updateSAP()
receipt_inSAP()
close_PRDO()
process_Error()

## -- Added 12/31/2021 Clear lists for third round run -- ##

spinwebdata.clear()
sapimport.clear()
sapcomp.clear()
manerror.clear()
success_create_prdo.clear()
processerror.clear()
processcreate.clear()
processcomp.clear()
byprod.clear()
byprod_active.clear()
byprod_active_update.clear()

## -- Third round run -- ##

datagather()
parse()
createPRDO()
byProd_Active()
reportcomp()
errortable()

## -- Start Production Order processing in SAP third run -- ##

createPRDO_inSAP()
byproduct_updateSAP()
receipt_inSAP()
close_PRDO()
process_Error()

## -- Added 1/06/2022 Clear lists for fourth round run -- ##

spinwebdata.clear()
sapimport.clear()
sapcomp.clear()
manerror.clear()
success_create_prdo.clear()
processerror.clear()
processcreate.clear()
processcomp.clear()
byprod.clear()
byprod_active.clear()
byprod_active_update.clear()

## -- fourth round run -- ##

datagather()
parse()
createPRDO()
byProd_Active()
reportcomp()
errortable()

## -- Start Production Order processing in SAP fourth run -- ##

createPRDO_inSAP()
byproduct_updateSAP()
receipt_inSAP()
close_PRDO()
process_Error()

## -- Send Process Report to FTP -- ##
ftpfile.alert()
