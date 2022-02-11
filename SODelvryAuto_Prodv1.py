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
import SOAutoProcessReport

username = cred.sluid
password = cred.slpwd

httpsuccess=[200,201,204]
spinwebdata = []
orderupdate = []
manerror = []
ordr = {}
rdr1 = []
odln = {}
dln1 = []
dlnbatch = []
deltable = []




logindict = {"UserName": username, "Password": password, "CompanyDB": "EverspinTech"}
x = requests.post('https://everspinsap2:50000/b1s/v1/Login', json=logindict, verify=False)
logcookies = x.cookies.get_dict()


def datagather():
    print("Starting Spinweb Bulk Data gather...")
    mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', port=3306, database='mtsdb')
    cursor1 = mysqlcon.cursor()
    connection = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor = connection.cursor()
    
    mySQLquery=("""Select T0.orderPartNumber, T0.waferFamily, DATE_FORMAT(T0.shipDate, "%Y%m%d") 'shipDate'
                , CASE WHEN RIGHT(LEFT(T1.ftEndLotID,7), 1) != 'C' THEN
                    CASE WHEN T1.shipQty < T1.parentLotQty THEN T1.shipQty ELSE T1.parentLotQty End
                    ELSE T1.parentLotQty END 'LotQty' 
                , T0.orderBatchID, T0.trackingNo, T0.InvoiceNo, T1.ftEndLotID 'Lot', T2.shipStatus

                from mtsdb.tblDropShipment T0
                INNER JOIN mtsdb.tblDropShipmentTrace T1 on T0.shipmentID = T1.shipmentID
                INNER JOIN mtsdb.tblDropShipmentOrderBatch T2 on T0.orderBatchID = T2.orderBatchID

                Where T0.shipDate between DATE_ADD(CURDATE(), INTERVAL -15 day) and DATE_ADD(CURDATE(), INTERVAL 1 day) and T2.shipStatus = 'Complete'""")
    cursor1.execute(mySQLquery)
    result = cursor1.fetchone()
    
    while result:
        spinwebdata.append(result)
        result = cursor1.fetchone()
        
    for x in spinwebdata:
        itemcode, family, shipdate, shipqty, soline, tracking, inv, lot, status = x
        sonum = soline.split('-')[0]
        linenum = soline.split('-')[1]
        
        query1 = ("""INSERT INTO VALIDATION.dbo.SO_AUTO_TO_PROCESS (ItemCode, Family, ShipDate, ShipQty, SOBatchID, TrackingNo, InvoiceNo, LotNo, Status, SONum, LineNum)
                    values
                    ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}')""".format(itemcode, family, shipdate, shipqty, soline, tracking, inv, lot, status, sonum, linenum))
        cursor.execute(query1)
        
    print("Finished Spinweb bulk Data gather")
    cursor1.close()
    mysqlcon.close()
    cursor.commit()
    cursor.close()
    connection.close()
    

def parse():
    print("Starting data parse for Spinweb SO records gathered for SAP handling...")
    connection = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor = connection.cursor()
    connection2 = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor2 = connection2.cursor()
    
    
    for x in spinwebdata:
        itemcode, family, shipdate, shipqty, soline, tracking, inv, lot, status = x 
        
        sonum = soline.split('-')[0]
        linenum = soline.split('-')[1]
        
        # print(sonum, linenum)
        if status.lower() == 'complete':
            query1 = ("""Select T0.LineNum, T0.VisOrder, T0.LineStatus, T0.Quantity, T0.ItemCode, T1.DocNum, T0.DocEntry
                        from EverspinTech.dbo.RDR1 T0
                        INNER JOIN EverspinTech.dbo.ORDR T1 on T0.DocEntry = T1.DocEntry
                        WHERE T1.DocNum = '{0}' and CASE WHEN T1.DocDate <= '20220101' then T0.LineNum Else T0.VisOrder end = '{1}'""".format(sonum, linenum))
            cursor.execute(query1)
            result1 = cursor.fetchone()
            
            if result1 != None:
                if result1[2] == 'O':
                    query2=("""Select T0.ItemCode, T0.DistNumber, SUM(T1.Quantity) 'Qty'

                                from EverspinTech.dbo.OBTN T0
                                INNER JOIN EverspinTech.dbo.ITL1 T1 on T0.SysNumber = T1.SysNumber and T0.ItemCode = T1.ItemCode

                                Where T0.DistNumber = '{0}' and T0.ItemCode = '{1}'
                                Group by T0.ItemCode, T0.DistNumber""".format(lot, itemcode))
                    cursor.execute(query2)
                    result2 = cursor.fetchone()
                    
                    if result2 != None:
                        if result2[2] >= shipqty:
                            rdr1.append({"LineNum": result1[0],
                                        "U_ActualShipDate": shipdate,
                                        "U_TrackNum": tracking,
                                        "U_LineStatus": "S"
                                        })
                            ordr["DocumentLines"]=rdr1
                            
                            # print(ordr)
                            # print(sonum)
                            # test = requests.get("https://everspinsap2:50000/b1s/v1/DeliveryNotes({0})".format('17722'), cookies=logcookies, verify=False)
                            # print(test.json())
                            # input()
                            
                            soupdate = requests.patch("https://everspinsap2:50000/b1s/v1/Orders({0})".format(result1[6]), json=ordr, cookies=logcookies, verify=False)
                            # print(soupdate)
                            # orderupdate.append(x)
                            
                            ordr.clear()
                            rdr1.clear()
                            
                            qtytotal = 0
                            
                            query3 = ("""Select * from VALIDATION.dbo.SO_AUTO_TO_PROCESS where SOBatchID = '{0}'""".format(soline))
                            # print(query3)
                            cursor2.execute(query3)
                            result3 = cursor2.fetchone()
                            if result3 == None:
                                print("Already processed")
                            else:
                                while result3:
                                    dlnbatch.append({"BatchNumber": result3[7],
                                                    "Quantity": result3[3],
                                                    "ItemCode": result3[0]
                                                    })
                                    
                                    qtytotal = int(result3[3]) + qtytotal
                                    result3 = cursor2.fetchone()
                                    
                                    
                                query4 = ("""DELETE FROM VALIDATION.dbo.SO_AUTO_TO_PROCESS WHERE SOBatchID = '{0}'""".format(soline))
                                cursor2.execute(query4)
                                
                                odln["DocDate"]=shipdate
                                odln["Comments"]="PIO Auto Delivery Process Via ServiceLayer.  Based on Sales Order {0}".format(sonum)
                                dln1.append({"BaseEntry": result1[6],
                                            "BaseLine": result1[0],
                                            "BaseType": "17",
                                            "U_LineStatus": "S"
                                            })
                                
                                dln1[0]["BatchNumbers"]=dlnbatch
                                odln["DocumentLines"]=dln1
                                # print(odln)
                                try:
                                    delupdate = requests.post("https://everspinsap2:50000/b1s/v1/DeliveryNotes", json=odln, cookies=logcookies, verify=False)
                                    # print(delupdate.json())
                                    if delupdate.status_code in httpsuccess:
                                        toupdate = (soline, delupdate.json()["DocEntry"], itemcode, lot, qtytotal, shipdate, tracking)
                                        deltable.append(toupdate)
                                        qtytotal = 0
                                    else:
                                        slerror = (soline, itemcode, lot, shipqty, delupdate.json()["error"]["message"]["value"])
                                        manerror.append(slerror)
                                except:
                                    raise
                                
                                odln.clear()
                                dln1.clear()
                                dlnbatch.clear()
                            
                        else:
                            dlerror = (soline, itemcode, lot, shipqty, "Not enough on hand in SAP.  SAP OH {0}, ship Qty {1}".format(result2[2], shipqty))
                            manerror.append(dlerror)
                            # print("not enough on hand in SAP.  SAP OH {0}, ship Qty {1}".format(result2[2]), shipqty)
                    else:
                        dlerror = (soline, itemcode, lot, shipqty, "Lot {0} does not exist in SAP".format(lot))
                        manerror.append(dlerror)
                        # print("lot {0} does not exist in SAP".format(lot))
                        
                # print(result1[0], '<--LineNum, ', result1[1], '<--VisOrder, ', linenum, '<--batchline, ',  result1[2], ', ', result1[3], ', ', result1[4], '<--SAP Item, ', itemcode, '<--SpinWeb Item,', result1[5], ', ', sonum)
                else:
                    pass
                    # print("line closed")
            else:
                pass
        else:
            swerror = (soline, itemcode, lot, shipqty, "Error Record from Spinweb")
            manerror.append(swerror)
            # print("error")
    query5 = ("""TRUNCATE TABLE VALIDATION.dbo.SO_AUTO_TO_PROCESS""")
    cursor2.execute(query5)
    
    cursor.close()
    connection.close()
    cursor2.commit()
    cursor2.close()
    connection2.close()
    

def table_populate():
    print("Starting table population post processing...")
    connection = pyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
    cursor = connection.cursor()
    
    for x in deltable:
        soline, delno, item, lot, qty, shipdate, tracking = x 
        query1 = ("""Select * from VALIDATION.dbo.DLN_AUTO WHERE SOBatchID = '{0}' and DeliveryNo = '{1}'""".format(soline, delno))
        cursor.execute(query1)
        result1 = cursor.fetchone()
        
        if result1 != None:
            pass
        else:
            query2 = ("""INSERT INTO VALIDATION.dbo.DLN_AUTO (SOBatchID, DeliveryNo, ItemCode, LotNo, Quantity, ShipDate, TrackingNo, CreateDate)
                        values 
                        ('{0}','{1}','{2}','{3}','{4}','{5}','{6}',GETDATE())""".format(soline, delno, item, lot, qty, shipdate, tracking))
            cursor.execute(query2)
        
    for y in manerror:
        soline, item, lot, qty, err = y 
        query3 = ("""Select * from VALIDATION.dbo.ERROR_ENTRY_DLN where SOBatchID = '{0}' and LotNo = '{1}'""".format(soline, lot))
        cursor.execute(query3)
        result2 = cursor.fetchone()
        
        if result2 != None:
            pass
        else:
            query4 = ("""INSERT INTO VALIDATION.dbo.ERROR_ENTRY_DLN (SOBatchID, ItemCode, LotNo, Quantity, ErrorReason, ErrorDate)
                        values
                        ('{0}','{1}','{2}','{3}','{4}', GETDATE())""".format(soline, item, lot, qty, err))
            cursor.execute(query4)
            
    cursor.commit()
    cursor.close()
    connection.close()
    
    print("Completed table population")

########################################
datagather()
parse()
table_populate()

## -- Trigger Report -- ##
SOAutoProcessReport.alert()
