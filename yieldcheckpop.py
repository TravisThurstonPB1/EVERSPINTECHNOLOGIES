import sys
import pypyodbc
import csv
import pymysql
import re 
import itertools
import time
import mysqllogin

start_time = time.time()

yieldPop = []

def dataGather():
	print ("Starting Data Gathering from Spinweb...")
	mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', port=3306, database='mtsdb')
	cursor1 = mysqlcon.cursor()
	query = ("SELECT assyLot, endLot, workOrder, startQty, endQty, flowCode, opnCodeEnd, targetDevice, transTime, startLot from mtsdb.tblFTLotEndTrans where transTime >= date_add(curdate(), interval -1 day) and opnCodeEnd not in ('C600', 'C6B1', 'C6B2', 'C601', 'LOST', 'C699','C5ENG')")
	cursor1.execute(query)
	results = cursor1.fetchone()

	
	while results:
		yieldPop.append(results)
		results = cursor1.fetchone()


	print ("Spinweb Test data gathering complete.")
	
	print("\n -----{} seconds------".format(time.time()-start_time))
	
	cursor1.close()
	mysqlcon.close()
	
	
	
def uploadData():
	print ("Starting Data Upload to YIELD_CHECK Table...")
	connection = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
	cursor = connection.cursor()
	
	for x in yieldPop:
		asemlot, endlot, workorder, startqty, endqty, flow, opncodeend, item, date, startlot = x
		test1 = ("Select * from VALIDATION.dbo.YIELD_CHECK where AssyLot = '{0}' and EndLot = '{1}' and WorkOrder = '{2}' and EndQty = '{3}'".format(asemlot,endlot,workorder,endqty))
		cursor.execute(test1)
		result = cursor.fetchone()
		if result == None:
			query = ("INSERT INTO VALIDATION.dbo.YIELD_CHECK (AssyLot, EndLot, WorkOrder, StartQty, EndQty, FlowCode, OpnCodeEnd, ItemCode, CreateDate, StartLot) values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}')".format(asemlot, endlot, workorder, startqty, endqty, flow, opncodeend, item, date, startlot))
			cursor.execute(query)
		else:
			pass
			
		
	cursor.commit()
	cursor.close()
	connection.close()
	
	print("YIELD_CHECK table population Complete.")
	
	print("\n -----{} seconds------".format(time.time()-start_time))
	
	
#### Start ####

dataGather()
uploadData()