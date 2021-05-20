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
intransitimport = []

##### Get Data from Spinweb for InTransit Data  ######
def dataGather():
	print ("\n Starting Intransit Data Gathering...")
	mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', port=3306, database='mtsdb')
	cursor1 = mysqlcon.cursor()
	evsquery = ("Select deviceID, lotID, lotQty, fromLocation, toLocation, lotType, ifnull(PONumber,'') 'PONumber', startDate, datediff(Curdate(),startDate) 'IntransitDays' from mtsdb.tblTransitLotInfo where ifnull(endDate,'') = '' and fromLocation <> 'TSMC'")
	cursor1.execute(evsquery)
	result = cursor1.fetchone()
	
	while result:
		preimportEVS.append(result)
		result = cursor1.fetchone()
		
	print ("\n Spinweb intransit data gathering complete.")
	print("\n -----{} seconds------".format(time.time()-start_time))
	cursor1.close()
	mysqlcon.close()

###### Intransit table insert ##########	
def intransitUpdate():
	print ("\n Starting Data Insert...")
	connection = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=VALIDATION; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
	cursor = connection.cursor()
	
	for x in preimportEVS:
		devid, lot, qty, fromloc, toloc, type, ponum, startdate, transdays = x
		query = ("Insert into VALIDATION.dbo.INTRANSIT (ItemCode,LotNumber,Qty,FromLocation,ToLocation,LotType,PONumber,StartDate,InTransitDays,AsOfDate) values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}',Convert(Date,GetDate(),112))".format(devid, lot, qty, fromloc, toloc, type, ponum, startdate, transdays))
		cursor.execute(query)
		
	cursor.commit()
	cursor.close()
	connection.close()
	
	print ("\n intransit data insert complete.")
	print("\n -----{} seconds------".format(time.time()-start_time))
	
	
	
##### Start Script ######

dataGather()
intransitUpdate()