import sys
import pypyodbc
import pymysql
import re 
import itertools
import time
import mysqllogin

start_time=time.time()

preimportEVS = []
parse1 = []
sapimport = []


### -- Gather data from Spinweb -- ###
def dataGather():
	print ("\n Starting Final Test Invoice Data Gathering...")
	mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', database='mtsdb')
	cursor1 = mysqlcon.cursor()
	mySQLcomVT1 = ("select distinct invNumber from mtsdb.tblTestInvoice T0 inner join mtsdb.tblTestInvoiceFile T1 on T0.fileID = T1.fileID where ifnull(T0.itemCode,'') != '' and T0.invDate >= date_add(curdate(), interval -120 day) and T1.validated = 1")
	cursor1.execute(mySQLcomVT1)
	results = cursor1.fetchone()

	while results:
		preimportEVS.append(results)
		results = cursor1.fetchone()
		
	print ("\n Spinweb Final Test Invoice data gathering complete.")
	print("\n -----{} seconds------".format(time.time()-start_time))
	cursor1.close()
	mysqlcon.close()
	
	
### -- Validated data and import to FINAL_TEST_INVOICE Table -- ###
def dataParse():
	print ("\n Starting Final Test Data validation Parse...")
	connection = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))
	cursor = connection.cursor()
	
	mysqlcon = pymysql.connect(user=mysqllogin.user, password=mysqllogin.password, host='10.10.60.198', database='mtsdb')
	cursor1 = mysqlcon.cursor()
	
	try:
		for x in preimportEVS:
			invnum = x[0] 
			test1 = ("Select DocNum from EverspinTech.dbo.OPCH where NumAtCard = '{0}'".format(invnum))
			cursor.execute(test1)
			result = cursor.fetchone()
			if result == None:
				parse1.append(x)
			else:
				print("Invoice Exists in SAP.  Vendor Invoice Number {0}".format(invnum))
		print ("\n Spinweb Final Test Invoice validation complete.")
		print("\n -----{} seconds------".format(time.time()-start_time))
		

		print("\n Gathering Final Test Invoice Data for valid values")
		for x in parse1:
			invnum = x[0] 
			mySQLcomVT1 = ("select 'V000979' as 'CardCode', T0.invNumber, T1.fileName as 'InternalRemarks', T0.itemCode, sum(T0.opnQty) 'opnQty', T0.unitPrice, 'T_UTC' as 'Warehouse', CASE WHEN substring_index(T0.itemCode, '-', 1) like '%Panther16%' THEN 'Panth16' WHEN substring_index(T0.itemCode, '-', 1) like '%Panther8%' THEN 'Panth8' WHEN substring_index(T0.itemCode, '-', 1) like '%Condor%' then 'Condor' WHEN substring_index(T0.itemCode, '-', 1) like '%Lynx%' then 'Lynx' When substring_index(T0.itemCode, '-', 1) like '%Spider%' then 'Spider'	WHEN substring_index(T0.itemCode, '-', 1) like '%Acari%' then 'Acari' WHEN substring_index(T0.itemCode, '-', 1) like '%Ike%' then 'Ike' WHEN substring_index(T0.itemCode, '-', 1) like '%Logan%' then 'Logan'	WHEN substring_index(T0.itemCode, '-', 1) like '%Mantis%' then 'Mantis'	WHEN substring_index(T0.itemCode, '-', 1) like '%McKinley%' then 'McKinley' WHEN substring_index(T0.itemCode, '-', 1) like '%Sensor%' then 'Sensor' WHEN substring_index(T0.itemCode, '-', 1) like '%Aeroflex%' then 'Aeroflex' When substring_index(T0.itemCode, '-', 1) like '%Puma%' then 'Puma'	WHEN substring_index(T0.itemCode, '-', 1) like '%Foundry%' then 'Foundry' WHEN substring_index(T0.itemCode, '-', 1) like '%Bobcat%' then 'Bobcat' End as 'Family', T0.opnSequence 'spinTransID'from mtsdb.tblTestInvoice T0 left join mtsdb.tblTestInvoiceFile T1 on T0.fileID = T1.fileID Where ifnull(T0.itemCode,'') != '' and T0.opnSequence != 0  and T0.invNumber = '{0}' Group by T0.invNumber, T1.fileName, T0.itemCode, T0.unitPrice, T0.opnSequence order by T0.invNumber, T0.itemCode, T0.opnQty".format(invnum))
			cursor1.execute(mySQLcomVT1)
			results = cursor1.fetchone()
			
			while results:
				sapimport.append(results)
				results = cursor1.fetchone()
		print ("\n Spinweb Final Test Invoice data gathering complete.")
		print("\n -----{} seconds------".format(time.time()-start_time))		
		
		print("\n Insert of Final Test Data start....")
		for x in sapimport:
			cardcode, numatcard, intrem, itemcode, qty, price, whs, family, spntrnsid = x
			test1 = ("Select top 1 * from VALIDATION.dbo.PROCESSED_FINAL_TEST_INVOICE where NumAtCard = '{0}' and ItemCode = '{1}' and Quantity = '{2}' and Price = '{3}' and spinTransID = '{4}'".format(numatcard, itemcode,qty, price, spntrnsid))
			cursor.execute(test1)
			result = cursor.fetchone()
			if result == None:
				test2 = ("Select top 1 * from VALIDATION.dbo.FINAL_TEST_INVOICE where NumAtCard = '{0}' and ItemCode = '{1}' and Quantity = '{2}' and Price = '{3}' and spinTransID = '{4}'".format(numatcard, itemcode,qty, price, spntrnsid))
				cursor.execute(test2)
				result2 = cursor.fetchone()
				if result2 == None:
					query = ("Insert Into VALIDATION.dbo.FINAL_TEST_INVOICE (CardCode, NumAtCard, InternalRemarks, ItemCode, Quantity, Price, WhsCode, Family, spinTransID) values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}')".format(cardcode, numatcard, intrem, itemcode, qty, price, whs, family, spntrnsid))
					cursor.execute(query)
				else:
					print("record exists", numatcard, itemcode, qty, spntrnsid)
					#pass
			else:
				print("Record has been processed and is a Draft in SAP.  Vendor Invoice Number {0}".format(numatcard))
				#pass
	except:
		print ("An error occured In Processing Error Entry Information.")
		raise
		
	cursor.commit()
	cursor.close()
	connection.close()
	cursor1.close()
	mysqlcon.close()
	print ("Insert Final Test Data Information Complete.")
	print("\n -----{} seconds------".format(time.time()-start_time))
	

###Script Start####

dataGather()
dataParse()
					