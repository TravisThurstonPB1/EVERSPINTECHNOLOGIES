import xlrd
from xlrd import XL_CELL_EMPTY
from xlutils.copy import copy as xl_copy
import xlwt
import datetime
import glob
import re
import mysqllogin
import pypyodbc
import os
import csv
import os.path
from Sharepoint.SharepointUpload_py3 import SharepointSAPtoThirdRep
import cred


currentday = datetime.date.today()		## Gets the current Days date
filedate = currentday.strftime('%Y%m%d')


def fileCreate():
	sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=10.0.0.6\SAPB1_SQL; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))		## Create Connection to SQL Server 
	cursor = sapsqlcon.cursor()		## Create Cursor Object
	
	onhandmatch=[('Stage','Family','SAPItemCode', 'SAPLotNumber', 'SAPParentLot', 'SAPOnHand', 'SAPOnHandCost', 'SAPWIPItemCode', 'SAPWIPLot', 'SAPWIPParentLot', 'SAPWIPOnHand', 'SAPWIPOnHandCost', '3PItemCode', '3POriginalItemCode', '3PLot', '3PParentLot', '3POnHand', '3POnHandCost', 'VendorFileName', 'AsOfDate')]
	
	print("Start SAP Match Gather...")
	query1 = ("Select * from EverspinTech.dbo.vw_SAP_3P_Match")
	cursor.execute(query1)
	results = cursor.fetchone()
	
	while results:
		onhandmatch.append(results)
		results = cursor.fetchone()
	
	
	book = xlwt.Workbook()		## Create New Workbook
	sheet1 = book.add_sheet("Match")		## Create New sheet in workbook
	for r, row in enumerate(onhandmatch):		## iterate through .csv file rows
		for c, col in enumerate(row):		## iterate through .csv file columns
			sheet1.write(r,c,col)		## write data to new file
	book.save('C:\\PB1\\SAPto3PReport\\SAPto3PDetailsTest_'+filedate+'.xls')		## Save new File as .xls

	
# ## ---------------------------- ## Adding Partial Match Sheet
	
	partialmatch =[('Stage','Family','SAPItemCode', 'SAPLotNumber', 'SAPParentLot', 'SAPOnHand', 'SAPOnHandCost', 'SAPWIPItemCode', 'SAPWIPLot', 'SAPWIPParentLot', 'SAPWIPOnHand', 'SAPWIPOnHandCost', '3PItemCode', '3POriginalItemCode', '3PLot', '3PParentLot', '3POnHand', '3POnHandCost', 'VendorFileName', 'AsOfDate', 'Delta')]
	
	print("Start Partial match gather...")
	query2 = ("Select * from EverspinTech.dbo.vw_SAP_3P_PartialMatch")
	cursor.execute(query2)
	results2 = cursor.fetchone()
	
	while results2:
		partialmatch.append(results2)
		results2 = cursor.fetchone()

	
	openbook = xlrd.open_workbook('C:\\PB1\\SAPto3PReport\\SAPto3PDetailsTest_'+filedate+'.xls', formatting_info=True)  ##Open the exisitng workbook
	book = xl_copy(openbook)   ## Create Copy of the open workbook.
	sheet2 = book.add_sheet("Partial_Match")		## Create New sheet in workbook
	for r, row in enumerate(partialmatch):		## iterate through .csv file rows
		for c, col in enumerate(row):		## iterate through .csv file columns
			sheet2.write(r,c,col)		## write data to opened workbook file
	book.save('C:\\PB1\\SAPto3PReport\\SAPto3PDetailsTest_'+filedate+'.xls')		## Save updates to .xls File
	
	
## -------------------------- ## Gather Data from the No Match Queries to compare together
		
	sapnomatch = []
	thrdnomatch = []
	matches = [('Stage','Family','SAPItemCode', 'SAPLotNumber', 'SAPParentLot', 'SAPOnHand', 'SAPOnHandCost', 'SAPWIPItemCode', 'SAPWIPLot', 'SAPWIPParentLot', 'SAPWIPOnHand', 'SAPWIPOnHandCost', '3PItemCode', '3POriginalItemCode', '3PLot', '3PParentLot', '3POnHand', '3POnHandCost', 'VendorFileName', 'AsOfDate', 'Delta')]
	sapnomatches = []
	thrdnomatches = [('Stage','Family','SAPItemCode', 'SAPLotNumber', 'SAPParentLot', 'SAPOnHand', 'SAPOnHandCost', 'SAPWIPItemCode', 'SAPWIPLot', 'SAPWIPParentLot', 'SAPWIPOnHand', 'SAPWIPOnHandCost', '3PItemCode', '3POriginalItemCode', '3PLot', '3PParentLot', '3POnHand', '3POnHandCost', 'VendorFileName', 'AsOfDate')]
	
	print("Start SAP No Match Gather...")
	query3 = ("Select * from EverspinTech.dbo.vw_SAPNoMatch")
	cursor.execute(query3)
	results3 = cursor.fetchone()
	
	while results3:
		sapnomatch.append(results3)
		results3 = cursor.fetchone()
	
	print("Start 3P No Match Gather...")
	query4 = ("Select * from EverspinTech.dbo.vw_3PNoMatch")
	cursor.execute(query4)
	results4 = cursor.fetchone()
	
	while results4:
		thrdnomatch.append(results4)
		results4 = cursor.fetchone()

	print("SAPnomatch ", len(sapnomatch))
	print("3rdnomatch ", len(thrdnomatch))
	
	print("Comparing data....")
	for x in sapnomatch:
		stage, fam, sapitem, saplot, sapplot, sapoh, sapohcost, wipitem, wiplot, wipplot, wipoh, wipohcost, thrditem, orgthrditem, thrdlot, thrdplot, thrdoh, thrdohcost, date = x
		if sapitem == '':
			for y in thrdnomatch:
				ystage, yfam, ysapitem, ysaplot, ysapplot, ysapoh, ysapohcost, ywipitem, ywiplot, ywipplot, ywipoh, ywipohcost, ythrditem, yorgthrditem, ythrdlot, ythrdplot, ythrdoh, ythrdohcost, yvendor , ydate = y
				
				if wipplot =='':
					if wiplot == ythrdplot:
						result = (stage, fam, sapitem, saplot, sapplot, sapoh, sapohcost, wipitem, wiplot, wipplot, wipoh, wipohcost, ythrditem, yorgthrditem, ythrdlot, ythrdplot, ythrdoh, ythrdohcost, yvendor, ydate, (sapoh+wipoh)-ythrdoh)
						matches.append(result)
						thrdnomatch.remove(y)
				elif wipplot == ythrdplot:
					result = (stage, fam, sapitem, saplot, sapplot, sapoh, sapohcost, wipitem, wiplot, wipplot, wipoh, wipohcost, ythrditem, yorgthrditem, ythrdlot, ythrdplot, ythrdoh, ythrdohcost, yvendor, ydate, (sapoh+wipoh)-ythrdoh)
					matches.append(result)
					thrdnomatch.remove(y)
					
				else:
					if x in sapnomatches:
						pass
					else:
						sapnomatches.append(x)
			# sapnomatch.remove(x)
				
		else:
			for y in thrdnomatch:
				ystage, yfam, ysapitem, ysaplot, ysapplot, ysapoh, ysapohcost, ywipitem, ywiplot, ywipplot, ywipoh, ywipohcost, ythrditem, yorgthrditem, ythrdlot, ythrdplot, ythrdoh, ythrdohcost, yvendor, ydate = y
				
				if sapplot=='':
					if saplot==ythrdplot:
						result = (stage, fam, sapitem, saplot, sapplot, sapoh, sapohcost, wipitem, wiplot, wipplot, wipoh, wipohcost, ythrditem, yorgthrditem, ythrdlot, ythrdplot, ythrdoh, ythrdohcost, yvendor, ydate, (sapoh+wipoh)-ythrdoh)
						matches.append(result)
						thrdnomatch.remove(y)
					elif saplot==ythrdlot:
						result = (stage, fam, sapitem, saplot, sapplot, sapoh, sapohcost, wipitem, wiplot, wipplot, wipoh, wipohcost, ythrditem, yorgthrditem, ythrdlot, ythrdplot, ythrdoh, ythrdohcost,yvendor, ydate, (sapoh+wipoh)-ythrdoh)
						matches.append(result)
						thrdnomatch.remove(y)
				elif sapplot == ythrdplot:
					result = (stage, fam, sapitem, saplot, sapplot, sapoh, sapohcost, wipitem, wiplot, wipplot, wipoh, wipohcost, ythrditem, yorgthrditem, ythrdlot, ythrdplot, ythrdoh, ythrdohcost, yvendor, ydate, (sapoh+wipoh)-ythrdoh)
					matches.append(result)
					thrdnomatch.remove(y)
				elif saplot == ythrdlot:
					result = (stage, fam, sapitem, saplot, sapplot, sapoh, sapohcost, wipitem, wiplot, wipplot, wipoh, wipohcost, ythrditem, yorgthrditem, ythrdlot, ythrdplot, ythrdoh, ythrdohcost, yvendor, ydate, (sapoh+wipoh)-ythrdoh)
					matches.append(result)
					thrdnomatch.remove(y)
			
				else:
					if x in sapnomatches:
						pass
					else:
						sapnomatches.append(x)
			# sapnomatch.remove(x)
					
	for i in thrdnomatch:
		thrdnomatches.append(i)
	
	print("Sap No match before set list ", len(sapnomatches))
	
	cleanedlist = list(set(sapnomatches))

	cleanedlist2 = [('Stage','Family','SAPItemCode', 'SAPLotNumber', 'SAPParentLot', 'SAPOnHand', 'SAPOnHandCost', 'SAPWIPItemCode', 'SAPWIPLot', 'SAPWIPParentLot', 'SAPWIPOnHand', 'SAPWIPOnHandCost', '3PItemCode', '3POriginalItemCode', '3PLot', '3PParentLot', '3POnHand', '3POnHandCost', 'AsOfDate')]
	
	for n in cleanedlist:
		cleanedlist2.append(n)
		
	print("matches ", len(matches))	
	print("SAP after comparison ", len(cleanedlist2))
	print("3rd after comparison ", len(thrdnomatch))
	
	
	openbook = xlrd.open_workbook('C:\\PB1\\SAPto3PReport\\SAPto3PDetailsTest_'+filedate+'.xls', formatting_info=True)  ##Open the exisitng workbook
	book = xl_copy(openbook)   ## Create Copy of the open workbook.
	sheet3 = book.add_sheet("No_Match_Compare")		## Create New sheet in workbook
	for r, row in enumerate(matches):		## iterate through .csv file rows
		for c, col in enumerate(row):		## iterate through .csv file columns
			sheet3.write(r,c,col)    ## write data to opened workbook file
	book.save('C:\\PB1\\SAPto3PReport\\SAPto3PDetailsTest_'+filedate+'.xls')
	
	openbook = xlrd.open_workbook('C:\\PB1\\SAPto3PReport\\SAPto3PDetailsTest_'+filedate+'.xls', formatting_info=True)  ##Open the exisitng workbook
	book = xl_copy(openbook)   ## Create Copy of the open workbook.
	sheet4 = book.add_sheet("No_Match_SAP")		## Create New sheet in workbook
	for r, row in enumerate(cleanedlist2):		## iterate through .csv file rows
		for c, col in enumerate(row):		## iterate through .csv file columns
			sheet4.write(r,c,col)   ## write data to opened workbook file
	book.save('C:\\PB1\\SAPto3PReport\\SAPto3PDetailsTest_'+filedate+'.xls')
	
	openbook = xlrd.open_workbook('C:\\PB1\\SAPto3PReport\\SAPto3PDetailsTest_'+filedate+'.xls', formatting_info=True)  ##Open the exisitng workbook
	book = xl_copy(openbook)   ## Create Copy of the open workbook.
	sheet5 = book.add_sheet("No_Match_3P")		## Create New sheet in workbook
	for r, row in enumerate(thrdnomatches):		## iterate through .csv file rows
		for c, col in enumerate(row):		## iterate through .csv file columns
			sheet5.write(r,c,col)		## write data to opened workbook file		
	book.save('C:\\PB1\\SAPto3PReport\\SAPto3PDetailsTest_'+filedate+'.xls')		## Save updates to .xls File

					
	
	cursor.close()
	sapsqlcon.close()
	
	
	## --- Summary Tab creation --- ##
	
	totalrecords = (len(onhandmatch)+len(partialmatch)+len(matches)+len(cleanedlist2)+len(thrdnomatch))-5
	percntmatch = ((len(onhandmatch)-1)/totalrecords)*100
	percntpart = ((len(partialmatch)-1)/totalrecords)*100
	percntcomp = ((len(matches)-1)/totalrecords)*100
	percntsap = ((len(cleanedlist2)-1)/totalrecords)*100
	percnt3p = ((len(thrdnomatch)-1)/totalrecords)*100
	
	summaryinfo =[('Total Records', 'Matching', 'Partial match (on hand)', 'Comparison of No Match', 'SAP No Match', '3P No Match')]
	numrecords = (totalrecords, len(onhandmatch)-1, len(partialmatch)-1, len(matches)-1, len(cleanedlist2)-1, len(thrdnomatch)-1)
	recpercnt = ('Percentage', percntmatch, percntpart, percntcomp, percntsap, percnt3p)
	summaryinfo.append(numrecords)
	summaryinfo.append(recpercnt)
	
	openbook = xlrd.open_workbook('C:\\PB1\\SAPto3PReport\\SAPto3PDetailsTest_'+filedate+'.xls', formatting_info=True)  ##Open the exisitng workbook
	book = xl_copy(openbook)   ## Create Copy of the open workbook.
	sheet6 = book.add_sheet("Summary")		## Create New sheet in workbook
	for r, row in enumerate(summaryinfo):		## iterate through .csv file rows
		for c, col in enumerate(row):		## iterate through .csv file columns
			sheet6.write(r,c,col)		## write data to opened workbook file		
	book.save('C:\\PB1\\SAPto3PReport\\SAPto3PDetailsTest_'+filedate+'.xls')		## Save updates to .xls File
	
	
	## --- Upload File to Sharepoint --- ##
	
	folderurl = '/CP/Shared Documents/Automation/SAP to 3P Detail Automation'
	
	findfile = glob.glob('C:\\PB1\\SAPto3PReport\\SAPto3PDetailsTest_'+filedate+'.xls')
	if findfile != []:
		loc = findfile[0]
		
	file_name = loc
	file_create = 'SAPto3PDetails_'+filedate+'.xls'
	
	start_upload = SharepointSAPtoThirdRep()
	start_upload.main(file_create, file_name, folderurl)
	
##------------------------------------------##
	
fileCreate()
