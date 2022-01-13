import pypyodbc
import mysqllogin
import csv
import datetime
import glob
import sftpcred
import pysftp

## - SFTP connection Information - ##
myUsername = sftpcred.user
myHostname = sftpcred.host
myPassword = sftpcred.passWord

opts = pysftp.CnOpts()
opts.hostkeys = None

## - Date and time for file names - ##
today = datetime.datetime.today()
filedate = today.strftime('%Y%m%d_%H%M%S')
ffdt = today.strftime('%Y%m%d_%H')

def alert():
    ## Create Connection to SQL Server ##
    sapsqlcon = pypyodbc.connect('DRIVER={0}; SERVER=EverspinSQL2\SAPB1_SQL02; DATABASE=EverspinTech; UID={1}; PWD={2}'.format('SQL Server',mysqllogin.mssql_user, mysqllogin.mssql_pass))		 
    cursor = sapsqlcon.cursor()		## Create Cursor Object ##

    query = ("SELECT * FROM EverspinTech.dbo.vw_TR_Process_report ORDER BY SAP_PRDO")
    cursor.execute(query)
    # print(result)
    columns = ['SAP_PRDO', 'TR_WorkOrder', 'ItemCode', 'Status', 'CompleteQty', 'ErrorReason', 'LastUpdateTime', 'LastEditBy', 'CreditLot', 'IssueBy']
    # columns=[i[0].title() for i in cursor.description]
    # print(columns)
    # input()

    file_name = 'C:\\PB1\\TRProcessRep\\TR_Process_Report_'+ filedate +'.csv'
    # print(file_name)
    
    with open(file_name,'w',newline="") as outfile:
        report = csv.writer(outfile,delimiter=',')
        report.writerow(columns)
        report.writerows(cursor)

    cursor.close()
    sapsqlcon.close()
    
    with pysftp.Connection(host=myHostname, port=22, username=myUsername, password=myPassword, cnopts=opts) as sftp:
        print("connection succesfully established ...")
        findfile = glob.glob('C:\\PB1\\TRProcessRep\\TR_Process_Report_'+ffdt+'*') 
        # print(findfile[0].split('\\')[3])
        sftp_path = './Pioneer/ready/incoming/TRProcessReport/'+str(findfile[0].split('\\')[3])
        
        sftp.put(findfile[0],sftp_path)
        sftp.chmod(sftp_path,777)
        print("Completed file load.")
        
### -- Run Script -- ###

alert()
