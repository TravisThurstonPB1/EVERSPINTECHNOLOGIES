from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import json, os
import datetime
import time

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
config_path = '\\'.join([ROOT_DIR, 'config.json'])

# read json file
with open(config_path) as config_file:
    config = json.load(config_file)
    config = config['share_point']

sharepoint_user = config['user']
sharepoint_password = config['password']
sharepoint_base_url = config['url']
folder_in_sharepoint = config['site']
utc_key_file_in_sharepoint = config['utc_key_file_path']

# Generating current day folder name.
date = datetime.datetime.now()
if date.month < 10:
    month = '0'+str(date.month)
else:
    month = date.month

date_folder = str(date.day)+str(month)+str(date.year)+"/"
folder_in_sharepoint = folder_in_sharepoint+date_folder
# Output file with todays date.
output_xlsx = 'output - '+str(date.day)+'-'+str(month)+'-'+str(date.year)+'.xlsx'

def delete_downloaded_files():
    if os.path.exists("UTC Key.xlsx"):
        os.remove("UTC Key.xlsx")
    if os.path.exists("Amkor_WIP_Report.xls"):
        os.remove("Amkor_WIP_Report.xls")
    if os.path.exists("DAILY_WIP_DG.xls"):
        os.remove("DAILY_WIP_DG.xls")
    if os.path.exists("EVERSPIN_AssyWIP.xls"):
        os.remove("EVERSPIN_AssyWIP.xls")
    if os.path.exists("inventory_report.xls"):
        os.remove("inventory_report.xls")
    if os.path.exists("MSINV076.xls"):
        os.remove("MSINV076.xls")
    if os.path.exists("OSE WIP Report - EVERSPIN.xls"):
        os.remove("OSE WIP Report - EVERSPIN.xls")
    if os.path.exists("Promis_EWS_WIP.xls"):
        os.remove("Promis_EWS_WIP.xls")
    if os.path.exists("Subleger.xls"):
        os.remove("Subleger.xls")
    if os.path.exists("UTC_EVERSPIN_WIP_REPORT.xls"):
        os.remove("UTC_EVERSPIN_WIP_REPORT.xls")
    if os.path.exists("wpsp006a_Everspin_ASSY.xls"):
        os.remove("wpsp006a_Everspin_ASSY.xls")
    if os.path.exists(output_xlsx):
        os.remove(output_xlsx)

# Checking old files and deleting it.
print("Deleting old downloaded xls files if any.")
delete_downloaded_files()
time.sleep(5)

class SharePoint():

    # Generating Auth Access
    auth = AuthenticationContext(sharepoint_base_url)
    auth.acquire_token_for_user(sharepoint_user, sharepoint_password)
    ctx = ClientContext(sharepoint_base_url, auth)
    web=ctx.web
    ctx.load(web)
    ctx.execute_query()
    print('Connected to SharePoint: ',web.properties['Title'])


    # Collecting files names from the folder
    def folder_details(ctx, folder_in_sharepoint):  
        folder = ctx.web.get_folder_by_server_relative_url(folder_in_sharepoint)
        folder_names = []
        sub_folders = folder.files
        ctx.load(sub_folders)
        ctx.execute_query()
        for each_folder in sub_folders:
            folder_names.append(each_folder.properties["Name"])
        return folder_names

    # Passing auth ctx and folder path
    file_list = folder_details(ctx, folder_in_sharepoint)

    # Reading File from SharePoint Folder and saving it in local.
    print("Downloading 3P & Subledger file from sharepoint")
    for each_file in file_list:    
        sharepoint_file = folder_in_sharepoint+each_file
        file_response = File.open_binary(ctx, sharepoint_file)
        print(file_response)

        with open(each_file, 'wb') as output_file:
            output_file.write(file_response.content)


    # Downloading the UTC Key file from the sharepoint
    print("Downloading UTC Key.xlsx file from sharepoint")
    sharepoint_file = utc_key_file_in_sharepoint+"UTC Key.xlsx"
    file_response = File.open_binary(ctx, sharepoint_file)
    print(file_response)
    with open("UTC Key.xlsx", 'wb') as output_file:
        output_file.write(file_response.content)

    
    # Processing the files and generating the output file.
    print("Processing the files and generating the output file")
    import automation

    # Uploading the output file in the output folder of sharepoint.
    with open(output_xlsx, 'rb') as content_file:
        print(output_xlsx)
        file_content = content_file.read()
        target_folder = ctx.web.get_folder_by_server_relative_url('/CP/Shared%20Documents/Automation/Subledger%20Automation/Output/')
        print("Uploading the output file in the output folder of the sharepoint.")
        target_folder.upload_file(output_xlsx,file_content)
        ctx.execute_query()


# Connecting to the SharePoint and Processing the Automation.
SharePoint()

# Deleting all downloaded excel files from sharepoint
print("Deleting all downloaded excel files from the sharepoint")
time.sleep(5)
delete_downloaded_files()

