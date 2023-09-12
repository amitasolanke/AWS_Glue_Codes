import sys
from awsglue.transforms import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
import botocore
import io
import csv
import xlsxwriter
import pandas as pd
import secrets
from datetime import datetime
import pytz
import re
import requests
import json
import traceback
import BridgeUtils
import datetime
import xlrd

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()

args = getResolvedOptions(sys.argv, ['JOB_NAME','auth_key','auth_pass','base_URL','Dest_S3',
        'Source_S3','Tenant_Bridge_Id','Email_Format','Domain','Service_Quantity', 'use_REST',
        'Client_Name','Default_M3_Owner','Upload_User','Vendor_Id','Folder_Name','Tenant_Id','TempDir'])
        

job.init(args['JOB_NAME'], args)

session = requests.Session()
creds = {"email": args['auth_key'],"password": args['auth_pass']}
base_URL = args['base_URL']
AuthURL = base_URL + "auth/signin"
response = session.post(url = AuthURL, json=creds)
if (response.status_code == 200):
    token = response.json()["access_token"]

session.headers.update({'Authorization': 'Bearer ' + token })
runInfoURL = base_URL + "connector/bridge-next-run-info?tenant_bridge_id=" + args['Tenant_Bridge_Id']

REST_flag = int(args['use_REST'])
logger.info("Rest status: " + str(REST_flag))


if REST_flag == 1:
    respFiles = session.get(url = runInfoURL)
    logger.info("bridge_next_info :" + respFiles.text)
else:
    logger.info("Not using REST")
    #Comment when using Rest Api
    respFiles = json.loads('{"tenant_bridge_id":"550d09a1-a81d-11eb-8e61-02f663df2a5d","last_run_on_utc":null,\
        "files":[{"file_name":"DOC-899-089858a4-1905-4cc6-a344-2a55f81a92d8-Other_Lumen_gttn_FinancialData_2023-01-17_1-L7HV2 (INTEROUTE COMMUNICATIONS LIMITED).csv",\
                "document_id":"089858a4-1905-4cc6-a344-2a55f81a92d8",\
                "original_file_name":"SID_bloomberg_Capital One Financial Corp_30020989_2021-03-08_0_0.csv","display_id":"DOC-1"}]}')


email_sep = args["Email_Format"].replace("@<domain>","").replace("<firstname>","").replace("<lastname>","")

client = boto3.client('s3')
s3 = boto3.resource('s3')
input_files = 0
output_files = 0
error_files = ""
objects_to_convert = []

resp_json = json.loads('{  "files": [],  "tenant_bridge_id": "' + args["Tenant_Bridge_Id"] + '", "tenant_bridge_run_id": "' + args["JOB_RUN_ID"] + '", "summary": "EOF"}')

input_s3 = "s" + args['Source_S3'][1:]
output_s3 = "s" + args['Dest_S3'][1:]

bucket_name = output_s3.split('//')[1].split('/')[0]
prefix = output_s3.split('//' + bucket_name + '/')[1]
bucket = s3.Bucket(bucket_name)

if REST_flag == 1:
    resp_arr = respFiles.json()["files"]
else:
    resp_arr = respFiles["files"]

for file in resp_arr:
    err = ""
    try:
        logger.info(input_s3 + file.get('file_name'))
        df_in = pd.read_csv(input_s3 + file.get('file_name'),  encoding='utf-8',low_memory=False)
    except Exception as e:
        err = "File not found in Source folder. Skipping."
    try:
        df_out = pd.read_excel(output_s3 + file.get('file_name').replace('.csv','.xlsx'), header=0)
        err = err + "File already exists in Destination folder. Skipping"
    except Exception as e:
        err = err+ ""

    logger.info("err" + err)
    resp_json["files"].append({"document_id":file.get('document_id'), "extraction_date" : "", "error":err, "status":"Error"})
    input_files = input_files + 1
    
    if err == "":
        objects_to_convert.append(input_s3 + file.get('file_name'))

logger.info("RESPJSON :" + json.dumps(resp_json))


# # Set the transfer configuration to chunk the file into parts
# config = boto3.s3.transfer.TransferConfig(
#     multipart_threshold=1024*25,  # 25MB
#     max_concurrency=10,
#     multipart_chunksize=1024*25,  # 25MB
#     use_threads=True
# )

# # Upload the file
# client.upload_file(
#     bucket,
#     bucket_name,
#     file.get('file_name'),
#     Config=config,
#     ExtraArgs={
#         'ContentType': 'application/octet-stream'
#     }
# )

# read excel input and process it
logger.info('LEN(objects_to_convert)'+str(len(objects_to_convert)))
if len(objects_to_convert) > 0:
    for infile in objects_to_convert:
        logger.info('INFILE'+infile)
        flag = True
        try:
            df = pd.read_csv(infile, encoding='utf-8',low_memory=False)
            df = df.dropna(how = 'all')
           
            df_columns = df.columns
            missing_columns = ''
            
            total_columns = []
            for col in df_columns:
                total_columns.append(col)
            
            crucial_columns = ['Billing Account Number','Charge Description', 'Charge Amount','Discount Amount', 'Invoice Date']
        
            optional_columns = ['Units', 'Charge Type Description','Service Number', 'Bill Invoice Number', 'Account Description 1',
                            'Bus Org Id', 'Internal Account Number', 'Legacy* Circuit Id', 'Product Family', 'Service ID (Supporting ID)',
                            'Product Line','Install Date','Carrier Circuit ID', 'PIID-Product',
                            'Service Line (Customer Service)', 'Service Address A', 'Location A City', 'Location A State',
                            'Location A Country', 'Service Address B',
                            'Location Z City', 'Location Z State', 'Location Z Country', 'Currency']
            
            for name in crucial_columns:
                if name not in total_columns:
                    missing_columns += name + ', '
            if len(missing_columns) > 0:
                err = missing_columns + ' are missing.'
                error_files += "File=" + infile + ", Error=" + err
                outputfile = infile.split("/")[-1].replace(".csv","")
                docidportion = "DOC-" + outputfile.replace("DOC-","").split("-")[0]
                for file in resp_json["files"]:
                    if outputfile.replace(docidportion+"-","").startswith(file['document_id']):
                        file["error"] = err
                        break
            else:
                if df.empty:
                    err  = "Empty file. No rows found. Being ignored."
                    error_files+= "File=" + infile + ", Error=" + err
                    outputfile = infile.split("/")[-1].replace(".csv","")
                    docidportion = "DOC-" + outputfile.replace("DOC-","").split("-")[0]  
                    for file in resp_json["files"]:
                        if outputfile.replace(docidportion+"-","").startswith(file['document_id']):
                            file["error"] = err
                            break   
                    continue
                
                for name in optional_columns:
                    if name not in total_columns:
                        if name == 'Currency':
                            df[name] = 'USD'
                        else:
                            df[name] = ''

      
                outputfile = infile.split('/')[-1].replace('.csv', '')
                docidportion = "DOC-" + outputfile.replace("DOC-","").split("-")[0]
                
                logger.info("outputfile: " + outputfile) 
                logger.info('TOTAL NUMBER OF ROWS: '+str(len(df.index)))
                df["Charge Description 2"]=''
                df["Custom:Toll Free Number"]=''
                i=0
                for row in df.itertuples(index=True):
                    logger.info('ROW NUMBER'+str(row))
                    this_raw=df["Charge Description"].values[i]
                    if " Customer Reference ID: " in this_raw and "-Circuit ID: " in this_raw:
                        customer_ref=this_raw.split(" Customer Reference ID: ")[1].split('-')[0]
                        ckt_ref=this_raw.split(" Customer Reference ID: ")[0].split("-Circuit ID: ")[1]
                        e=this_raw.split(" Customer Reference ID: ")[1].split('-')[1:]
                        listToStr5=' '.join([str(elem5) for elem5 in e])
                        df.at[i, "Custom:Toll Free Number"]=customer_ref+' '+ckt_ref
                        df.at[i, "Charge Description 2"]=this_raw.split(" Customer Reference ID: ")[0].split("-Circuit ID: ")[0]+' '+listToStr5
                    
                    elif " Customer Reference ID: " in this_raw:
                        customer_ref=this_raw.split(" Customer Reference ID: ")[1].split('-')[0]
                        e=this_raw.split(" Customer Reference ID: ")[1].split('-')[1:]
                        listToStr5=' '.join([str(elem5) for elem5 in e])
                        df.at[i, "Custom:Toll Free Number"]=customer_ref
                        df.at[i, "Charge Description 2"]=this_raw.split(" Customer Reference ID: ")[0]+' '+listToStr5
                    
                    elif "-Circuit ID: " in this_raw:
                        ckt_toll_withspace=this_raw.split("-Circuit ID: ")[1].split(' ')[0]
                        a = this_raw.split("-Circuit ID: ")[1].split(' ')[1:]
                        listToStr1 = ' '.join([str(elem1) for elem1 in a])
                        df.at[i, "Custom:Toll Free Number"]=ckt_toll_withspace
                        df.at[i, "Charge Description 2"]=this_raw.split("-Circuit ID: ")[0]+' '+ listToStr1
                        
                    elif "- Circuit ID: " in this_raw:
                        ckt_toll_without=this_raw.split("- Circuit ID: ")[1].split(' ')[0]
                        b = this_raw.split("- Circuit ID: ")[1].split(' ')[1:]
                        listToStr2 = ' '.join([str(elem2) for elem2 in b])
                        df.at[i, "Custom:Toll Free Number"]=ckt_toll_without
                        df.at[i, "Charge Description 2"]=this_raw.split("- Circuit ID: ")[0]+' '+ listToStr2
                        
                    elif "- Toll Free: " in this_raw: 
                        withspace_tollfree=this_raw.split("- Toll Free: ")[1].split('-')[0]
                        c=this_raw.split("- Toll Free: ")[1].split('-')[1:]
                        listToStr3 = ' '.join([str(elem3) for elem3 in c])
                        df.at[i, "Custom:Toll Free Number"] = withspace_tollfree
                        df.at[i, "Charge Description 2"] = this_raw.split("- Toll Free: ")[0]+' '+listToStr3
                    
                    elif "-Toll Free: " in this_raw:
                        without_spacetoll=this_raw.split("-Toll Free: ")[1].split('-')[0]
                        d=this_raw.split("-Toll Free: ")[1].split('-')[1:]
                        listToStr4=' '.join([str(elem4) for elem4 in d])
                        df.at[i, "Custom:Toll Free Number"] = without_spacetoll
                        df.at[i, "Charge Description 2"] = this_raw.split("-Toll Free: ")[0]+' '+listToStr4
                        
                    elif "TN Fee - " in this_raw:
                        tn_toll=this_raw.split("-")[1]
                        df.at[i, "Custom:Toll Free Number"]=tn_toll
                        df.at[i, "Charge Description 2"]=this_raw.split("-")[0]
                    
                    elif "Business White Pages Listing "  in this_raw:
                        page=this_raw.split("-")[1]
                        df.at[i, "Custom:Toll Free Number"]=page
                        df.at[i, "Charge Description 2"]=this_raw.split('-')[0]
                        
                    else:
                        df.at[i, "Custom:Toll Free Number"]=''
                        df.at[i, "Charge Description 2"] = this_raw
                    logger.info('ROW END    '+str(row))    
                    i=i+1
                    
                logger.info("for row ended:") 
                
                df['Product Name']=df["Charge Description 2"]
                df['Lumen-Custom:Toll Free Number']=df["Custom:Toll Free Number"]
            
                df['Invoice Date'] = df['Invoice Date'].astype(str)
                df['Invoice Date'] = pd.to_datetime(df['Invoice Date'])
                df['Invoice Date'] = df['Invoice Date'].dt.strftime('%Y-%m-%d')
                extraction_date = df['Invoice Date'][0]
                
                #check_it = df[(df['Charge Type Description'] != 'Recurring')&(df['Charge Type Description'] != 'Usage')].index
                check_it = df[(df['Charge Type Description'] =='Non-Recurring')|(df['Charge Type Description'] == 'NON-RECURRING')].index
                df.drop(check_it , inplace = True)
                
                
                df["Key"]= df['Billing Account Number'].astype(str).replace('nan','')+df['Service ID (Supporting ID)'].astype(str).replace('nan','')+df['Charge Description'].astype(str).replace('nan','')
                df["Key"]= df["Key"].map(str)
                df = df.sort_values(by = "Key", ascending = True)
                
                logger.info("key created") 
                 
                end = pd.to_datetime('1900-01-01') 
                df["Bill To Date"] = df["Bill To Date"].fillna(end)
                df = df.sort_values(by = ["Key","Bill To Date"], ascending = [True,True])
                df.groupby("Key").first().reset_index()
                df.drop_duplicates(subset=["Key"], keep="last", inplace=True)    #2nd DROP (GIVEN)
                df["Bill To Date"] = df["Bill To Date"].replace('1900-01-01', '')
                
                df['Vendor (Purchasing)'] = 'Lumen'
                df['Account Identifier'] = df['Billing Account Number']
                
                ###df['Charge Amount'] = df['Charge Amount'].astype('str').str.replace(',', '').str.replace(' ', '').str.replace('$', '').apply(lambda x: float(x))
                ###df['Discount Amount'] = df['Discount Amount'].astype('str').str.replace(',', '').str.replace(' ', '').str.replace('$', '').apply(lambda x: float(x))
                df['Charge Amount'] = df['Charge Amount'].astype('str').str.replace('[\s$(),]','').apply(lambda x: float(x))
                df['Discount Amount'] = df['Discount Amount'].astype('str').str.replace('[\s$(),]','').apply(lambda x: float(x))
                df["Units"]= df["Units"].map(str)
                df=df.sort_values(by=["Units"]).reset_index(drop=True)
                i=0
                for row in df.itertuples(index=True):
                    this_raw=df["Units"].values[i]
                    if this_raw < '1':
                        df.at[i, "Units"]= '1.00'
                    else:
                         df.at[i, "Units"]= this_raw
                    i=i+1
                df["Units"]=df["Units"].fillna(0)  
                df["Units"] = pd.to_numeric(df["Units"].astype(str).str.replace(r'[-\s;,:_()\/]','',regex=True),errors='coerce')
                df['Units'] = df['Units'].apply(lambda x: float(x))
                df['Revised Unit Cost'] = (df['Charge Amount'] - df['Discount Amount'])/df['Units']
                df['Unit Recurring Cost'] = df['Revised Unit Cost'].round(3)
                df['Unit Recurring Cost'].fillna(value = 0, inplace = True)
                df['Units']=df['Units'].astype(str)
                df['Units']=df.Units.str.replace(",",'')
                df['Units']= pd.to_numeric(df['Units'].astype(str).replace(",",""), errors='coerce')
                df['Units']= df['Units'].fillna(0)
                df['Units']=df['Units'].round().astype(int).replace(0,1)
                df['Service Quantity']=df['Units']
                
                logger.info("units merged")
                
                # df['Revised Unit Cost'] = df['Charge Amount'] - df['Discount Amount']
                # df['Unit Recurring Cost'] = df['Revised Unit Cost']
                # df['Unit Recurring Cost'].fillna(value = 0, inplace = True)
                
                df['Bill To Date'] = pd.to_datetime(df['Bill To Date'],  errors='coerce')
                df['Bill From Date'] = pd.to_datetime(df['Bill From Date'],  errors='coerce')
                df['Billing Frequency'] = ((df['Bill To Date'] - df['Bill From Date']).dt.days)/30
                #df['Billing Frequency'].apply(lambda x: float(x))
                #drop_bill = df[df['Billing Frequency'] < 0.5].index   # 3RD DROP ?????
                #df.drop(drop_bill, inplace = True)
                df['Billing Frequency'] = df['Billing Frequency'].round()
                #df['Billing Frequency']=df['Billing Frequency'].replace(0.0,1.0)
                df['Billing Frequency'][df['Billing Frequency'] <= 0.9] = 1   # done my sheetal for negative data coming in Doc-899

                df['Unit Non-Recurring Cost'] = ''
                df['Currency'] = df['Currency'].fillna('USD')
                df['Service Start Date'] = df['Install Date']
                df['Service Term'] = ''
                df['Service End Date'] = ''
                df['Service ID (Supporting ID)'] = df['Service ID (Supporting ID)'].fillna('')
                df['Service Identifier'] = df['Service ID (Supporting ID)'].fillna('')
                
                df['Location A'] = df['Service Address A'] + ' ' + df['Location A City'] + ' ' + df['Location A State'].fillna('').astype(str) + ' ' + df['Location A Country']
                df['Location B'] = df['Service Address B'].apply(str)+ ' ' + df['Location Z City'].apply(str)+ ' ' + df['Location Z State'].apply(str) + ' ' + df['Location Z Country'].apply(str)
                df['Location B'] = df['Location B'].replace('nan nan nan nan','')
                df['Email of M3 Owner'] = args['Default_M3_Owner']
                df['Service Owner'] = ''
                df['Order Number'] = ''
                df['Order Date'] = ''
                df['DocID'] = docidportion
                df['Lumen - A Location Name'] = ''
                df['Lumen - Alternate ID'] = df['Carrier Circuit ID']
                df['Lumen - Bill Invoice Number'] = ''
                df['Lumen - Customer Name'] = df['Account Description 1']
                df['Lumen - Customer Number'] = df['Bus Org ID']
                df['Lumen - Data Source'] = ''
                df['Lumen - Internal Account Number'] = df['Internal Account Number']
                df['Lumen - Legacy Circuit ID'] = df['Legacy* Circuit Id']
                df['Lumen - Product Family'] = df['Product Family']
                df['Lumen - Product Identifier (PIID) [Parent]'] = df['PIID-Product'] 
                df['Lumen - Product Line'] = df['Product Line']
                df['Lumen - Service Component Name'] = ''
                df['Lumen - Service Line'] = df['Service Line (Customer Service)']
                df['Lumen - Status'] = ''
                df['Lumen - VPN ID'] = ''
                df['Lumen - Z Location Name'] = ''
                
                
                df['Latest - Extraction Date'] = extraction_date
                df['Latest - Invoice #'] = df['Bill Invoice Number']
                df['Latest - DOC-ID'] = docidportion
                
                
                
                # Filtered Product Line in df1
                df1 = df[df['Product Line'].apply(lambda val: all(val != s for s in ['HSIP', 'Level 3 Dedicated Internet Access', 'Wavelength']))]
                
                logger.info("Filtered Product Line ")
                
                if not df1.empty:
                    df1["Service ID (Supporting ID)"] = df1["Service ID (Supporting ID)"].map(str)
                    # Sorted Service ID (Supporting ID) in df1 in ascending order
                    df1 = df1.sort_values(by='Service ID (Supporting ID)', ascending=True, na_position='first')

                
                #df.drop(df[df['Product Line'] == 'tw NA PRODUCTS'].index, inplace=True)
                
                df = df.sort_values(by = ["Service Identifier","Lumen - Product Identifier (PIID) [Parent]"], ascending = [True,True])
                df.groupby("Service Identifier").first().reset_index()
                
                if not df1.empty:
                    df_final = pd.concat([df, df1])
                else: 
                    df_final = df
                
                df_final = df.drop(['Billing Account Number','Product Family','Annotation','Charge Level',"Charge Type Description","Product",
                    'Service Line (Customer Service)','Product Line','Location A City','Location A State','Location A Country','Location Z City',
                    'Location Z State','Location Z Country','Market/Tier Name','Rate Center','Service ID (Supporting ID)','Charge Description',
                    'Additional Charge Information','Bill From Date','Bill To Date','Units','Charge Amount','Discount Amount','Tax: Federal',
                    'Tax: State','Tax: City','Tax: County','Tax: Other','Tax: VAT','Account Description 1','Account Description 2',
                    'Bill Invoice Number','Bill Invoice Row ID','Billing Account Number-Account Description 1-Service ID','Bus Org ID',
                    'Carrier Circuit ID','Carrier ID','Child Account, Name','Circuit ID','Circuit Information','Circuit Type','Circuit_ID_A',
                    'Circuit_ID_Z','CLIN / BPID Code','CLIN /BPID Description','CLLIA','CLLIZ','Contract Number','Customer Designated Codes',
                    'Day of Month','Day of Week','Digital Access','Disconnect Date','Discount Description','Display Account Number',
                    'Effective Date','End User','Event Date','Event Time','Hour','Install Date','Internal Account Number','Invoice Date',
                    'Invoice Section Name','IP_Address','Jurisdiction','Legacy* Circuit Id','Legacy* Circuit Term','Legacy* IP VPN PIID',
                    'Legacy* Logical Interface PIID','Legacy* Port Id','Legacy* Service End User','Legacy* Service ID',
                    'Location A Type','Location Z','Location Z Type','Managed Service Type','Member ID','Month','NPA_NXX','PIID-Product',
                    'PO Number','POP Units','Port Code','Product Account Name','Provisioning Circuit ID','PSCID - Service','Quarter','Rate',
                    'SCID-GLMPS (MM)','SCID-Service','Seat Price','Seat Quantity','Section Header 2','Service Address A','Service Address B',
                    'Service Description 1','Service Description 2','Service Number','Service Order','Service Provider','Service Request','Service Type','Site Name',
                    'Site Number','Speed','Star','Subsection Header','Switch ID Location A','Switch ID Location Z','Switch Name Location A',
                    'Switch Name Location Z','Tariff/Period','Tax Included (Y/N)','Tax Label','TM','TM, WEB, VDE, STAR, Annotation',
                    'Unit Number','Unit Rate','V&H Miles','VDE','Web','Week','WTN','Year','Revised Unit Cost',"Custom:Toll Free Number"], axis=1, errors='ignore')
                

                df_final = df_final.reindex(columns = (" ", "Vendor (Purchasing)", "Account Identifier", "Product Name", "Service Quantity",
                    "Unit Recurring Cost", "Billing Frequency", "Unit Non-Recurring Cost", "Currency", "Service Start Date", "Service Term",
                    "Service End Date", "Service Identifier", "Location A", "Location B", "Email of M3 Owner", "Service Owner", "Order Number", "Order Date",
                    "DocID", "Lumen - A Location Name","Lumen - Alternate ID","Lumen - Bill Invoice Number","Lumen - Customer Name",
                    "Lumen - Customer Number","Lumen - Data Source","Lumen - Internal Account Number","Lumen - Legacy Circuit ID","Lumen - Product Family",
                    "Lumen - Product Identifier (PIID) [Parent]","Lumen - Product Line","Lumen - Service Component Name","Lumen - Service Line",
                    "Lumen - Status","Lumen - VPN ID","Lumen - Z Location Name","Lumen-Custom:Toll Free Number","Latest - Extraction Date",
                    "Latest - Invoice #","Latest - DOC-ID"))
                
                if(df.any().any() == False):
                    err = "After applying all the rules file found as empty."
                    error_files += "File=" + infile + ", Error=" + err
                    outputfile = infile.split("/")[-1].replace(".xls","")
                    docidportion = "DOC-" + outputfile.replace("DOC-","").split("-")[0]
                    for file in resp_json["files"]:
                        if outputfile.replace(docidportion+"-","").startswith(file['document_id']):
                            file["error"] = err
                            break
                    continue
          
                time_stamp = datetime.datetime.now().strftime("%d-%b-%Y %H:%M:%S")
    
                blanks = [
                    (' ', 'Service Upload Sheet', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', 
                     ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',' ', ' ', ' ',
                     ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', 
                     ' ', ' ', ' ', ' ', ' ', ' '),                         

                    (' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',' ', ' ', ' ', ' ',
                     ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', 
                     ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', 
                     ' ', ' ', ' ', ' ', ' ', ' '),  
                    
                    (' ', args['Client_Name'], ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',' ', 
                     ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',
                     ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', 
                     ' ', ' ', ' ', ' ', ' ', ' '), 
                    
                    (' ', 'User: ' + args['Upload_User'], ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',' ', 
                     ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',
                     ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', 
                     ' ', ' ', ' ', ' ', ' ', ' '),
                    
                    (' ', time_stamp, ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',' ',
                     ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',
                     ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', 
                     ' ', ' ', ' ', ' ', ' ', ' '), 
                        
                    (' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',' ', ' ', ' ', ' ',
                     ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', 
                     ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', 
                     ' ', ' ', ' ', ' ', ' ', ' '),
                         
                    (' ', "Vendor (Purchasing)", "Account Identifier", "Product Name", "Service Quantity",
                    "Unit Recurring Cost", "Billing Frequency", "Unit Non-Recurring Cost", "Currency", "Service Start Date", "Service Term",
                    "Service End Date", "Service Identifier", "Location A", "Location B", "Email of M3 Owner", "Service Owner", "Order Number", "Order Date",
                    "DocID", "Lumen - A Location Name","Lumen - Alternate ID","Lumen - Bill Invoice Number","Lumen - Customer Name",
                    "Lumen - Customer Number","Lumen - Data Source","Lumen - Internal Account Number","Lumen - Legacy Circuit ID","Lumen - Product Family",
                    "Lumen - Product Identifier (PIID) [Parent]","Lumen - Product Line","Lumen - Service Component Name","Lumen - Service Line",
                    "Lumen - Status","Lumen - VPN ID","Lumen - Z Location Name",'Lumen-Custom:Toll Free Number',"Latest - Extraction Date",
                    "Latest - Invoice #","Latest - DOC-ID")]
             
                df_blank = pd.DataFrame(blanks, columns=df_final.columns)
                df_final = pd.concat([df_blank, df_final])

                df_final.rename(columns = {' ' : '', 'Vendor (Purchasing)' : '', 'Account Identifier' : '', 'Product Name' : '', 'Service Quantity' : '', 'Unit Recurring Cost' : '',
                                    'Billing Frequency' : '', 'Unit Non-Recurring Cost' : '', 'Currency' : '', 'Service Start Date' : '', 'Service Term' : '', 
                                    'Service End Date': '', 'Service Identifier' : '', 'Location A' : '', 'Location B' : '', 'Email of M3 Owner' : '', 
                                    'Service Owner' :'', 'Order Number' : '', 'Order Date':'', 'DocID' : '', 
                                    'Lumen - A Location Name' :'', 'Lumen - Alternate ID' :'', 'Lumen - Bill Invoice Number' :'',
                                    'Lumen - Customer Name' :'', 'Lumen - Customer Number' :'', 'Lumen - Data Source' :'', 'Lumen - Internal Account Number' :'',
                                    'Lumen - Legacy Circuit ID' :'','Lumen - Product Family' :'', 'Lumen - Product Identifier (PIID) [Parent]' :'', 
                                    'Lumen - Product Line' :'', 'Lumen - Service Component Name' :'', 'Lumen - Service Line' :'',
                                    'Lumen - Status' :'', 'Lumen - VPN ID' :'', 'Lumen - Z Location Name' :'','Lumen-Custom:Toll Free Number':'',
                                    "Latest - Extraction Date":'',"Latest - Invoice #":'',"Latest - DOC-ID":''}, inplace = True)
    
                filepath = requests.utils.unquote(prefix + outputfile + '.xlsx')
                
                logger.info("start writer")
                
                with io.BytesIO() as output:
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        df_final.to_excel(writer, 'Bulk Upload',index=False)
                    data = output.getvalue()
                bucket.put_object(Key=filepath, Body=data)
                logger.info("filepath: " + filepath)
                for file in resp_json["files"]:
                    if outputfile.replace(docidportion+"-","").startswith(file['document_id']):
                        file['extraction_date'] = extraction_date
                        file['status'] = "Success"
                        break
                output_files += 1
                
                logger.info("ended writer")
            
        except Exception as e:
            logger.info("exception")
            logger.info(e)
            err = "".join(traceback.TracebackException.from_exception(e).format())
            err = err.replace('\n','').replace('\r','').replace(r'\n', '').replace(r'\r', '')
            logger.info(err)
            error_files+= " File=" + infile + ", original=" + outputfile+ ".xlsx, Error=" + err
            flag = False
            for file in resp_json["files"]:
                if outputfile.replace(docidportion+"-","").startswith(file['document_id']):
                    file['error'] = err
                    break                
logger.info("reached end")       
BridgeUtils.summary(input_files,output_files,error_files,resp_json,base_URL,REST_flag,logger,session)
logger.info("BridgeUtils.summary")    
job.commit()