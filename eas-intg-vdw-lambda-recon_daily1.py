import os
from datetime import datetime
import psycopg2
import boto3

def lambda_handler(event, context):

    conn_string = psycopg2.connect(dbname=os.environ['database'], user = os.environ['user'], password = os.environ['password'], port = os.environ['port'], host = os.environ['host'])
    cur = conn_string.cursor()

    s3 = boto3.resource('s3')
    s3_client = boto3.client('s3')
    loaded_dttime = datetime.now().strftime("%Y%m%d%H%M%S") 
    s3path ='s3://viac-intg-vdw-distribution/venerable/datawarehouse/Voya_ViacTaxRecon_Daily.csv'
    
    # temp = "('SELECT Business_Unit, TO_CHAR(CURRENT_DATE, \\'YYYY-MM-DD\\') as Curr_Date FROM vdw_reports.vw_voya_daily_tax_nopii_v1')"
    temp = "('SELECT \\'Business_Unit\\' as Business_Unit, \\'Curr_Date\\' as Curr_Date, \\'Account\\' as Account, \\'Oper_Unit\\' as Oper_Unit, \\'Dept\\' as Dept, \\'Product\\' as Product, \\'Amount\\' as Amount, \\'Source\\' as Source, \\'Policy_Number\\' as Policy_Number, \\'State_Cd\\' as State_Cd, \\'State_Cd\\' as State_Cd UNION SELECT Business_Unit, TO_CHAR(CURRENT_DATE, \\'YYYY-MM-DD\\') as Curr_Date, Account, Oper_Unit, Dept, Product, cast(Amount as char(25)), Source, Policy_Number, State_Cd, TO_CHAR(Posted, \\'YYYY-MM-DD\\') as Posted FROM vdw_reports.vw_voya_daily_tax_nopii_v1 WHERE Posted = DATE_TRUNC(\\'DAY\\', CURRENT_DATE-1) ORDER BY Business_Unit, Oper_Unit, Account')"
    
    sql_query = "UNLOAD %s TO '%s' iam_role '%s' \nCSV \nMAXFILESIZE 100 MB \nALLOWOVERWRITE \nPARALLEL OFF;" %\
    (temp ,s3path,os.environ['arn_role'])
    print(sql_query)
    cur.execute(sql_query) 
    tgkey ='venerable/datawarehouse/Voya_ViacTaxRecon_Daily_'+str(loaded_dttime)+'.csv'
    copy_source = {'Bucket': 'viac-intg-vdw-distribution', 'Key': 'venerable/datawarehouse/Voya_ViacTaxRecon_Daily.csv000'}
    s3_client.copy_object(CopySource = copy_source, Bucket = 'viac-intg-vdw-distribution', Key = tgkey)
    s3_client.delete_object(Bucket = 'viac-intg-vdw-distribution', Key = 'venerable/datawarehouse/Voya_ViacTaxRecon_Daily.csv000')