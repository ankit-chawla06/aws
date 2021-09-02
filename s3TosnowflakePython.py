import sys
import re
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from botocore.exceptions import ClientError

print ("args taken")
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'key_arn','format'])
tableName  = ""
testDataPath = ""
fileFormat  = args['format']

if fileFormat == 'csv':
    tableName = "TEST_CSV_PYTHON"
    testDataPath = "s3://terraform-20210726142826483100000007/performanceTestData/csvData.csv"
elif fileFormat == 'parquet':
    tableName = 'TEST_PARQUET_PYTHON'
    testDataPath = "s3://terraform-20210726142826483100000007/performanceTestData/parquetData.parquet"
elif fileFormat == 'json':
    tableName = 'TEST_JSON_PYTHON'
    testDataPath = "s3://terraform-20210726142826483100000007/performanceTestData/jsonData.json"
else:
    print ("WRONG FILE FORMAT : Expected (json,csv,parquet)")

secret_client = boto3.client("secretsmanager", region_name="us-east-1", endpoint_url="https://secretsmanager.us-east-1.amazonaws.com")
response = secret_client.get_secret_value(SecretId=args['key_arn'])

print ("secret generated")
new_test = re.sub("-*(BEGIN|END) RSA PRIVATE KEY-*\r\n","", response["SecretString"]).replace("\n","").replace("\r","")


SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

snowflake_properties = {
    "sfUrl": "sfceawseast1d01.us-east-1.privatelink.snowflakecomputing.com",
    "sfDatabase": "CNAS_DEV",
    "sfWarehouse": "CNAS_DEV_WAREHOUSE",
    "pem_private_key": new_test,
    "sfUser": "CNAS_DEV_SNOWFLAKE_GLUE_USER",
    "sfRole": "CNAS_DEV_ROLE",
    "sfSchema": "PUBLIC",
    "connectionName": "snowflakeConnection",
}
print ("src starts")
DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = {"quoteChar":"\"","escaper":"","withHeader":True,"separator":","}, connection_type = "s3", format = fileFormat, connection_options = {"paths": [testDataPath], "recurse":True}, transformation_ctx = "DataSource0")
source0 = DataSource0.toDF()
print ("src read done")

print ("transform starts")
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("Region", "string", "Region", "string"), ("Country", "string", "Country", "string"), ("Item Type", "string", "ItemType", "string"), ("Sales Channel", "string", "SalesChannel", "string"), ("Order Priority", "string", "OrderPriority", "string"), ("Order Date", "string", "OrderDate", "string"), ("Order ID", "string", "OrderID", "string"), ("Ship Date", "string", "ShipDate", "string"), ("Units Sold", "string", "UnitsSold", "string"), ("Unit Price", "string", "UnitPrice", "string"), ("Unit Cost", "string", "UnitCost", "string"), ("Total Revenue", "string", "TotalRevenue", "string"), ("Total Cost", "string", "TotalCost", "string"), ("Total Profit", "string", "TotalProfit", "string"), ("message_digest", "string", "message_digest", "string")], transformation_ctx = "Transform0")

print ("transform ended")

begin_txn = "BEGIN TRANSACTION;"
create_main_table_if_not_exist = """create table """ + tableName +"""  IF NOT EXISTS ( batchid_in varchar(64), batchid_out varchar(64), Region VARCHAR(128), Country VARCHAR(128), ItemType VARCHAR(64), SalesChannel VARCHAR(128), OrderPriority VARCHAR(128) , OrderDate VARCHAR(128), OrderID Number(38,0), ShipDate VARCHAR(128), UnitsSold Number(38,0), UnitPrice Number(38,0), UnitCost Number(38,0), TotalRevenue Number(38,0), TotalCost Number(38,0), TotalProfit Number(38,0), message_digest varchar(64) ); """

create_metadata_table_if_not_exist = """CREATE TABLE BATCH_METADATA IF NOT EXISTS
                                        (
                                        TABLENAME  varchar(255) NOT NULL,
                                        BATCH_ID   integer  NOT NULL,
                                        BATCH_TS timestamp NOT null,
                                        START_TS timestamp NOT null,
                                        END_TS timestamp NULL,
                                        BATCH_STATUS varchar(32) NOT Null,
                                        Primary key ( TABLENAME,  BATCH_ID, BATCH_TS)
                                          ); """
initialize_metadata = """Insert into BATCH_METADATA (TABLENAME,BATCH_ID,BATCH_TS,START_TS,BATCH_STATUS) select
'""" + tableName + """',
(SELECT COALESCE( MAX(BATCH_ID), 0) +1 FROM PUBLIC.BATCH_METADATA  M where M.TABLENAME = '"""+tableName+"""'),
current_timestamp,
current_timestamp,
'INITIALIZED';"""
update_hash = "Update " +tableName+"__STAGE set message_digest = HASH(*);"
update_batch_out = """Update """ + tableName + """ as T set Batchid_out = (SELECT MAX(BATCH_ID) -1 FROM PUBLIC.BATCH_METADATA  M where M.TABLENAME = '"""+tableName+"""')
Where Batchid_out = 999999999 And exists ( Select * from """+tableName+"""__STAGE S
where T.message_digest <> S.message_digest and T.OrderID = S.OrderID  ); """
# where T.message_digest <> S.message_digest and T.id = S.id  );"""
insert_into_main_table = """Insert into """ + tableName +"""
Select (SELECT MAX(BATCH_ID) FROM BATCH_METADATA  M where M.TABLENAME = '"""+tableName+"""'), 999999999,*
From """+tableName+"""__STAGE st
Where not exists (
Select * from """+tableName+""" M
Where M.batchid_out = 999999999 and
M.message_digest = st.message_digest and m.OrderID = st.OrderID
);"""
Update_metadata = """Update BATCH_METADATA M
Set
M.End_ts = current_timestamp,
M.BATCH_STATUS = 'DONE'
where TABLENAME='"""+tableName+"""'
and Batch_id=(SELECT MAX(BATCH_ID) FROM BATCH_METADATA  M where M.TABLENAME = '"""+tableName+"""' and BATCH_STATUS = 'INITIALIZED');"""
truncate_stage = "TRUNCATE TABLE ""+tableName+""__STAGE;"
commit_work = "Commit WORK;"

preactions = begin_txn+create_main_table_if_not_exist +create_metadata_table_if_not_exist
postactions = initialize_metadata+update_hash+update_batch_out+insert_into_main_table+Update_metadata+truncate_stage+commit_work

print (preactions)
print (postactions)
stageTable = tableName + "__STAGE"
snowflake_properties_dynamic = {
    "sfUrl": "sfceawseast1d01.us-east-1.privatelink.snowflakecomputing.com",
    "sfDatabase": "CNAS_DEV",
    "sfWarehouse": "CNAS_DEV_WAREHOUSE",
    "pem_private_key": new_test,
    "sfUser": "CNAS_DEV_SNOWFLAKE_GLUE_USER",
    "sfRole": "CNAS_DEV_ROLE",
    "sfSchema": "PUBLIC",
    "dbtable": stageTable,
    "connectionName": "snowflakeConnection",
    "preactions":preactions,
    "postactions":postactions
}
# print ("Write from DF starts")
# Sink0 = Transform0.toDF()
# Sink0.printSchema()
# Sink0.show()
# Sink0.write.format(SNOWFLAKE_SOURCE_NAME).options(**snowflake_properties).option("dbtable", """"+tableName+"""__STAGE").option("preactions", preactions).option("postactions", postactions).mode("append").save()
#  DynamicFrame.fromDF(Sink0, glueContext, "test_frame_conversion")
# print ("Write form df end")
print ("Writing started")
glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type="custom.spark", connection_options=snowflake_properties_dynamic)
print ("write ended")

job.commit()
