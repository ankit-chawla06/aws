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
from pyspark.sql.types import StructType

sqlDataTypeMappings ={
    "string":"VARCHAR",
    "integer":"NUMBER(38,0)",
    "double" :"NUMBER(30,4)",
    "long":"NUMBER(38,0)"
}
def pkeyHandler(T1,T2,pkeys):   
    res=[]
    for i in pkeys:
        res.append(str(T1) + '.' + str(i) + ' = ' + str(T2) + '.' +str(i))   
    s2 = str(' and '.join(res))   
    return s2
print ("args taken")
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'key_arn','format','Multiline','WORKFLOW_NAME','WORKFLOW_RUN_ID'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
formatOptions = {}
fileFormat  = args['format']


glueClient  = boto3.client("glue", region_name="us-east-1", endpoint_url="https://glue.us-east-1.amazonaws.com")
workflowProperties = glueClient.get_workflow_run_properties(
    Name = args['WORKFLOW_NAME'],
    RunId = args['WORKFLOW_RUN_ID']
)['RunProperties']
# Reading from json schema 
# schemaProperty =  fileFormat+'SchemaPath'
dataPath = fileFormat+'TestData'
# schemaPath =  workflowProperties['RunProperties'][schemaProperty]
# rdd = spark.sparkContext.wholeTextFiles(schemaPath)
# text = rdd.collect()[0][1]
# dict = json.loads(str(text))
# custom_schema =  StructType.fromJson(dict)

testDataPath =  workflowProperties[dataPath]

sch = workflowProperties["TestSchema"]
cnt_pk = int(workflowProperties['TestPrimaryKeys'])
rows = int(workflowProperties['TestRows'])
cols = int(workflowProperties['TestColumns'])
stlen = int(workflowProperties['TestMaxStrLen'])
tableName = str(workflowProperties['TestName'])+'_' + fileFormat.upper() + '_PYTHON'
cols_added=0

print (cnt_pk)
Fields=[]
pkeys = []
for j in range(1,len(sch),2):
	quantity = int(sch[j])
	character = sch[j-1]
	while quantity:
		quantity-=1
		temp = {'name':'','type':'','nullable':True,'metadata':{}}
		#print (temp)
		cols_added+=1
		temp['name'] = 'Column_'+str(cols_added)
		if character == 's':
			temp['type'] = 'string'
		elif character=='l':
			temp['type'] = 'long'
		elif character == 'd':
			temp['type'] = 'double'
		else:
			pass
		if cnt_pk>0:
			cnt_pk-=1
			pkeys.append(temp['name'])
			temp['nullable'] = False
		Fields.append(temp)

print ("pkeys",pkeys)				


finalDic = {
    'type':'struct',
    'fields':Fields
}


secret_client = boto3.client("secretsmanager", region_name="us-east-1", endpoint_url="https://secretsmanager.us-east-1.amazonaws.com")
response = secret_client.get_secret_value(SecretId=args['key_arn'])

print ("secret generated")
new_test = re.sub("-*(BEGIN|END) RSA PRIVATE KEY-*\r\n","", response["SecretString"]).replace("\n","").replace("\r","")


SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"



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
custom_schema = StructType.fromJson(finalDic)
print (finalDic)
srcDataFrame =  spark.read.schema(custom_schema).option("multiline",args['Multiline']).load(testDataPath,format = fileFormat )
print (srcDataFrame.show())
DataSource0 = DynamicFrame.fromDF(srcDataFrame,glueContext,"DataSource0")
# DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = formatOptions, connection_type = "s3", format = fileFormat, connection_options = {"paths": [testDataPath], "recurse":True}, transformation_ctx = "DataSource0")
# source0 = DataSource0.toDF()
print ("src read done")

print ("transform starts")
columns = []
tableColumns=[["BATCHID_IN","VARCHAR(64)"],["BATCHID_OUT","VARCHAR(64)"]]

for i in range(len(finalDic['fields'])):
    col = finalDic['fields'][i]
    #print (col)
    name = col['name']
    dataType = sqlDataTypeMappings[col['type']]
    
    stringSize = ""
    isNull = ""
    if col['type']=='string':
        stringSize+="("+str(stlen) + ")"
    if col['nullable'] == False:
        isNull+="NOT NULL"
    
    columns.append((col['name'],col['type'],col['name'],col['type']))
    tableColumns.append([name,dataType,stringSize,isNull])
columns.append(("message_digest","string","message_digest","string"))
tableColumns.append(['message_digest','VARCHAR(64)'])
allset = []
for i in tableColumns:
    allset.append(' '.join(i))
s1= ','.join(allset)

print (tableColumns)
print (columns)
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = columns, transformation_ctx = "Transform0")

print ("transform ended")

begin_txn = "BEGIN TRANSACTION;"
create_main_table_if_not_exist = """create table """ + tableName +"""  IF NOT EXISTS (""" + s1 + """ ); """

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
where T.message_digest <> S.message_digest and """ + pkeyHandler("T","S",pkeys) +"""  ); """
# where T.message_digest <> S.message_digest and T.id = S.id  );"""
insert_into_main_table = """Insert into """ + tableName +"""
Select (SELECT MAX(BATCH_ID) FROM BATCH_METADATA  M where M.TABLENAME = '"""+tableName+"""'), 999999999,*
From """+tableName+"""__STAGE st
Where not exists (
Select * from """+tableName+""" M
Where M.batchid_out = 999999999 and
M.message_digest = st.message_digest and """ + pkeyHandler("m","st",pkeys) + """
 );"""
Update_metadata = """Update BATCH_METADATA M
Set
M.End_ts = current_timestamp,
M.BATCH_STATUS = 'DONE'
where TABLENAME='"""+tableName+"""'
and Batch_id=(SELECT MAX(BATCH_ID) FROM BATCH_METADATA  M where M.TABLENAME = '"""+tableName+"""' and BATCH_STATUS = 'INITIALIZED');"""
truncate_stage = "TRUNCATE TABLE "+tableName+"__STAGE;"
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
Sink0 = Transform0.toDF()
Sink0.show()
# Sink0.write.format(SNOWFLAKE_SOURCE_NAME).options(**snowflake_properties).option("dbtable", """"+tableName+"""__STAGE").option("preactions", preactions).option("postactions", postactions).mode("append").save()
#  DynamicFrame.fromDF(Sink0, glueContext, "test_frame_conversion")
# print ("Write form df end")
print ("Writing started")
glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type="custom.spark", connection_options=snowflake_properties_dynamic)
print ("write ended")


putProperties = glueClient.put_workflow_run_properties(
    Name = args['WORKFLOW_NAME'],
    RunId =  args['WORKFLOW_RUN_ID'],
    RunProperties={
        args['JOB_NAME']:args['JOB_RUN_ID']
    }
)
job.commit()



  
