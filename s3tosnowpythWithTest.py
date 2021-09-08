import sys
import boto3
import base64
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from py4j.java_gateway import java_import
from awsglue.dynamicframe import DynamicFrame
from botocore.exceptions import ClientError
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,ArrayType,MapType
from pyspark.sql.functions import col,struct,when
# Use this code snippet in your app.
# If you need more information about configurations or implementing the sample code, visit the AWS docs:   
# https://aws.amazon.com/developers/getting-started/python/
dataTypeMappings ={
    "string":"VARCHAR",
    "integer":"BIGINT",
    "double" :"NUMBER(30,4)",
    "long":"NUMBER(38,0)"
}
def pkeyHandler(T1,T2,pkeys):
    print ("pkeysINFunction",pkeys)
    res=[]
    for i in pkeys:
        res.append(str(T1) + '.' + str(i) + ' = ' + str(T2) + '.' +str(i))
    
    s2 = str(' and '.join(res))
    print(s2)
    return s2

def get_secret():
    secret_name = "snowcred"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
             return get_secret_value_response['SecretString']
        else:
             return base64.b64decode(get_secret_value_response['SecretBinary'])

##sqs = boto3.client('sqs', region_name="us-east-1")

def retrieve_messages(queue_url):
  response = sqs.receive_message(
    QueueUrl=queue_url,
    AttributeNames=[
      'revesionId',
      'fileLocation'
    ],
    MaxNumberOfMessages=10,
    MessageAttributeNames=[
      'All'
    ],
    VisibilityTimeout=200,
    WaitTimeSeconds=0
  )
  print('response: %s' % response)

  if 'Messages' in response:
    return response['Messages']
  else:
    print('Finished receiving all messages from SQS')
    return []
Fields = []
TEST = {
	"Name":"test-1",
	"Rows":"4",
	"Source":"s3",
	"Target":"Snowflake",
	"Columns":"4",
	"maxstrlen" : "64",
	"PK":"1",
	"Format":"json",
	"Schema":"l1s2d1"
}

sch = TEST["Schema"]

cnt_pk = int(TEST['PK'])
rows = int(TEST['Rows'])
cols = int(TEST['Columns'])
stlen = int(TEST['maxstrlen'])
cols_added=0

print (cnt_pk)
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



args = getResolvedOptions(sys.argv, ['JOB_NAME', 'URL', 'WAREHOUSE', 'DB', 'SCHEMA','Multiline'])

# Receive a batch of messages from the SQS queue
##messages = retrieve_messages(args['SQSQUEUEURL'])       
##print(messages)
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
## @params: [JOB_NAME]
##args = getResolvedOptions(sys.argv, ['JOB_NAME'])
Multiline = args['Multiline']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

json_file_path = "s3://test-bucket-glue-ankit/input/schema.json"
contents ={}
rdd = spark.sparkContext.wholeTextFiles(json_file_path)
text = rdd.collect()[0][1]
dict = json.loads(str(text))
custom_schema = StructType.fromJson(dict)
print (dict)
print (finalDic)
schema2= StructType.fromJson(finalDic)
print (custom_schema)
print (schema2)
df1 = spark.read.option("multiline",True).load('s3://test-bucket-glue-ankit/input/test-1.json', format = 'json', schema = schema2)
print (df1.show())
DataSource0 = DynamicFrame.fromDF(df1,glueContext,"DataSource0")
# DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = {"jsonPath":"$.*","multiline":True}, connection_type = "s3", format = "json", connection_options = {"paths": ["s3://test-bucket-glue-ankit/input/jsonData(1).json"], "recurse":True}, transformation_ctx = "DataSource0")
columns = []
tableColumns=[["BATCHID_IN","VARCHAR(64)"],["BATCHID_OUT","VARCHAR(64)"]]

for i in range(len(finalDic['fields'])):
    col = finalDic['fields'][i]
    #print (col)
    name = col['name']
    dataType = dataTypeMappings[col['type']]
    
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


Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = columns, transformation_ctx = "Transform0")
begin_txn = "BEGIN TRANSACTION;"
create_main_table_if_not_exist = """ create table DEMO_DB.DEMO.JSONTEST IF NOT EXISTS ( """ +s1+  """ ); """

initialize_metadata = """Insert into DEMO.BATCH_METADATA (TABLENAME,BATCH_ID,BATCH_TS,START_TS,BATCH_STATUS) select
'JSONTEST',
(SELECT COALESCE( MAX(BATCH_ID), 0) +1 FROM DEMO.BATCH_METADATA  M where M.TABLENAME = 'JSONTEST'),
current_timestamp,
current_timestamp,
'INITIALIZED';"""
update_hash = "Update DEMO_DB.DEMO.JSONTEST__STAGE set message_digest = HASH(*);"
update_batch_out = """Update DEMO_DB.DEMO.JSONTEST as T set Batchid_out = (SELECT MAX(BATCH_ID) -1 FROM DEMO.BATCH_METADATA  M where M.TABLENAME = 'JSONTEST')
Where Batchid_out = 999999999 And exists ( Select * from DEMO_DB.DEMO.JSONTEST__STAGE S
where T.message_digest <> S.message_digest and """ +pkeyHandler("T","S",pkeys)+ """ ); """

insert_into_main_table = """Insert into DEMO_DB.DEMO.JSONTEST
Select (SELECT MAX(BATCH_ID) FROM DEMO.BATCH_METADATA  M where M.TABLENAME = 'JSONTEST'), 999999999,*
From DEMO_DB.DEMO.JSONTEST__STAGE st
Where not exists (
Select * from DEMO_DB.DEMO.JSONTEST M
Where M.batchid_out = 999999999 and
M.message_digest = st.message_digest and """ + pkeyHandler("m","st",pkeys) +""" 
);"""
Update_metadata = """Update DEMO.BATCH_METADATA M
Set
M.End_ts = current_timestamp,
M.BATCH_STATUS = 'DONE'
where TABLENAME='JSONTEST'
and Batch_id=(SELECT MAX(BATCH_ID) FROM DEMO.BATCH_METADATA  M where M.TABLENAME = 'JSONTEST' and BATCH_STATUS = 'INITIALIZED');"""
truncate_stage = "TRUNCATE TABLE DEMO_DB.DEMO.JSONTEST__STAGE;"
commit_work = "Commit WORK;"

preactions = begin_txn+create_main_table_if_not_exist
postactions = initialize_metadata+update_hash+update_batch_out+insert_into_main_table+Update_metadata+truncate_stage+commit_work
secret_value = get_secret()

j = json.loads(secret_value)
password = j['PASSWORD']
username = j['USERNAME']
Sink0 = Transform0.toDF()


sfOptions = {
"sfURL" : args['URL'],
"sfUser" : username,
"sfPassword" : password,
"sfDatabase" : args['DB'],
"sfSchema" : args['SCHEMA'],
"sfWarehouse" : args['WAREHOUSE'],
"dbTable" :"JSONTEST__STAGE"
}

print (preactions)
print (postactions)
Sink0.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "JSONTEST__STAGE").option("preactions", preactions).option("postactions", postactions).mode("append").save()

##DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "json", connection_options = {"path": "s3://snowbuck1/snow/", "partitionKeys": []}, transformation_ctx = "DataSink0")
##datasink2 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type="custom.spark", connection_options = sfOptions )

job.commit()




