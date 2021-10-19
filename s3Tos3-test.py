import sys
import re
import boto3
import json
import copy
import datetime
import time
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from botocore.exceptions import ClientError
from pyspark.sql.types import StructType

starting_time =  datetime.datetime.now()
dataTypeMappings ={
    "string":"VARCHAR",
    "double" :"NUMBER(30,4)",
    "long":"NUMBER(38,0)",
    "boolean":"BOOLEAN"
}

character_mappings={
    "s":"string",
    "l":"long",
    "d":"double",
    "b":"boolean"
}
header_mappings = {
    "csv":"True",
    "json":"False",
    "parquet":"False"
}
def primary_key_constraints(Table1,Table2,primaryKeys):   
    if len(primaryKeys) == 0:
        return "No Primary Keys"
    total_constraints=[]
    for columns in primaryKeys:
        total_constraints.append(str(Table1) + '.' + str(columns) + ' = ' + str(Table2) + '.' +str(columns))   
    return str(' and '.join(total_constraints))   
    
print ("args taken")
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'key_arn','WORKFLOW_NAME','WORKFLOW_RUN_ID'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
formatOptions = {}



glueClient  = boto3.client("glue", region_name="us-east-1", endpoint_url="https://glue.us-east-1.amazonaws.com")
workflowProperties = glueClient.get_workflow_run_properties(
    Name = args['WORKFLOW_NAME'],
    RunId = args['WORKFLOW_RUN_ID']
)['RunProperties']

secret_client = boto3.client("secretsmanager", region_name="us-east-1", endpoint_url="https://secretsmanager.us-east-1.amazonaws.com")
response = secret_client.get_secret_value(SecretId=args['key_arn'])

print ("secret generated")
print(response)
new_test = re.sub("-*(BEGIN|END) RSA PRIVATE KEY-*\r\n","", response["SecretString"]).replace("\n","").replace("\r","")
print(new_test)

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
# Reading from json schema 
# schemaProperty =  fileFormat+'SchemaPath'

# schemaPath =  workflowProperties['RunProperties'][schemaProperty]
# rdd = spark.sparkContext.wholeTextFiles(schemaPath)
# text = rdd.collect()[0][1]
# dict = json.loads(str(text))
# custom_schema =  StructType.fromJson(dict)

# dataPath = fileFormat+'TestData'
# s3://terraform-20210726142826483100000007/fakedata/Five_Thousand_Rows_Five_Columns/csv/baseline/
# s3://terraform-20210726142826483100000007/fakedata/Fifty_Thousand_Rows_Five_Columns/json/baseline/
test_format = workflowProperties["TestFormat"]
testDataPath =  workflowProperties["TestOutdir"]+test_format+"/baseline/"
abbreviated_test_schema = workflowProperties["TestSchema"]
primary_key_count = int(workflowProperties['TestPrimaryKeys'])
maximum_string_length = int(workflowProperties['TestMaxStrLen'])
read_source =  workflowProperties['TestSource']
write_target = workflowProperties['TestTarget']
test_name = workflowProperties['TestName']

# testDataPath =  "s3://terraform-20210726142826483100000007/fakedata/One_Million_row_Five_col/csv/baseline/"
# abbreviated_test_schema = "l1s2d2"
# primary_key_count = 1
# maximum_string_length = 128
# read_source =  "s3"
# write_target = "snowflake"
# test_name = "One_Million_row_Five_col"
print (testDataPath)

tableName = test_name+'_' + test_format.upper() + '_PYTHON'

current_column_number=0


Fields=[{'name':'id','type':'long','nullable':False,'metadata':{}}]
primaryKeys = ['id']


end_index = len(abbreviated_test_schema)-1
start_index = 0

while start_index< end_index:
    current_index = start_index
    current_character = abbreviated_test_schema[start_index]
    current_column_count = 0
    current_index+=1
    while abbreviated_test_schema[current_index] >='0' and abbreviated_test_schema[current_index]<='9':
        print (current_index)
        current_column_count = current_column_count*10 + int(abbreviated_test_schema[current_index])
        current_index+=1
        if current_index>end_index:
            break
    while current_column_count:
        current_column_count-=1
        current_field = {'name':'','type':'','nullable':True,'metadata':{}}
        current_column_number+=1
        current_field['name'] = 'col' + str(current_column_number)
        
        current_field['type'] = character_mappings[current_character]
        if primary_key_count>0:
            primary_key_count-=1
            primaryKeys.append(current_field['name'])
            current_field['nullable']= False    
        Fields.append(current_field)

    start_index = current_index

print ("primaryKeys",primaryKeys)				


struct_type_generated_schema = {
    'type':'struct',
    'fields':Fields
}
custom_schema = StructType.fromJson(struct_type_generated_schema)
print (struct_type_generated_schema)
print ("src starts")
# if test_format == "csv":
#     srcDataFrame =  spark.read.schema(custom_schema).option("multiline",multiline_mappings[test_format]).option("header","True").load(testDataPath,format = test_format )
# elif test_format =="json":
srcDataFrame =  spark.read.option("header",header_mappings[test_format]).load(testDataPath,format = test_format, schema = custom_schema)

print (srcDataFrame.count())
print (srcDataFrame.show())
DataSource0 = DynamicFrame.fromDF(srcDataFrame,glueContext,"DataSource0")
# DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = formatOptions, connection_type = "s3", format = fileFormat, connection_options = {"paths": [testDataPath], "recurse":True}, transformation_ctx = "DataSource0")
# source0 = DataSource0.toDF()
print ("src read done")

print ("transform starts")
applyMappingColumns = []
source_columns_for_transformations = []
main_table_columns_list_of_list=[["BATCHID_IN","VARCHAR(64)"],["BATCHID_OUT","VARCHAR(64)"]]
for field in Fields:
    field_name =  field['name']
    data_type =  dataTypeMappings[field['type']]
    string_max_length = ""
    is_null = ""
    if field['type'] == 'string':
        string_max_length += "("+str(maximum_string_length) + ")"
    if field['nullable'] == False:
        is_null+="NOT NULL"
    source_columns_for_transformations.append((field['name'],field['type']))
    main_table_columns_list_of_list.append([field_name,data_type,string_max_length,is_null])

if read_source== 's3' and write_target == 'snowflake':
    source_columns_for_transformations.append(("message_digest","string"))
    main_table_columns_list_of_list.append(['message_digest','VARCHAR(64)'])
    target_columns_for_transformations =  copy.deepcopy(source_columns_for_transformations)

    for i in range(len(source_columns_for_transformations)):
        mapping_tuple =  source_columns_for_transformations[i] +  target_columns_for_transformations[i]
        applyMappingColumns.append(mapping_tuple)



    main_table_column_list_of_strings = []
    for columns in main_table_columns_list_of_list:
        main_table_column_list_of_strings.append(' '.join(columns))
    main_table_columns_final_string= ','.join(main_table_column_list_of_strings)

    Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = applyMappingColumns, transformation_ctx = "Transform0")

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
    begin_txn = "BEGIN TRANSACTION;"
    create_main_table_if_not_exist = """create table """ + tableName +"""  IF NOT EXISTS (""" + main_table_columns_final_string + """ ); """

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
    where T.message_digest <> S.message_digest and """ + primary_key_constraints("T","S",primaryKeys) +"""  ); """
    # where T.message_digest <> S.message_digest and T.id = S.id  );"""
    insert_into_main_table = """Insert into """ + tableName +"""
    Select (SELECT MAX(BATCH_ID) FROM BATCH_METADATA  M where M.TABLENAME = '"""+tableName+"""'), 999999999,*
    From """+tableName+"""__STAGE st
    Where not exists (
    Select * from """+tableName+""" M
    Where M.batchid_out = 999999999 and
    M.message_digest = st.message_digest and """ + primary_key_constraints("m","st",primaryKeys) + """
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
elif read_source == 's3' and write_target=='s3':
    
    target_columns_for_transformations =  copy.deepcopy(source_columns_for_transformations)

    for i in range(len(source_columns_for_transformations)):
        mapping_tuple =  source_columns_for_transformations[i] +  target_columns_for_transformations[i]
        applyMappingColumns.append(mapping_tuple)

    main_table_column_list_of_strings = []
    for columns in main_table_columns_list_of_list:
        main_table_column_list_of_strings.append(' '.join(columns))
    main_table_columns_final_string= ','.join(main_table_column_list_of_strings)

    Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = applyMappingColumns, transformation_ctx = "Transform0")

    glueContext.write_dynamic_frame.from_options(frame = Transform0,connection_type = "s3", format =  test_format,connection_options={
        "path":"s3://terraform-20210726142826483100000007/performanceTestDataOutput/"+test_name+"_"+test_format+"_"+"Python/"
    })

else:
    print("Source/Sink Not supported")

time.sleep(30)

finish_time =  datetime.datetime.now()



putProperties = glueClient.put_workflow_run_properties(
    Name = args['WORKFLOW_NAME'],
    RunId =  args['WORKFLOW_RUN_ID'],
    RunProperties={
        args['JOB_NAME']:args['JOB_RUN_ID']
    }
)

job.commit()



  
