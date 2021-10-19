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
print ("args taken")
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

glueClient  = boto3.client("glue")

test_format = "csv"
testDataPath =  "s3://test-bucket-glue-ankit/input/l2s2.csv"
abbreviated_test_schema = "l2s2"
primary_key_count = 1
maximum_string_length = 8
primaryKeys = ['id','col1']
print ("primaryKeys",primaryKeys)				


struct_type_generated_schema = {'type': 'struct', 'fields': [{'name': 'id', 'type': 'long', 'nullable': False, 'metadata': {}}, {'name': 'col1', 'type': 'long', 'nullable': False, 'metadata': {}}, {'name': 'col2', 'type': 'long', 'nullable': True, 'metadata': {}}, {'name': 'col3', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'col4', 'type': 'string', 'nullable': True, 'metadata': {}}]}

custom_schema = StructType.fromJson(struct_type_generated_schema)
print (struct_type_generated_schema)
print ("src starts")
# if test_format == "csv":
#     srcDataFrame =  spark.read.schema(custom_schema).option("multiline",multiline_mappings[test_format]).option("header","True").load(testDataPath,format = test_format )
# elif test_format =="json":
srcDataFrame =  spark.read.option("header","True").load(testDataPath,format = test_format, schema = custom_schema)
# srcDataFrame =  spark.read.option("header","True").schema(custom_schema).format(test_format).load(testDataPath)
print (srcDataFrame.count())
print (srcDataFrame.show())
DataSource0 = DynamicFrame.fromDF(srcDataFrame,glueContext,"DataSource0")
# DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = formatOptions, connection_type = "s3", format = fileFormat, connection_options = {"paths": [testDataPath], "recurse":True}, transformation_ctx = "DataSource0")
# source0 = DataSource0.toDF()
print ("src read done")

print ("transform starts")


applyMappingColumns= [
        ("id", "long", "id", "long"),
        ("col1", "long", "col1", "long"),
        ("col2", "long", "col2", "long"),
        ("col3", "string", "col3", "string"),
        ("col4", "string", "col4", "string"),
    ]

Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = applyMappingColumns, transformation_ctx = "Transform0")

glueContext.write_dynamic_frame.from_options(frame = Transform0,connection_type = "s3", format =  test_format,connection_options={
    "path":"s3://test-bucket-glue-ankit/output/"
})


time.sleep(30)



job.commit()

# jr_b6b0c4ea83bcda985c54861e912c408333990645aa671bc399fb1cdaaab3597e
# id,col1,col2,col3,col4
# 1,344,11,djsdscfb,abcdefgh
# 2,176,11,qwertrew,qghjvbsd
# 3,16672,11112,jhggsbak,lkdbsghj
# 4,111,156,kjhgfddd,aaaaaaaa
