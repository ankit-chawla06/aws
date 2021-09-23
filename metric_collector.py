import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from datetime import datetime, timedelta, date
from botocore.exceptions import ClientError
from pyspark.sql.types import *
import boto3
import json
import csv
import io
import copy

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','key_arn','Multiline','WORKFLOW_NAME','WORKFLOW_RUN_ID'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


region='us-east-1'

s3_client = boto3.resource('s3', region_name=region)
client = boto3.client('cloudwatch', region_name=region)
glue_client = boto3.client('glue', region_name=region)
time = datetime.utcnow().strftime ('%Y-%m-%d-%H-%M-%S')
run_date = date.today().strftime("%d/%m/%Y")


def get_req_count(region, metric_name):
    print('start of method')
    response2 = client.get_metric_statistics(
            Namespace="Glue",
            MetricName='CUSTOM_GLUE_1',
            Dimensions=[
                {'Name': 'Type', 'Value': 'count'},
                {
                    'Name': 'xyz',
                    'Value': 'ABC'
                },
            ],
            StartTime = datetime.utcnow() - timedelta(seconds = 60000),
            EndTime = datetime.utcnow(),
            Period=86460,
            Statistics=[
                "Sum",
            ]
    )

def get_worflow_job_list(workflow_name,exclude_job_list):
    workflow_graph=glue_client.get_workflow(Name=workflow_name, IncludeGraph=True)
    workflow_nodes=workflow_graph['Workflow']['Graph']['Nodes']
    print('workflow_nodes', workflow_nodes)
    exclude_job_list=exclude_job_list
    job_list=[]
    for node in workflow_nodes:
        if node['Type'] == 'JOB' and node['Name'] not in exclude_job_list :
            job_list.append(node['Name'])
        else:
            continue
    print('job_list', job_list)
    return job_list

def get_job_metrics(  namespace, metric_name, metric_type, job_run_id, job_name, Statistics_type):
    print(namespace, metric_name, metric_type, job_run_id, job_name, Statistics_type)
    response = client.get_metric_statistics(
        Namespace=namespace,
        MetricName=metric_name,
        Dimensions=[
                    {'Name': 'Type', 'Value': metric_type},
                    {
                        "Name": "JobRunId",
                        "Value": job_run_id
                    },
                    {'Name': 'JobName', 'Value': job_name }
        ],
        StartTime = datetime.utcnow() - timedelta(seconds = 10800),
        EndTime = datetime.utcnow(),
        Period=300,
        Statistics= [Statistics_type]
    )
    data_points = response['Datapoints']
    print('response get_metric_statistics data_points', data_points)
    data_point = data_points[0] if data_points else 'data not available'
    stats = data_point[Statistics_type] if data_points else -1.0
    return stats


def write_cvs_into_s3(bucket, s3_path):

    fieldnames = ['Workflow_run_id','Workflow_name','Job_run_id','Job_name','Job_language','Source','Target','Glue_version', 'Worker_type', 'Number_of_workers','Number_of_rows','Number_of_columns','Elapsed_time','bytesRead','recordsRead','cpuSystemLoad','heap_usage','diskSpaceUsed_MB','Execution_timestamp']
    exclude_job_list = ['metrics_collector_glue_Job','fakedata_generator_job']
    #get all the job names in a list or array from the workflow api
    #get the first  and last job name in fthe exclude job list
    #get the job metadata of the jobs
    #iterate on the list of jobs metadat list of jobs
    # call get_job_metrics with all params to get specific metrics
    # write it in csv in s3 bucket

    #glue_job_runs = glue_client.get_job_runs(JobName='ParquetLoadJob')
    #print('glue_job_runs', glue_job_runs)
    #print('glue_job_runs')
    job_names = get_worflow_job_list('performanceTestWf', exclude_job_list )
    batch_get_jobs = glue_client.batch_get_jobs(JobNames=job_names)
    print('batch_get_job', batch_get_jobs)
    print('batch_get_job')

    metrics_list = []
    with open('/tmp/file_name', 'w', newline='') as csvFile:
        w = csv.writer(csvFile, dialect='excel')
        w.writerow(fieldnames)
    #batch_get_jobs['Jobs']
        namespace = 'Glue'
        metric_type_count = 'count'
        metric_type_gauge = 'gauge'
        #job_run_id = 'ALL'
        #get job runid from workflow runid
        Statistics_type_avg = 'Average'
        Statistics_type_max = 'Max'
        Statistics_type_sum = 'Sum'
        Workflow_run_id = args['WORKFLOW_RUN_ID']
        Workflow_name = args['WORKFLOW_NAME']
        workflowProperties = glue_client.get_workflow_run_properties(
             Name = args['WORKFLOW_NAME'],
             RunId = args['WORKFLOW_RUN_ID']
        )['RunProperties']
        #    fieldnames = ['Job_name','Job_language', 'Glue_version', 'Worker_type', 'Number_of_workers','Number_of_rows','Number_of_columns','Elasped_time','Execution_timestamp']
        for job in batch_get_jobs['Jobs']:
            print('JOB details ::', job)
            Job_name = job['Name']
            job_run_id = workflowProperties[Job_name]
            print('job_run_id ::', job_run_id)
            default_arguments = job['DefaultArguments']
            Job_language = default_arguments['--job-language'] if '--job-language' in default_arguments else 'python'
            Glue_version = job['GlueVersion']
            Number_of_workers = job['NumberOfWorkers']
            Worker_type = job['WorkerType']
            Metric_list = ['glue.driver.aggregate.elapsedTime','glue.driver.aggregate.bytesRead','glue.driver.aggregate.recordsRead','glue.driver.system.cpuSystemLoad','glue.driver.jvm.heap.usage','glue.driver.BlockManager.disk.diskSpaceUsed_MB']

            job_definition = glue_client.get_job_run(
                JobName=Job_name,
                RunId=job_run_id,
                PredecessorsIncluded=False
            )['JobRun']
            job_execution_time = job_definition['ExecutionTime']
            metric_mappings= {
                'glue.driver.aggregate.elapsedTime': [Statistics_type_avg,"count"],
                'glue.driver.aggregate.bytesRead' : [Statistics_type_sum,"count"],
                'glue.driver.aggregate.recordsRead': [Statistics_type_sum,"count"],
                'glue.driver.system.cpuSystemLoad': [Statistics_type_avg,"gauge"],
                'glue.driver.jvm.heap.usage': [Statistics_type_avg,"gauge"],
                'glue.driver.BlockManager.disk.diskSpaceUsed_MB': [Statistics_type_avg,"gauge"]    
            }
            Elapsed_time = get_job_metrics(namespace,'glue.driver.aggregate.elapsedTime',metric_type_count,job_run_id,Job_name, Statistics_type_avg )
            bytesRead = get_job_metrics(namespace,'glue.driver.aggregate.bytesRead',metric_type_count,job_run_id,Job_name, Statistics_type_avg )
            recordsRead = get_job_metrics(namespace,'glue.driver.aggregate.recordsRead',metric_type_count,job_run_id,Job_name, Statistics_type_sum )
            cpuSystemLoad = get_job_metrics(namespace,'glue.driver.system.cpuSystemLoad',metric_type_gauge,job_run_id,Job_name, Statistics_type_avg )
            heap_usage = get_job_metrics(namespace,'glue.driver.jvm.heap.usage',metric_type_gauge,job_run_id,Job_name, Statistics_type_avg )
            diskSpaceUsed_MB = get_job_metrics(namespace,'glue.driver.BlockManager.disk.diskSpaceUsed_MB',metric_type_gauge,job_run_id,Job_name, Statistics_type_avg )
            # get it from the input test json set on workflow
            met = {}
            for a in Metric_list:
                met.__setitem__(a,get_job_metrics(namespace,a,metric_mappings[a][1],job_run_id,Job_name, metric_mappings[a][0] ))
            met.__setitem__("job_execution_time",job_execution_time)
            
            Number_of_rows = 1000000
            Number_of_columns = 5
            source = "s3"
            target = "snowflake"

            dict_val = dict()
            dict_val['Workflow_run_id'] = Workflow_run_id
            dict_val['Workflow_name'] = Workflow_name
            dict_val['Job_run_id'] = job_run_id
            dict_val['Job_name'] = Job_name
            dict_val['Job_language'] = Job_language
            dict_val['Source'] = source
            dict_val['Target'] = target
            dict_val['Glue_version'] = Glue_version
            dict_val['Worker_type'] = Worker_type
            dict_val['Number_of_workers'] = Number_of_workers
            dict_val['Number_of_rows'] = Number_of_rows
            dict_val['Number_of_columns'] = Number_of_columns
            dict_val['Execution_timestamp'] = time


            for metric in met:
                # Metric_value = get_job_metrics(namespace,metric,metric_type_count,job_run_id,Job_name, Statistics_type_avg )
                
                dict_temp = copy.deepcopy(dict_val)
                dict_temp['Metric_name'] = metric
                dict_temp['Metric_value'] = met[metric]
                metrics_list.append(dict_temp)

         #   dict_val['Elapsed_time'] = Elapsed_time
         #   dict_val['bytesRead'] = bytesRead
         #   dict_val['recordsRead'] = recordsRead
         #   dict_val['cpuSystemLoad'] = cpuSystemLoad
         #   dict_val['heap_usage'] = heap_usage
         #   dict_val['diskSpaceUsed_MB'] = diskSpaceUsed_MB
         #   dict_val['Execution_timestamp'] = time


            #metrics_list.append(dict_val)
            row = [
                    Workflow_run_id,
                    Workflow_name,
                    job_run_id,
                    Job_name,
                    Job_language,
                    source,
                    target,
                    Glue_version,
                    Worker_type,
                    Number_of_workers,
                    Number_of_rows,
                    Number_of_columns,
                    Elapsed_time,
                    bytesRead,
                    recordsRead,
                    cpuSystemLoad,
                    heap_usage,
                    diskSpaceUsed_MB,
                    time,
                   ]
            w.writerow(row)
            row = []
    csvFile.close()
    bucket.upload_file('/tmp/file_name', s3_path)
    print('dictionary test :', metrics_list)
    #sourceDf = sc.parallelize(metrics_list).toDF()
    schema = StructType([ \
    StructField("Workflow_run_id",StringType(),True), \
    StructField("Workflow_name",StringType(),True), \
    StructField("Job_run_id",StringType(),True), \
    StructField("Job_name",StringType(),True), \
    StructField("Job_language",StringType(),True), \
    StructField("Source",StringType(),True), \
    StructField("Target",StringType(),True), \
    StructField("Glue_version",StringType(),True), \
    StructField("Number_of_workers", IntegerType(), True), \
    StructField("Worker_type",StringType(),True), \
    StructField("Number_of_rows", IntegerType(), True), \
    StructField("Number_of_columns", IntegerType(), True), \
    StructField("Metric_name", StringType(), True), \
    StructField("Execution_timestamp", StringType(), True), \
    StructField("Metric_value", DoubleType(), True) \
    ])
    print('schema', schema)
    df = spark.createDataFrame(data = metrics_list, schema = schema )
    print('dataframe created ')
    df.printSchema()
    print('schema printed')
    df.show(truncate=False)

    tableName="GLUE_PERFORMANCE_TEST"
    stageTable = tableName + "__STAGE"
    begin_txn = "BEGIN TRANSACTION;"
    create_main_table_if_not_exist = """create table """ + tableName +"""  IF NOT EXISTS AS SELECT * FROM """+ stageTable + """ WHERE 1=2 ;"""
    insert_into_main_table = """Insert into """ + tableName +""" SELECT * FROM """+ stageTable + """ ;"""
    truncate_stage = "TRUNCATE TABLE "+stageTable + " ;"
    commit_work = "Commit WORK;"

    preactions = begin_txn+create_main_table_if_not_exist
    postactions = insert_into_main_table+truncate_stage+commit_work
    print (preactions)
    print (postactions)
    secret_client = boto3.client("secretsmanager", region_name="us-east-1", endpoint_url="https://secretsmanager.us-east-1.amazonaws.com")
    response = secret_client.get_secret_value(SecretId=args['key_arn'])

    print ("secret generated")
    new_test = re.sub("-*(BEGIN|END) RSA PRIVATE KEY-*\r\n","", response["SecretString"]).replace("\n","").replace("\r","")
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
    # print ("Write from DF starts")

    # Sink0.write.format(SNOWFLAKE_SOURCE_NAME).options(**snowflake_properties).option("dbtable", """"+tableName+"""__STAGE").option("preactions", preactions).option("postactions", postactions).mode("append").save()
    #  DynamicFrame.fromDF(Sink0, glueContext, "test_frame_conversion")
    # print ("Write form df end")
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    print ("Writing started")
    df.write.format(SNOWFLAKE_SOURCE_NAME).options(**snowflake_properties).option("dbtable", stageTable).option("preactions", preactions).option("postactions", postactions).mode("append").save()
    #glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(sourceDf,glueContext,"sourceDf"), connection_type="custom.spark", connection_options=snowflake_properties_dynamic)
    print ("write ended")
        #namespace, metric_name, metric_type, job_run_id, job_name, Statistics_type
        #'WorkerType': 'G.1X', 'NumberOfWorkers': 3, 'GlueVersion': '2.0'

s3_client = boto3.resource('s3')
bucket = s3_client.Bucket('terraform-20210726142826483100000007')
file_name = ('glue_job_metrics' + time + '.csv')
s3_path = 'glue_job_metrics/'+run_date+'/'+file_name
write_cvs_into_s3(bucket, s3_path)
job.commit()
