import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.DynamicFrame
import com.amazonaws.services.glue.util.JsonOptions
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClient
import com.amazonaws.services.secretsmanager._
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult
import com.amazonaws.services.secretsmanager.model.DecryptionFailureException
import com.amazonaws.services.secretsmanager.model.InternalServiceErrorException
import com.amazonaws.services.secretsmanager.model.InvalidRequestException
import com.amazonaws.services.secretsmanager.model.InvalidParameterException 
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.util._
import scala.collection.Map
object GlueApp {
  

  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "key_arn","format" ).toArray)
    
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    var tableName  = ""
    var testDataPath 
    val fileFormat = args("format")
    var jsonformatOptions = JsonOptions("""{}""")
    if(fileFormat == "csv"){
        testDataPath = JsonOptions("""{"paths": ["s3://terraform-20210726142826483100000007/performanceTestData/csvData.csv"], "recurse":true}""")
        jsonformatOptions = JsonOptions("""{"quoteChar":"\"","escaper":"","withHeader":true,"separator":","}""")
        tableName = "TEST_CSV_SCALA"
    }
    else if (fileFormat == "parquet"){
        testDataPath = JsonOptions("""{"paths": ["s3://terraform-20210726142826483100000007/performanceTestData/parquetData.parquet"], "recurse":true}""")
        jsonformatOptions = JsonOptions("""{}""")
        tableName = "TEST_PARQUET_SCALA"
    }
    else if (fileFormat == "json"){
        testDataPath = JsonOptions("""{"paths": ["s3://terraform-20210726142826483100000007/performanceTestData/jsonData.json"], "recurse":true}""")
        jsonformatOptions = JsonOptions("""{"jsonPath":"$.*", "multiline":True}""")
        tableName = "TEST_JSON_SCALA"
    }
    else{
        println ("WRONG FILE FORMAT : Expected (json,csv,parquet)")
    }
    println ("Source Read Starts")
    val DataSource0 = glueContext.getSourceWithFormat(connectionType = "s3", options = testDataPath, transformationContext = "DataSource0", format = fileFormat, formatOptions = jsonformatOptions).getDynamicFrame()
    println ("Source Read Success")
    println ("Transform Starts")
    val Transform0 = DataSource0.applyMapping(mappings = Seq(("Region", "string", "Region", "string"), ("Country", "string", "Country", "string"), ("Item_Type", "string", "ItemType", "string"), ("Sales_Channel", "string", "SalesChannel", "string"), ("Order_Priority", "string", "OrderPriority", "string"), ("Order_Date", "string", "OrderDate", "string"), ("Order_ID", "string", "OrderID", "string"), ("Ship_Date", "string", "ShipDate", "string"), ("Units_Sold", "string", "UnitsSold", "string"), ("Unit_Price", "string", "UnitPrice", "string"), ("Unit_Cost", "string", "UnitCost", "string"), ("Total_Revenue", "string", "TotalRevenue", "string"), ("Total_Cost", "string", "TotalCost", "string"), ("Total_Profit", "string", "TotalProfit", "string"), ("message_digest", "string", "message_digest", "string" )), caseSensitive = false, transformationContext = "Transform0")
    println ("Transform Ends")
    val begin_txn = "BEGIN TRANSACTION;"
    val create_main_table_if_not_exist = """create table """ + tableName +"""  IF NOT EXISTS ( batchid_in varchar(64), batchid_out varchar(64), Region VARCHAR(128), Country VARCHAR(128), ItemType VARCHAR(64), SalesChannel VARCHAR(128), OrderPriority VARCHAR(128) , OrderDate VARCHAR(128), OrderID Number(38,0), ShipDate VARCHAR(128), UnitsSold Number(38,0), UnitPrice Number(38,0), UnitCost Number(38,0), TotalRevenue Number(38,0), TotalCost Number(38,0), TotalProfit Number(38,0), message_digest varchar(64) ); """

    val create_metadata_table_if_not_exist = """CREATE TABLE BATCH_METADATA IF NOT EXISTS
                                            (
                                            TABLENAME  varchar(255) NOT NULL,
                                            BATCH_ID   integer  NOT NULL,
                                            BATCH_TS timestamp NOT null,
                                            START_TS timestamp NOT null,
                                            END_TS timestamp NULL,
                                            BATCH_STATUS varchar(32) NOT Null,
                                            Primary key ( TABLENAME,  BATCH_ID, BATCH_TS)
                                            ); """
    val initialize_metadata = """Insert into BATCH_METADATA (TABLENAME,BATCH_ID,BATCH_TS,START_TS,BATCH_STATUS) select
    '""" + tableName + """',
    (SELECT COALESCE( MAX(BATCH_ID), 0) +1 FROM PUBLIC.BATCH_METADATA  M where M.TABLENAME = '"""+tableName+"""'),
    current_timestamp,
    current_timestamp,
    'INITIALIZED';"""
    val update_hash = "Update " +tableName+"__STAGE set message_digest = HASH(*);"
    val update_batch_out = """Update """ + tableName + """ as T set Batchid_out = (SELECT MAX(BATCH_ID) -1 FROM PUBLIC.BATCH_METADATA  M where M.TABLENAME = '"""+tableName+"""')
    Where Batchid_out = 999999999 And exists ( Select * from """+tableName+"""__STAGE S where T.message_digest <> S.message_digest and T.OrderID = S.OrderID  ); """
   
    val insert_into_main_table = """Insert into """ + tableName +"""
    Select (SELECT MAX(BATCH_ID) FROM BATCH_METADATA  M where M.TABLENAME = ' """ +tableName+ """'), 999999999,*
    From """+tableName+"""__STAGE st
    Where not exists (
    Select * from """+tableName+""" M
    Where M.batchid_out = 999999999 and
    M.message_digest = st.message_digest and m.OrderID = st.OrderID
    );"""
    val update_metadata = """Update BATCH_METADATA M
    Set
    M.End_ts = current_timestamp,
    M.BATCH_STATUS = 'DONE'
    where TABLENAME='"""+tableName+"""'
    and Batch_id=(SELECT MAX(BATCH_ID) FROM BATCH_METADATA  M where M.TABLENAME = '"""+tableName+"""' and BATCH_STATUS = 'INITIALIZED');"""
    val truncate_stage = "TRUNCATE TABLE "+tableName+"__STAGE;"
    val commit_work = "Commit WORK;"

    val preactions = begin_txn+create_main_table_if_not_exist +create_metadata_table_if_not_exist
    val postactions = initialize_metadata+update_hash+update_batch_out+insert_into_main_table+update_metadata+truncate_stage+commit_work

    println("Write Starts")
    val stageTable = tableName + "__STAGE"
    val snowflake_properties_dynamic = JsonOptions("""
        {
        "sfUrl": "sfceawseast1d01.us-east-1.privatelink.snowflakecomputing.com",
        "sfDatabase": "CNAS_DEV",
        "sfWarehouse": "CNAS_DEV_WAREHOUSE",
        "pem_private_key": new_test,
        "sfUser": "CNAS_DEV_SNOWFLAKE_GLUE_USER",
        "sfRole": "CNAS_DEV_ROLE",
        "sfSchema": "PUBLIC",
        "connectionName": "snowflakeConnection",
        "dbtable": """+stageTable+""",        
        "preactions":"""+preactions+""",
        "postactions":"""+postactions+"""
        }
        """
    )
    // # print ("Write from DF starts")
    // # Sink0 = Transform0.toDF()
    // # Sink0.printSchema()
    // # Sink0.show()
    // # Sink0.write.format(SNOWFLAKE_SOURCE_NAME).options(**snowflake_properties).option("dbtable", """"+tableName+"""__STAGE").option("preactions", preactions).option("postactions", postactions).mode("append").save()
    // #  DynamicFrame.fromDF(Sink0, glueContext, "test_frame_conversion")
    // # print ("Write form df end")
    // print ("Writing started")
    //glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type="custom.spark", connection_options=snowflake_properties_dynamic)
    val DataSink0 = glueContext.getSinkWithFormat(connectionType = "custom.spark", options = snowflake_properties_dynamic, transformationContext = "DataSink0", format = fileFormat).writeDynamicFrame(Transform0)
    println("Write Success")
    Job.commit()
    
  }
}
