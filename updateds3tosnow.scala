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
import java.util.Base64

object GlueApp {
  def jsonStrToMap(jsonStr: String): Map[String, String] = {
    implicit val formats = org.json4s.DefaultFormats
    
    parse(jsonStr).extract[Map[String, String]]
  }
  def getSecret(): String = {
    val secretName = "snowcred";
    val region = "us-east-2";

    // Create a Secrets Manager client
    val client:AWSSecretsManager  = AWSSecretsManagerClientBuilder.standard().withRegion(region).build()
    
    // In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    // See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    // We rethrow the exception by default.
    
    var secret= ""
    var decodedBinarySecret = ""
    var getSecretValueRequest:GetSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName)
    var getSecretValueResult:GetSecretValueResult = null

    try {
        getSecretValueResult = client.getSecretValue(getSecretValueRequest)
    } catch {
        // Secrets Manager can't decrypt the protected secret text using the provided KMS key.
        // Deal with the exception here, and/or rethrow at your discretion.
        case a:DecryptionFailureException => throw a
        // An error occurred on the server side.
        // Deal with the exception here, and/or rethrow at your discretion.
        case b:InternalServiceErrorException => throw b
        // You provided an invalid value for a parameter.
        // Deal with the exception here, and/or rethrow at your discretion.
        case c:InvalidParameterException => throw c
        // You provided a parameter value that is not valid for the current state of the resource.
        // Deal with the exception here, and/or rethrow at your discretion.
        case d:InvalidRequestException => throw d
        // We can't find the resource that you asked for.
        // Deal with the exception here, and/or rethrow at your discretion.
        case e:ResourceNotFoundException => throw e
    }
        
        
        
    

    // Decrypts secret using the associated KMS CMK.
    // Depending on whether the secret is a string or binary, one of these fields will be populated.
    if (getSecretValueResult.getSecretString() != null) {
        secret = getSecretValueResult.getSecretString()
        return secret
    }
    else {
        decodedBinarySecret = new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array())
        return decodedBinarySecret
    }
    

  }
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "URL", "WAREHOUSE", "DB", "SCHEMA").toArray)
    //val job:Job = new Job(glueContext)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    java_import(glueContext.spark_session._jvm,SNOWFLAKE_SOURCE_NAME)
    

    
    // @type: DataSource
    // @args: [format_options = JsonOptions("""{"quoteChar":"\"","escaper":"","withHeader":true,"separator":","}"""), connection_type = "s3", format = "csv", connection_options = {"paths": ["s3://test-bucket-glue-ankit/input/5m Sales Records.csv"], "recurse":true}, transformation_ctx = "DataSource0"]
    // @return: DataSource0
    // @inputs: []
    val DataSource0 = glueContext.getSourceWithFormat(connectionType = "s3", options = JsonOptions("""{"paths": ["s3://test-bucket-glue-ankit/input/5m Sales Records.csv"], "recurse":true}"""), transformationContext = "DataSource0", format = "csv", formatOptions = JsonOptions("""{"quoteChar":"\"","escaper":"","withHeader":true,"separator":","}""")).getDynamicFrame()
    // @type: ApplyMapping
    // @args: [mappings = [("Region", "string", "Region", "string"), ("Country", "string", "Country", "string"), ("Item Type", "string", "Item Type", "string"), ("Sales Channel", "string", "Sales Channel", "string"), ("Order Priority", "string", "Order Priority", "string"), ("Order Date", "string", "Order Date", "string"), ("Order ID", "long", "Order ID", "long"), ("Ship Date", "string", "Ship Date", "string"), ("Units Sold", "long", "Units Sold", "long"), ("Unit Price", "double", "Unit Price", "double"), ("Unit Cost", "double", "Unit Cost", "double"), ("Total Revenue", "double", "Total Revenue", "double"), ("Total Cost", "double", "Total Cost", "double"), ("Total Profit", "double", "Total Profit", "double")], transformation_ctx = "Transform0"]
    // @return: Transform0
    // @inputs: [frame = DataSource0]
    val Transform0 = DataSource0.applyMapping(mappings = Seq(("Region", "string", "Region", "string"), ("Country", "string", "Country", "string"), ("Item Type", "string", "Item Type", "string"), ("Sales Channel", "string", "Sales Channel", "string"), ("Order Priority", "string", "Order Priority", "string"), ("Order Date", "string", "Order Date", "string"), ("Order ID", "long", "Order ID", "long"), ("Ship Date", "string", "Ship Date", "string"), ("Units Sold", "long", "Units Sold", "long"), ("Unit Price", "double", "Unit Price", "double"), ("Unit Cost", "double", "Unit Cost", "double"), ("Total Revenue", "double", "Total Revenue", "double"), ("Total Cost", "double", "Total Cost", "double"), ("Total Profit", "double", "Total Profit", "double")), caseSensitive = false, transformationContext = "Transform0")
    // @type: DataSink
    // @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://test-bucket-glue-ankit/output/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
    // @return: DataSink0
    // @inputs: [frame = Transform0]
    val begin_txn = "BEGIN TRANSACTION;"
    val create_main_table_if_not_exist = """ create table DEMO_DB.DEMO.TESTPARQUET IF NOT EXISTS ( batchid_in varchar(64), batchid_out varchar(64), Region VARCHAR(128), Country VARCHAR(128), ItemType VARCHAR(64), SalesChannel VARCHAR(128), OrderPriority VARCHAR(128) , OrderDate VARCHAR(128), OrderID Number(38,0), ShipDate VARCHAR(128), UnitsSold Number(38,0), UnitPrice Number(38,0), UnitCost Number(38,0), TotalRevenue Number(38,0), TotalCost Number(38,0), TotalProfit Number(38,0), message_digest varchar(64) ); """
   
    val initialize_metadata = """Insert into DEMO.BATCH_METADATA (TABLENAME,BATCH_ID,BATCH_TS,START_TS,BATCH_STATUS) select
    'TESTPARQUET',
    (SELECT COALESCE( MAX(BATCH_ID), 0) +1 FROM DEMO.BATCH_METADATA  M where M.TABLENAME = 'TESTPARQUET'),
    current_timestamp,
    current_timestamp,
    'INITIALIZED';"""
    val update_hash = "Update DEMO_DB.DEMO.TESTPARQUET__STAGE set message_digest = HASH(*);"
    val update_batch_out = """Update DEMO_DB.DEMO.TESTPARQUET as T set Batchid_out = (SELECT MAX(BATCH_ID) -1 FROM DEMO.BATCH_METADATA  M where M.TABLENAME = 'TESTPARQUET')
    Where Batchid_out = 999999999 And exists ( Select * from DEMO_DB.DEMO.TESTPARQUET__STAGE S
    where T.message_digest <> S.message_digest and T.OrderID = S.OrderID  ); """
    
    val insert_into_main_table = """Insert into DEMO_DB.DEMO.TESTPARQUET
    Select (SELECT MAX(BATCH_ID) FROM DEMO.BATCH_METADATA  M where M.TABLENAME = 'TESTPARQUET'), 999999999,*
    From DEMO_DB.DEMO.TESTPARQUET__STAGE st
    Where not exists (
    Select * from DEMO_DB.DEMO.TESTPARQUET M
    Where M.batchid_out = 999999999 and
    M.message_digest = st.message_digest and m.OrderID = st.OrderID
    );"""
    val Update_metadata = """Update DEMO.BATCH_METADATA M
    Set
    M.End_ts = current_timestamp,
    M.BATCH_STATUS = 'DONE'
    where TABLENAME= 'TESTPARQUET'
    and Batch_id=(SELECT MAX(BATCH_ID) FROM DEMO.BATCH_METADATA  M where M.TABLENAME = 'TESTPARQUET' and BATCH_STATUS = 'INITIALIZED');"""
    val truncate_stage = "TRUNCATE TABLE DEMO_DB.DEMO.TESTPARQUET__STAGE;"
    val commit_work = "Commit WORK;"

    val preactions = begin_txn+create_main_table_if_not_exist
    val postactions = initialize_metadata+update_hash+update_batch_out+insert_into_main_table+Update_metadata+truncate_stage+commit_work
    val secret_value = getSecret()
    println(" secret_value :" + secret_value)
    val j = jsonStrToMap(secret_value)
    val password:String = j.get("PASSWORD")
    val username:String = j.get("USERNAME")
    var Sink0 = Transform0.toDF()
    //println(Sink0.show())
    
    glueContext.spark_session._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
    val sfOptions = Map("sfURL" -> args("URL"),
    "sfUser" -> username,
    "sfPassword" -> password,
    "sfDatabase" -> args("DB"),
    "sfSchema" -> args("SCHEMA"),
    "sfWarehouse" -> args("WAREHOUSE"),
    "dbTable" -> "TESTPARQUET__STAGE")
    
    
    Sink0.write.format(SNOWFLAKE_SOURCE_NAME).options(sfOptions).option("dbtable", "TESTPARQUET__STAGE").option("preactions", preactions).option("postactions", postactions).mode("append").save()
    //val DataSink0 = glueContext.getSinkWithFormat(connectionType = "s3", options = JsonOptions("""{"path": "s3://test-bucket-glue-ankit/output/", "partitionKeys": []}"""), transformationContext = "DataSink0", format = "json").writeDynamicFrame(Transform0)
    Job.commit()
  }
}
