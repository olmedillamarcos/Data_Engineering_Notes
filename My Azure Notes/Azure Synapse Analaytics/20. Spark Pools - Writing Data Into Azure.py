df = spark.read.option("header","true").csv("abfss://csv@datalake244434.dfs.core.windows.net/Log.csv")
display(df)
 
 
# To write to pooldb
# We need to match the schema properly
# Make sure the Azure admin has the storage blob reader and contributor role
 
from pyspark.sql.types import StructType,StringType,TimestampType
 
dataSchema = StructType() \
    .add("Correlation id", StringType(), True) \
    .add("Operation name", StringType(), True) \
    .add("Status", StringType(), True) \
    .add("Event category",StringType(), True) \
    .add("Level",StringType(),True) \
    .add("Time", TimestampType(), True) \
    .add("Subscription",StringType(), True) \
    .add("Event initiated by", StringType(), True) \
    .add("Resource type",StringType(),True) \
    .add("Resource group",StringType(),True) \
    .add("Resource",StringType(),True)
 
df = spark.read.format("csv") \
.option("header",True) \
.schema(dataSchema) \
.load("abfss://csv@olmeddatalake.dfs.core.windows.net/Log.csv")
 
display(df)
 
 
 
import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants
 
df.write \
    .option(Constants.SERVER,"dataworkspace2000939.sql.azuresynapse.net") \
    .option(Constants.USER,"sqladminuser") \
    .option(Constants.PASSWORD,"sqlpassword@123") \
    .option(Constants.DATA_SOURCE,"pooldb") \
    .option(Constants.TEMP_FOLDER,"abfss://staging@datalake244434.dfs.core.windows.net") \
    ## Don't forget to give contributor priviliges to user 
    .option(Constants.STAGING_STORAGE_ACCOUNT_KEY,"dilbGv2rof6G4emB0qWgVwAOOexu/bIpvJiUnfal7+klHqCsKLB+JkQzMfRlgu0fm14iUFNHXPeU+AStZZXK2w==") \
    .mode("overwrite") \
    .synapsesql("pooldb.dbo.logdata")     