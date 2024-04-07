spark.conf.set(
   'fs.azure.account.key.datalekolmed.dfs.core.windows.net',
   'dCU6ACNyvzbiq2+yW3yGp9jtPZr5Z1/NFxvcVMu/Nr6k1t5MQ7YDosmOUS1Lq6NpqiwL1C4z9A7b+AStOX40Ug=='
)

from pyspark.sql.types import StructType, StructField, IntegerType, array, StringType

path = "abfss://csv@datalekolmed.dfs.core.windows.net/DimCustomer/"
checkpointPath =  "abfss://checkpoint@datalekolmed.dfs.core.windows.net/"
dataSchema = StructType([
    StructField("CustomerID",IntegerType(),True),
    StructField("CompanyName",StringType(),True),
    StructField("SalesPerson",StringType(),True),
])

dfDimCustomer = spark.readStream \
    .schema(dataSchema) \
    .format("cloudfiles") \
    .option("header","true") \
    .option("cloudFiles.format","csv") \
    .load(path)

dfDimCustomer.writeStream \
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://synapsewsolmed.sql.azuresynapse.net:1433;database=pooldb") \
  .option("user","sqladminuser") \
  .option("password","Engenharia@usp93") \
  .option("tempDir", "abfss://staging@datalekolmed.dfs.core.windows.net/databricks")  \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "DimCustomer") \
  .option("checkpointLocation", checkpointPath) \
  .start()
