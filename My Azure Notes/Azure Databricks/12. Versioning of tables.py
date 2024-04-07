from pyspark.sql.types import StructType, StructField, IntegerType, array, StringType


%sql
CREATE TABLE DimCustomer(
  CustomerID INT,
  CompanyName STRING,
  SalesPerson STRING
)

dataSchema = StructType([
    StructField("CustomerID",IntegerType(),True),
    StructField("CompanyName",StringType(),True),
    StructField("SalesPerson",StringType(),True),
])

spark.conf.set(
   'fs.azure.account.key.olmeddatalake.dfs.core.windows.net',
   'DhtnlCMs+qYSajEYzrDvTU91KDkkLL9w88Sav94BoYBtPIHGqdgEt9zjIcndR8Kc47mTmQuJasYY+AStAjRlMQ=='
)


path = "abfss://csv@olmeddatalake.dfs.core.windows.net/DimCustomer/"
checkpointPath =  "abfss://checkpoint@olmeddatalake.dfs.core.windows.net/"
schemaLocation =  "abfss://schema@olmeddatalake.dfs.core.windows.net/"


dfDimCustomer = spark.readStream \
    .schema(dataSchema) \
    .format("cloudfiles") \
    .option("header","true") \
    .option("cloudFiles.format","csv") \
    .load(path)

finalDimCustomer = dfDimCustomer.dropDuplicates("CustomerID")

finalDimCustomer.writeStream.format('delta') \
    .option("checkpointLocation",checkpointPath) \
    .option("mergeSchema","true") \
    .table("DimCustomer")


%sql

SELECT * FROM DimCustomer

## Check versions of table
%sql
DESCRIBE HISTORY DimCustomer

## Choose version
%sql
SELECT * FROM dimcustomer VERSION AS OF 1