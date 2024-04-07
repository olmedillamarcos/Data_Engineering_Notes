# Batch streaming - using Autoloader

%sql
CREATE TABLE DimCustomer(
  CustomerID STRING,
  CompanyName STRING,
  SalesPerson STRING
)

spark.conf.set(
   'fs.azure.account.key.olmeddatalake.dfs.core.windows.net',
   'DhtnlCMs+qYSajEYzrDvTU91KDkkLL9w88Sav94BoYBtPIHGqdgEt9zjIcndR8Kc47mTmQuJasYY+AStAjRlMQ=='
)


path = "abfss://csv@olmeddatalake.dfs.core.windows.net/DimCustomer/"
checkpointPath =  "abfss://checkpoint@olmeddatalake.dfs.core.windows.net/"
schemaLocation =  "abfss://schema@olmeddatalake.dfs.core.windows.net/"


dfDimCustomer = spark.readStream \
    .format("cloudfiles") \
    .option("cloudFiles.schemaLocation", schemaLocation) \
    .option("cloudFiles.format","csv") \
    .load(path)

finalDimCustomer = dfDimCustomer.dropDuplicates("CustomerID")

finalDimCustomer.writeStream.format('delta') \
    .option("checkpointLocation",checkpointPath) \
    .option("mergeSchema","true") \
    .table("DimCustomer")


%sql

SELECT * FROM DimCustomer

## If we add more data into the container, Azure Databricks will detect the added file
## and will add it into the table.