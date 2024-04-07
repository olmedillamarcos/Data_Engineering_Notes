# Note - activate DBFS on Databricks workspae - Admin Settings - Advanced - Enable DBFS File Browser
display(spark.read.parquet("dbfs:/FileStore/parquet/log.parquet",inferSchema=True))