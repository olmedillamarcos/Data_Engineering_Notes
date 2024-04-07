
## For staging area in storage
spark.conf.set(
   'fs.azure.account.key.datalekolmed.dfs.core.windows.net',
   'dCU6ACNyvzbiq2+yW3yGp9jtPZr5Z1/NFxvcVMu/Nr6k1t5MQ7YDosmOUS1Lq6NpqiwL1C4z9A7b+AStOX40Ug=='
)

df = spark.read \
    .format("com.databricks.spark.sqldw") \
    .option("url", "jdbc:sqlserver://synapsewsolmed.sql.azuresynapse.net:1433;database=pooldb") \
    .option("user","sqladminuser") \
    .option("password","yourpassword") \
    .option("tempDir", "abfss://staging@datalekolmed.dfs.core.windows.net/databricks")  \
    .option("forwardSparkAzureStorageCredentials", "true") \
    .option("dbTable", "pool_logdata") \
    .load()

## In the used example, I used a parquetfile, which cannot have whitespace

from pyspark.sql.functions import col

## removing whitespace from columns

renamed_df = df.select([col(i).alias(i.replace(' ', '_')) for i in df.columns])

display(renamed_df)