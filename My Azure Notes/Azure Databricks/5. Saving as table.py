df = spark.read.parquet("dbfs:/FileStore/parquet/log.parquet",inferSchema=True)

df.wirte.saveAsTable('logdata')

%sql
SELECT * FROM logdata