from pyspark.sql.functions import col

df = spark.read.parquet("dbfs:/FileStore/parquet/log.parquet",inferSchema=True)

dfisNull = df.filter(col('Resourcegroup').isNull())

dfisNotNull = df.filter(col('Resourcegroup').isNotNull())

