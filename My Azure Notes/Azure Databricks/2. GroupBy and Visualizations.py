
from pyspark.sql.functions import col


df = spark.read.parquet("dbfs:/FileStore/parquet/log.parquet",inferSchema=True)


display(df.groupby(col('Operationname')).count().alias("Count").filter(col("Count")>100))