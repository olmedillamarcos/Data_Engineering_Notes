from pyspark.sql.functions import col
from pyspark.sql.functions import month, day, year, to_date, to_timestamp

import datetime as dt


df = spark.read.parquet("dbfs:/FileStore/parquet/log.parquet",inferSchema=True)

display(df.select(year(col("time")),month(col('time')),day(col('time'))))



display(df.select(to_timestamp(col('Time')).alias('some date')))