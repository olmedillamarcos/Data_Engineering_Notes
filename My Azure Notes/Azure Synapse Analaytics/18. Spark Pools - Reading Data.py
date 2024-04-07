
from pyspark.sql.functions import col

df = spark.read.load('abfss://parquet@olmeddatalake.dfs.core.windows.net/log.parquet',
format='parquet')

# selecting certain columns

display(df.select(col('col1'),col('col2'),...))

## Null values

filtereddf = df.filter(col('col').isNull())

## number of rows
rows = filtereddf.count()

## groupingby
groupedbydf = df.groupBy('col').count()

