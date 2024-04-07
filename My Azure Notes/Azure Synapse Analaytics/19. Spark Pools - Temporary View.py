
## 


df = spark.read.load('abfss://parquet@olmeddatalake.dfs.core.windows.net/log.parquet', format='parquet')

df.createOrReplaceTempView("logdata")

sql_1=spark.sql("SELECT Operationname, count(Operationname) FROM logdata GROUP BY Operationname")
sql_1.show()

%%sql
SELECT Operationname, count(Operationname) FROM logdata GROUP BY Operationname