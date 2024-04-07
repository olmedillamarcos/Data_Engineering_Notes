#$# note: always remember the explode function.
df = spark.read.json('dbfs:/FileStore/json/Log.json')

df2 = spark.read.json('dbfs:/FileStore/json/customer_obj.json')

display(df2.select(col("customerid"),col("customername"),col("registered"),explode(col("courses"))))