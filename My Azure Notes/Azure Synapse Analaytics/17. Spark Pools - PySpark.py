
## Creatine a DataFrame

courses = [(1,'AZ-900',10.99),(2,'DP-203',11.99) ,(3,'AZ-104',12.99)]
df = spark.createDataFrame(courses,['Id','Name','Price'])

df.show()

from pyspark.sql.functions import col
from pyspark.sql.functions import desc

sorted_df = df.sort(col('Price').desc())

sorted_df.show()