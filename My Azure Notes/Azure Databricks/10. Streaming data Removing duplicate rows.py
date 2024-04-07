spark.conf.set(
   'fs.azure.account.key.olmeddatalake.dfs.core.windows.net',
   'DhtnlCMs+qYSajEYzrDvTU91KDkkLL9w88Sav94BoYBtPIHGqdgEt9zjIcndR8Kc47mTmQuJasYY+AStAjRlMQ=='
)

df= spark.read.csv("abfss://csv@olmeddatalake.dfs.core.windows.net/DimCustomer/Customer01.csv",header=True)

from pyspark.sql.functions import col

display(df.groupBy(df.CustomerID).count().alias('Count').filter(col('Count')>1))