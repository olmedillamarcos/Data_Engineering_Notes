## We need authorization to the Azure Account 
## Note: we can use a secret scope so we don't embed the key into our code.

spark.conf.set(
   'fs.azure.account.key.olmeddatalake.dfs.core.windows.net',
   'DhtnlCMs+qYSajEYzrDvTU91KDkkLL9w88Sav94BoYBtPIHGqdgEt9zjIcndR8Kc47mTmQuJasYY+AStAjRlMQ=='
)

df= spark.read.csv("abfss://csv@olmeddatalake.dfs.core.windows.net/Log.csv",inferSchema=True)