CREATE DATABASE appdb

USE appdb

CREATE TABLE logdata
/* 
Note that no schema was defined for the table. Normally in relational datastore, we should
define the columns and its types.
In a Delta Table, the data is residing within files, and the structure of the table is stored
in the metastore.
*/

spark.conf.set(
   'fs.azure.account.key.olmeddatalake.dfs.core.windows.net',
   'DhtnlCMs+qYSajEYzrDvTU91KDkkLL9w88Sav94BoYBtPIHGqdgEt9zjIcndR8Kc47mTmQuJasYY+AStAjRlMQ=='
)

COPY INTO logdata
FROM "abfss://parquet@olmeddatalake.dfs.core.windows.net/log.parquet"
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'true')

SELECT * FROM logdata