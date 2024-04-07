/*
Same approach used to create an external table - then copy if to the normal table.
PolyBase is a very efficient way to load data from an external table.


 Given the previous works, we can always check our current databas scoped credentials, 
data sources and file formtas checking system tables and reuse them.

Copy Command vs PolyBase vs Bulk insert

- Bulk is the slowest
    With Bulk insert all of the commands go through the Control Node.
    Better with less data to transfer

- PolyBase performs better on large amounts of data
    data movement operations go through the compute nodes in parallel

- COPY command is simple to execute, no need to define external source, file format and table
*/

SELECT * FROM sys.database_scoped_credentials 

SELECT * FROM sys.external_file_formats

SELECT * FROM sys.external_data_sources

DROP TABLE [pool_logdata_parquet]

CREATE TABLE [pool_logdata_parquet]
WITH (
    DISTRIBUTION = ROUND_ROBIN
)
AS 
SELECT * 
FROM [logdata_parquet]