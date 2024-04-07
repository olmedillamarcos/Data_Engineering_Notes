/* Since we are again connecting to an external source, we need to do the same steps we did with 
an external table in serverless sql pool: create a master key encryption, a database scoped credential
external source, file format and table
*/

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'P@ssword@123'

CREATE DATABASE SCOPED CREDENTIAL SasToken
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET='sv=2022-11-02&ss=b&srt=sco&sp=rwl&se=2024-03-01T18:18:15Z&st=2024-02-17T10:18:15Z&spr=https&sig=BtaB384l7XbI5aLFSAKIcGtOoRpINwvyblcpmSH28vQ%3D'


CREATE EXTERNAL DATA SOURCE log_data_parquet
WITH (
    LOCATION = 'https://storageolmed.blob.core.windows.net/parquet',
    CREDENTIAL = SasToken
)

CREATE EXTERNAL FILE FORMAT parquetfile
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec' 
)

CREATE EXTERNAL TABLE [logdata_parquet]
(
    [Correlationid] [varchar](200) NULL,
	[Operationname] [varchar](200) NULL,
	[Status] [varchar](100) NULL,
	[Eventcategory] [varchar](100) NULL,
	[Level] [varchar](100) NULL,
	[Time] [datetime](500) NULL,
	[Subscription] [varchar](200) NULL,
	[Eventinitiatedby] [varchar](1000) NULL,
	[Resourcetype] [varchar](1000) NULL,
	[Resourcegroup] [varchar](1000) NULL,
    [Resource] [varchar](2000) NULL
)
WITH (
    LOCATION  = '/log.parquet',
    DATA_SOURCE = log_data_parquet,
    FILE_FORMAT = parquetfile
)

DROP EXTERNAL TABLE [logdata_parquet]

SELECT * FROM [logdata_parquet]