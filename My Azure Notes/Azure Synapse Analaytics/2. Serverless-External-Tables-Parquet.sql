
-- Since I already created a token to acess with the csv file, just reuse it
CREATE EXTERNAL DATA SOURCE log_data_parquet
WITH (    
    LOCATION   = 'https://storageolmed.blob.core.windows.net/parquet',
    CREDENTIAL = SasToken
)

-- data compression technique used to compress the file
CREATE EXTERNAL FILE FORMAT parquetfile
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

-- attention to datetime type and removing of spacing in columnnames
CREATE EXTERNAL TABLE [logdata_parquet]
(
    [Correlationid] [varchar](200) NULL,
	[Operationname] [varchar](200) NULL,
	[Status] [varchar](100) NULL,
	[Eventcategory] [varchar](100) NULL,
	[Level] [varchar](100) NULL,
	[Time] [varchar](500) NULL,
	[Subscription] [varchar](200) NULL,
	[Eventinitiatedby] [varchar](1000) NULL,
	[Resourcetype] [varchar](1000) NULL,
	[Resourcegroup] [varchar](1000) NULL,
    [Resource] [varchar](2000) NULL)
WITH (
    -- if instead of an unique file we want everything within the container location, just change it to /
    LOCATION = './log.parquet',
    DATA_SOURCE = log_data_parquet,
    FILE_FORMAT = parquetfile
)

-- In case the table needs to be redone
DROP EXTERNAL TABLE [logdata_parquet]

SELECT * 
FROM [logdata_parquet]

