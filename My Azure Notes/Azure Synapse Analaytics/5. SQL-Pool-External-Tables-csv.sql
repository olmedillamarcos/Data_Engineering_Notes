/*
If we try to read data from the dedicated SQL Pool the same way as the serverless SQL Pool 
using the external table structure, we won't be able to use some of features we used before due to 
The underlying driver used in Azure Synapse to read data from Data Lake Gen2 having different
capabilities when it comes to dedicated/serverless sql pools. The native drives does not support
csv reading in dedicated SQL Pool for external tables. We have to use a Hadoop External Table.
*/


--We start creating a different scoped credential using access key
CREATE DATABASE SCOPED CREDENTIAL AzureStorageCredential
WITH
    IDENTITY = 'storageolmed',
    SECRET  = '2ZR94cCDFby8uN12JfUSGYL7zLUxES3lRly5zT5qMb/Smynh7oEgQ9RWL1Kyf8HG7zjECQ4Z+dNs+ASta97kGg=='


CREATE EXTERNAL DATA SOURCE log_data
WITH (
    -- For Hadoop, we need to used abfss protocol
    LOCATION = 'abfss://csv@storageolmed.dfs.core.windows.net',
    CREDENTIAL = AzureStorageCredential,
    TYPE = HADOOP
)

CREATE EXTERNAL FILE FORMAT TextFileFormat
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        FIRST_ROW = 2
    )

)

-- The Hadoop driver cannot convert dattime format accordingly - use varchar
CREATE EXTERNAL TABLE [logdata]
(
    [Correlation id] [varchar](200) NULL,
	[Operation name] [varchar](200) NULL,
	[Status] [varchar](100) NULL,
	[Event category] [varchar](100) NULL,
	[Level] [varchar](100) NULL,
	[Time] [VARCHAR](500) NULL,
	[Subscription] [varchar](200) NULL,
	[Event initiated by] [varchar](1000) NULL,
	[Resource type] [varchar](1000) NULL,
	[Resource group] [varchar](1000) NULL,
    [Resource] [varchar](2000) NULL
)
WITH (
    LOCATION = './Log.csv',
    DATA_SOURCE = log_data,
    FILE_FORMAT = TextFileFormat
)

DROP EXTERNAL TABLE [logdata]

SELECT * FROM [logdata]