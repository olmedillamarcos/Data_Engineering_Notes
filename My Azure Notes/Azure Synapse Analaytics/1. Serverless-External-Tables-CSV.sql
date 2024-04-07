-- create user-defined db to host tables, as the master db holds system tables
CREATE DATABASE [appdb]

-- To create an scoped credential, we need to define a master encription key
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'P@assword123' 

-- Defining SasToken so the script can connect to the container
CREATE DATABASE SCOPED CREDENTIAL SasToken
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
-- Pay attention to validity of secret
SECRET = 'sv=2022-11-02&ss=b&srt=sco&sp=rwl&se=2024-03-01T18:18:15Z&st=2024-02-17T10:18:15Z&spr=https&sig=BtaB384l7XbI5aLFSAKIcGtOoRpINwvyblcpmSH28vQ%3D'

/*
To create an External Table, we need three things: create external data source, create external 
file format and create external table
*/ 

CREATE EXTERNAL DATA SOURCE log_data
WITH (
    LOCATION = 'https://storageolmed.blob.core.windows.net/csv',
    -- We always need a form of authorization to work with data in a azure datalake gen2
    CREDENTIAL = SasToken
)

CREATE EXTERNAL FILE FORMAT TextFileFormat
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        FIRST_ROW = 2
    )
)

CREATE EXTERNAL TABLE [logdata]
(   -- map accordingly to csv file column names (spaces, name, etc)
    [Correlation id] [varchar](200) NULL,
	[Operation name] [varchar](200) NULL,
	[Status] [varchar](100) NULL,
	[Event category] [varchar](100) NULL,
	[Level] [varchar](100) NULL,
	[Time] [datetime] NULL,
	[Subscription] [varchar](200) NULL,
	[Event initiated by] [varchar](1000) NULL,
	[Resource type] [varchar](1000) NULL,
	[Resource group] [varchar](1000) NULL,
    [Resource] [varchar](2000) NULL
)
WITH (
    LOCATION = '/Log.csv',
    DATA_SOURCE = log_data,
    FILE_FORMAT = TextFileFormat
)

SELECT * FROM [logdata]