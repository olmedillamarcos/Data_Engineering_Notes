/*
The purpose of a SQL Pool is to create a SQL data warehouse, so we need to use persistent 
tables. Normally in a SQL Database, we use the INSERT command to insert rows. When it comes
to data warehouses, due to the large amount of data being transfered, data is usually 
transferred in chunks. For a data warehouse, what is important is HOW we ready our data.

*/

CREATE TABLE [pool_logdata]
(
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

COPY INTO [pool_logdata]
FROM 'https://storageolmed.blob.core.windows.net/csv/Log.csv'
WITH (
    FILE_TYPE = 'CSV',
    CREDENTIAL = (
        IDENTITY = 'Storage Account Key',
        SECRET = 'H0/+S8XrS6dBYzwAwvyFc9D9u7fgW2A8+qDqu3lRcDqQWHctCPr8lOKjOkkC0uGb1grqT6RqFlKz+AStZFrUsQ=='
    ),
    FIRSTROW = 2
)

SELECT  * FROM [pool_logdata]