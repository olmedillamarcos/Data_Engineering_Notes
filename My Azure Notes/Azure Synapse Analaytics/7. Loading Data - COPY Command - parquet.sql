

CREATE TABLE [pool_logdata_parquet]
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
    [Resource] [varchar](2000) NULL
)

COPY INTO [pool_logdata_parquet]
FROM 'https://storageolmed.blob.core.windows.net/parquet/log.parquet'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL =(
        IDENTITY = 'Storage Account Key',
        SECRET = 'H0/+S8XrS6dBYzwAwvyFc9D9u7fgW2A8+qDqu3lRcDqQWHctCPr8lOKjOkkC0uGb1grqT6RqFlKz+AStZFrUsQ=='
    )
)

SELECT * FROM [pool_logdata_parquet]