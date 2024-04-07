/*
Helps when queries are based on filtering data, when using the WHERE clause

They are normally created in a date column

Supported for all distribution and indexes types
    - When it comes to clustered columnstore tables, for optimal compression and performance,
    we should have at least a minimum of 1 mi rows per distribution and partition
    - For example, partitioning on month - 12 partitions*60 distributiosn * 1mi = 720 mi rows

Inserting and deleting data is easier with partitions (it can be expensive in large data
warehouses)
*/

CREATE TABLE logdata
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
WITH (
    DISTRIBUTION = HASH,
    -- RANGE RIGHT FOR VALUES selects the intervals to define the partitions
	-- for instance, it divides in  < '2023-01-01', >= '2023-02-01' & < 2023-02-01' & ..
	-- ..& >= 2023-04-01'
    PARTITION (TIME RANGE RIGHT FOR VALUES
            ('2023-01-01','2023-02-01','2023-03-01','2023-04-01')) 
)

-- Switching Partitions: lets say we want to delete data based on a certain partition

CREATE TABLE logdata_new
WITH (
    DISTRIBUTION = HASH,
    PARTITION (TIME RANGE RIGHT FOR VALUES
        ('2023-02-01','2023-03-01','2023-04-01')) 
)
AS 
SELECT * 
FROM logdata
WHERE 1=2

ALTER TABLE logdata SWITCH PARTITION 2 to logdata_new PARTITION 1   