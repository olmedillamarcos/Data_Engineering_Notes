/*
Indexes are used in database systems to help query data more efficiently

Useful when queries uses the WHERE clause to search for data

In a dedicated SQL Pool, Clustered Columnstore Index is automatically created
    - good for large fact tables. Better compression and query performance

When we cannot create clustered columnstored indexes:
    - Heap tables or clustered index tables
    if the table contains varchar, nvarchar, varbinary
    if we want to create temporary tables
    if we have a table with less than 60 mi rows

Clustered Indexes - improve performance of queries that have high selective filters

non-clustered indexes - if we want to perform filter performance on other columns
     needs additional space and processing time for the index itself.
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
    DISTRIBUION = HASH,
    -- choose column which you want to be indexed
    CLUSTERED INDEX([Resource type]) 
)

/*
Creating a non Clustered index: after you define a clustered index - if we have queries 
that migh use WHERE for other columns, create the non clustered index
*/

CREATE INDEX EventCategoryIndex ON logdata ([Event category])

-- Heap tables


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
WITH  
(   
    HEAP,
    DISTRIBUTION = ROUND_ROBIN
)

CREATE INDEX ResourceTypeIndex ON logdata ([Resource type])
