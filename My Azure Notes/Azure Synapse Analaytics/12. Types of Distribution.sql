/*
When we fire a query against a table or set of tables in the dedicated SQL Pool,
the query goes onto the control node. The control node distribute the fire query to the 
computer nodes - the workload is splitted between them. 
When it comes to storage in the dedicated SQL Pool, the data is split into distributions (60
of them) used to optime performance. The compute nodes work on each distribution independently


DBCC PWD_SHOWSPACEUSED('[db].[FactSales]')

Obs: Fact Tables -> usually large, use Hash for query
Staging area -> use Round Robin for loading speed
Dimension Tables -> used to merge with Fact Tables, use Replicated
No good candidate for hash dist, no joiining key - Round Robin

When choosing Hash - ensure the data gets distributed accross the distributions to avoid
data skew. Also choose a column that minimzes data movement - is used in JOIN, GROUP BY clauses

Choosing the distribution is improtant to avoid DATA MOVEMENT - data movement accross
Compute Nodes - can ben an expensive operation.
*/

--Hash distribution
CREATE TABLE db.FactSales
(
    [ProductID] [int] NOT NULL,
	[SalesOrderID] [int] NOT NULL,
	[CustomerID] [int] NOT NULL,
	[OrderQty] [smallint] NOT NULL,
	[UnitPrice] [money] NOT NULL,
	[OrderDate] [datetime] NULL,
	[TaxAmt] [money] NULL
)
WITH (
    DISTRIBUTION = HASH(CustomerID, ProductID) -- can have multiple
)

-- Replicate Distribution
CREATE TABLE db.FactSales
(
    [ProductID] [int] NOT NULL,
	[SalesOrderID] [int] NOT NULL,
	[CustomerID] [int] NOT NULL,
	[OrderQty] [smallint] NOT NULL,
	[UnitPrice] [money] NOT NULL,
	[OrderDate] [datetime] NULL,
	[TaxAmt] [money] NULL
)
WITH (
    DISTRIBUTION = Replicate
)

-- Round Robin: standard if distribution is not specified.