-- In SQL pool tables, we can use the IDENTITY column feature. When data is inserted,
-- it will automatically assign the values of the keys.

CREATE TABLE dbo.DimProduct
(
    [ProductSK] [int] IDENTITY(1,1) NOT NULL,
	[ProductID] [int] NOT NULL,
	[ProductModelID] [int] NOT NULL,
	[ProductcategoryID] [int] NOT NULL,
	[ProductName] varchar(50) NOT NULL,	
	[ProductModelName] varchar(50) NULL,
	[ProductCategoryName] varchar(50) NULL
)