/*
Usings SSMS and connectin both azure synapse and the SQL Database that we will
 fetch the data from to it.
*/

/* Since this in a test environment, we can  create a VIEW to understand the data, what
to copy, etc, CREATE a table based on our decision, and copy that into synapse.
However, usage of stage tables is also a viable solution, if we don't want to affect our
production database.
*/

CREATE VIEW [Sales_Fact_View]
AS
SELECT dt.ProductID, dt.SalesOrderID, dt.OrderQty, dt.UnitPrice, hd.OrderDate, hd.CustomerID, hd.TaxAmt
FROM SalesLT.SalesOrderDetail dt
JOIN SalesLT.SalesOrderHeader hd
ON dt.SalesOrderID = hd.SalesOrderID

SELECT * 
INTO FactSales
FROM Sales_Fact_View

SELECT * FROM FactSales