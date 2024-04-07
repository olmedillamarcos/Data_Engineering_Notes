
-- Same idea, we create the views and create tables in the SQL db.

CREATE VIEW Customer_view 
AS
SELECT ct.CustomerID,ct.CompanyName,ct.SalesPerson
FROM SalesLT.Customer ct  

-- Creating custoemr dimension table

SELECT CustomerID,CompanyName,SalesPerson
INTO DimCustomer
FROM Customer_view 


-- Creating product dimension view

CREATE VIEW Product_view 
AS
SELECT prod.ProductID,prod.Name as ProductName,model.ProductModelID,model.Name as ProductModelName,category.ProductcategoryID,category.Name AS ProductCategoryName
FROM SalesLT.Product prod
JOIN SalesLT.ProductModel model ON prod.ProductModelID = model.ProductModelID
JOIN SalesLT.ProductCategory category ON prod.ProductcategoryID=category.ProductcategoryID

-- Creating product dimension table

SELECT ProductID,ProductModelID,ProductcategoryID,ProductName,ProductModelName,ProductCategoryName
INTO DimProduct
FROM Product_view 



