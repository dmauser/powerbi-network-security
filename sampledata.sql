-- Demo schema
CREATE SCHEMA demo;
GO

-- Dimensions
CREATE TABLE demo.DimDate (
  DateKey int NOT NULL PRIMARY KEY,
  [Date] date NOT NULL,
  [Year] smallint NOT NULL,
  [Month] tinyint NOT NULL,
  [Day] tinyint NOT NULL
);

CREATE TABLE demo.DimProduct (
  ProductKey int IDENTITY(1,1) PRIMARY KEY,
  ProductName nvarchar(100) NOT NULL,
  Category nvarchar(50) NOT NULL
);

-- Fact
CREATE TABLE demo.FactSales (
  SalesKey bigint IDENTITY(1,1) PRIMARY KEY,
  DateKey int NOT NULL,
  ProductKey int NOT NULL,
  Quantity int NOT NULL,
  UnitPrice decimal(10,2) NOT NULL,
  SalesAmount AS (Quantity * UnitPrice) PERSISTED,
  ModifiedAt datetime2(3) NOT NULL,
  CONSTRAINT FK_FactSales_Date FOREIGN KEY (DateKey) REFERENCES demo.DimDate(DateKey),
  CONSTRAINT FK_FactSales_Product FOREIGN KEY (ProductKey) REFERENCES demo.DimProduct(ProductKey)
);
GO

-- Populate DimDate (last 365 days)
DECLARE @start date = DATEADD(day,-364, CAST(GETDATE() as date));
;WITH n AS (
  SELECT TOP (365) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS i
  FROM sys.all_objects
)
INSERT demo.DimDate(DateKey,[Date],[Year],[Month],[Day])
SELECT
  CONVERT(int, FORMAT(DATEADD(day,i,@start),'yyyyMMdd')) AS DateKey,
  DATEADD(day,i,@start) AS [Date],
  DATEPART(year, DATEADD(day,i,@start)) AS [Year],
  DATEPART(month, DATEADD(day,i,@start)) AS [Month],
  DATEPART(day, DATEADD(day,i,@start)) AS [Day]
FROM n;

-- Populate DimProduct
INSERT demo.DimProduct(ProductName, Category)
VALUES
 ('Contoso Laptop 13','Computers'),
 ('Contoso Laptop 15','Computers'),
 ('Contoso Mouse','Accessories'),
 ('Contoso Keyboard','Accessories'),
 ('Contoso Monitor 27','Displays'),
 ('Contoso Dock','Accessories'),
 ('Contoso Headset','Accessories'),
 ('Contoso Tablet','Mobile');

-- Populate FactSales (~50k rows)
;WITH n AS (
  SELECT TOP (50000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
  FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT demo.FactSales(DateKey, ProductKey, Quantity, UnitPrice, ModifiedAt)
SELECT
  (SELECT TOP 1 DateKey FROM demo.DimDate ORDER BY NEWID()) AS DateKey,
  (SELECT TOP 1 ProductKey FROM demo.DimProduct ORDER BY NEWID()) AS ProductKey,
  1 + ABS(CHECKSUM(NEWID())) % 5 AS Quantity,
  CAST(20 + ABS(CHECKSUM(NEWID())) % 900 AS decimal(10,2)) AS UnitPrice,
  DATEADD(minute, -1 * (ABS(CHECKSUM(NEWID())) % 20000), SYSUTCDATETIME()) AS ModifiedAt
FROM n;
GO

-- Helpful index for incremental loads
CREATE INDEX IX_FactSales_ModifiedAt ON demo.FactSales(ModifiedAt);
GO