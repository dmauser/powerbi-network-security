-- ============================================================
-- Demo Schema: Simple Sales Model
-- ============================================================

-- Products table
CREATE TABLE dbo.Products (
    ProductID    INT PRIMARY KEY IDENTITY(1,1),
    ProductName  NVARCHAR(100) NOT NULL,
    Category     NVARCHAR(50)  NOT NULL,
    UnitPrice    DECIMAL(10,2) NOT NULL
);

-- Customers table
CREATE TABLE dbo.Customers (
    CustomerID   INT PRIMARY KEY IDENTITY(1,1),
    CustomerName NVARCHAR(100) NOT NULL,
    Region       NVARCHAR(50)  NOT NULL,
    Country      NVARCHAR(50)  NOT NULL
);

-- Sales Fact table
CREATE TABLE dbo.SalesFact (
    SaleID       INT PRIMARY KEY IDENTITY(1,1),
    SaleDate     DATE          NOT NULL,
    CustomerID   INT           NOT NULL REFERENCES dbo.Customers(CustomerID),
    ProductID    INT           NOT NULL REFERENCES dbo.Products(ProductID),
    Quantity     INT           NOT NULL,
    TotalAmount  DECIMAL(10,2) NOT NULL
);

-- ============================================================
-- Seed: Products
-- ============================================================
INSERT INTO dbo.Products (ProductName, Category, UnitPrice) VALUES
('Azure VM D4s',      'Compute',    500.00),
('Azure SQL DB S2',   'Database',   150.00),
('Azure Blob Storage','Storage',     20.00),
('Azure Firewall',    'Security',   800.00),
('ExpressRoute 1Gbps','Networking', 1500.00),
('Azure Monitor',     'Management',  75.00),
('Azure Backup',      'Storage',     45.00),
('Azure Kubernetes',  'Compute',    600.00);

-- ============================================================
-- Seed: Customers
-- ============================================================
INSERT INTO dbo.Customers (CustomerName, Region, Country) VALUES
('Contoso Energy',    'South Central', 'USA'),
('Fabrikam Power',    'West',          'USA'),
('Northwind Utilities','East',         'USA'),
('Adventure Works',   'North',        'Canada'),
('Tailspin Renewables','Southeast',   'USA');

-- ============================================================
-- Seed: Sales Fact (12 months of data)
-- ============================================================
DECLARE @i INT = 0;
WHILE @i < 500
BEGIN
    INSERT INTO dbo.SalesFact (SaleDate, CustomerID, ProductID, Quantity, TotalAmount)
    SELECT
        DATEADD(DAY, -ABS(CHECKSUM(NEWID()) % 365), GETDATE()),
        (ABS(CHECKSUM(NEWID())) % 5) + 1,
        (ABS(CHECKSUM(NEWID())) % 8) + 1,
        (ABS(CHECKSUM(NEWID())) % 10) + 1,
        ((ABS(CHECKSUM(NEWID())) % 10) + 1) * (SELECT UnitPrice FROM dbo.Products WHERE ProductID = ((ABS(CHECKSUM(NEWID())) % 8) + 1));
    SET @i = @i + 1;
END;

-- Verify
SELECT 'Products' AS TableName, COUNT(*) AS [RowCount] FROM dbo.Products
UNION ALL SELECT 'Customers', COUNT(*) FROM dbo.Customers
UNION ALL SELECT 'SalesFact', COUNT(*) FROM dbo.SalesFact;