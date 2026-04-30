# 🏗️ Lab Demo: Power BI via Fabric Lakehouse → Azure SQL (Private Only via Managed Private Endpoint)

## 🗺️ Architecture Overview

```
Azure SQL Database (Public Access DISABLED)
        │
        │  ← Managed Private Endpoint (approved)
        ▼
Microsoft Fabric Workspace (Managed VNet)
        │
        ├── Notebook (PySpark) ─► Fabric Lakehouse (Delta Tables)
        │                              │
        └── Data Pipeline ────────────┘
                                       │
                                  Semantic Model (auto)
                                       │
                                  Power BI Report
```

---

## ⚠️ Critical Prerequisites (Read Before Starting!)

| Requirement | Details |
|---|---|
| **Fabric Capacity** | **F64 or higher, OR Fabric Trial** — Managed Private Endpoints are NOT available on F2/F4/F8/F16/F32 or P-SKUs |
| **Azure Subscription** | `Microsoft.Network` resource provider must be registered |
| **Workspace Admin** | You need admin permissions on the Fabric workspace |
| **Azure SQL Admin** | You need rights to approve private endpoint connections on the SQL server |
| **Fabric Region** | MPE requires Fabric Data Engineering workloads in your region |
| **Custom Pool** | MPE workspaces cannot use Starter Pools — Spark will use a Custom Pool |

> 💡 **For demo purposes**: A **Fabric Trial** capacity works perfectly and is free — it gives you F64-equivalent throughput for 60 days. Activate it at https://app.fabric.microsoft.com.

---

## 📦 PART 1 — Azure SQL Database Setup (Demo Data)

### Step 1.1 — Deploy Azure SQL + Seed Demo Data

Run this in **Azure Cloud Shell (PowerShell)** or Azure CLI:

```bash
# Variables — update these
RESOURCE_GROUP="rg-fabric-demo"
LOCATION="eastus"
SQL_SERVER_NAME="sqlsrv-fabric-demo-$RANDOM"
SQL_DB_NAME="SalesDB"
SQL_ADMIN_USER="sqladmin"
SQL_ADMIN_PASS="P@ssw0rd123!"   # Change this

# Create Resource Group
az group create --name $RESOURCE_GROUP --location $LOCATION

# Create SQL Server (public access will be disabled later)
az sql server create \
  --name $SQL_SERVER_NAME \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --admin-user $SQL_ADMIN_USER \
  --admin-password $SQL_ADMIN_PASS

# Create SQL Database (Basic SKU is fine for demo)
az sql db create \
  --resource-group $RESOURCE_GROUP \
  --server $SQL_SERVER_NAME \
  --name $SQL_DB_NAME \
  --service-objective Basic

# *** Temporarily allow YOUR IP to seed data ***
MY_IP=$(curl -s https://ifconfig.me)
az sql server firewall-rule create \
  --resource-group $RESOURCE_GROUP \
  --server $SQL_SERVER_NAME \
  --name "TempMyIP" \
  --start-ip-address $MY_IP \
  --end-ip-address $MY_IP

echo "SQL Server FQDN: ${SQL_SERVER_NAME}.database.windows.net"
echo "Resource ID: $(az sql server show --name $SQL_SERVER_NAME --resource-group $RESOURCE_GROUP --query id -o tsv)"
```

> 📋 **Copy the Resource ID** — you'll need it for the Managed Private Endpoint setup.

---

### Step 1.2 — Seed Demo Data (Sales Tables)

Connect to the SQL Database via **SSMS, Azure Data Studio, or Azure Portal Query Editor**, then run:

```sql
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
SELECT 'Products' AS TableName, COUNT(*) AS RowCount FROM dbo.Products
UNION ALL SELECT 'Customers', COUNT(*) FROM dbo.Customers
UNION ALL SELECT 'SalesFact', COUNT(*) FROM dbo.SalesFact;
```

---

### Step 1.3 — Disable Public Access on Azure SQL

**After** seeding data, disable public access so only the Managed Private Endpoint can connect:

```bash
# Disable public network access
az sql server update \
  --name $SQL_SERVER_NAME \
  --resource-group $RESOURCE_GROUP \
  --set publicNetworkAccess=Disabled

# Remove the temp firewall rule
az sql server firewall-rule delete \
  --resource-group $RESOURCE_GROUP \
  --server $SQL_SERVER_NAME \
  --name "TempMyIP"

echo "Public access is now DISABLED. Only MPE will work."
```

---

## 📦 PART 2 — Microsoft Fabric Setup

### Step 2.1 — Create Fabric Workspace on F64/Trial Capacity

1. Go to https://app.fabric.microsoft.com
2. Click **Workspaces** → **+ New workspace**
3. Name it: `FabricPrivateDemo`
4. In **Advanced** → set **License mode** to `Fabric Trial` (or your F64 capacity)
5. Click **Apply**

---

### Step 2.2 — Create a Lakehouse

1. Inside `FabricPrivateDemo` workspace → click **+ New item**
2. Select **Lakehouse**
3. Name it: `SalesLakehouse`
4. Click **Create**

---

### Step 2.3 — Configure Managed Private Endpoint to Azure SQL

This is the **key step** your customer wants to see.

1. In the workspace, click **Workspace settings** (gear icon top right)
2. Select **Network security** tab
3. Under **Managed Private Endpoints** → click **+ Create**
4. Fill in:

| Field | Value |
|---|---|
| **Name** | `mpe-azuresql-salesdb` |
| **Resource Identifier** | `/subscriptions/{sub-id}/resourceGroups/rg-fabric-demo/providers/Microsoft.Sql/servers/{sql-server-name}` |
| **Target sub-resource** | `sqlServer` |
| **Request message** | `Fabric Demo - Private connectivity to SalesDB` |

5. Click **Create** → Status shows **Pending**

### Step 2.4 — Approve the Private Endpoint in Azure Portal

1. Go to **Azure Portal** → search for your **SQL Server**
2. Navigate to **Networking** → **Private Access** tab
3. You'll see a pending connection request from Fabric
4. Click **Approve** → add a note → **Yes**
5. Back in Fabric workspace settings, refresh → status changes to ✅ **Approved**

> ⏱️ Allow 2–5 minutes for the endpoint to fully activate.

---

## 📦 PART 3 — Fabric Notebook (PySpark) — Ingest SQL → Lakehouse

### Step 3.1 — Create a Custom Spark Pool (Required for MPE)

Since MPE workspaces can't use Starter Pools:

1. Workspace settings → **Data Engineering/Science** → **Spark compute**
2. Click **+ New pool**
3. Name: `DemoSparkPool`
4. Node family: **Memory Optimized**, Size: **Small (4 vCores)**
5. Min nodes: **1**, Max nodes: **3**
6. Click **Create**

---

### Step 3.2 — Create the Ingestion Notebook

1. In the workspace → **+ New item** → **Notebook**
2. Name it: `01_IngestSQL_to_Lakehouse`
3. Attach it to `SalesLakehouse` (top-left Lakehouse selector)
4. Set the default pool to `DemoSparkPool`

Paste the following into the notebook cells:

---

#### 🔷 Cell 1 — Configuration

```python
# ============================================================
# CELL 1: Configuration
# Update these values for your environment
# ============================================================

SQL_SERVER   = "sqlsrv-fabric-demo-XXXX.database.windows.net"  # ← your server FQDN
SQL_DATABASE = "SalesDB"
SQL_USER     = "sqladmin"
SQL_PASSWORD = "P@ssw0rd123!"   # ← or use Key Vault (see Cell 6)
SQL_PORT     = 1433

# Tables to ingest
TABLES_TO_INGEST = [
    "dbo.Products",
    "dbo.Customers",
    "dbo.SalesFact"
]

JDBC_URL = f"jdbc:sqlserver://{SQL_SERVER}:{SQL_PORT};database={SQL_DATABASE};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

print(f"✅ Config loaded. Target: {SQL_SERVER}/{SQL_DATABASE}")
print(f"   JDBC URL: {JDBC_URL}")
```

---

#### 🔷 Cell 2 — Test Connectivity via MPE

```python
# ============================================================
# CELL 2: Test connectivity (reads row count from each table)
# This validates the Managed Private Endpoint is working
# ============================================================

connection_props = {
    "user":     SQL_USER,
    "password": SQL_PASSWORD,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

print("🔍 Testing connectivity via Managed Private Endpoint...")
print("-" * 60)

for table in TABLES_TO_INGEST:
    try:
        df_test = spark.read.jdbc(
            url=JDBC_URL,
            table=f"(SELECT COUNT(*) AS cnt FROM {table}) AS t",
            properties=connection_props
        )
        count = df_test.collect()[0]["cnt"]
        print(f"  ✅ {table:30s} → {count:>6,} rows")
    except Exception as e:
        print(f"  ❌ {table:30s} → ERROR: {str(e)[:80]}")

print("-" * 60)
print("🔒 Connection routed through Managed Private Endpoint only.")
```

---

#### 🔷 Cell 3 — Ingest All Tables to Lakehouse (Delta)

```python
# ============================================================
# CELL 3: Ingest all tables → Lakehouse Delta tables
# ============================================================

from pyspark.sql.functions import current_timestamp, lit

print("🚀 Starting ingestion: Azure SQL → Fabric Lakehouse")
print("=" * 60)

ingestion_results = []

for full_table_name in TABLES_TO_INGEST:
    schema, table = full_table_name.split(".")
    delta_table_name = f"{schema}_{table}"   # e.g. dbo_SalesFact

    print(f"\n📥 Ingesting: {full_table_name} → {delta_table_name}")

    try:
        # Read from Azure SQL via MPE
        df = spark.read.jdbc(
            url=JDBC_URL,
            table=full_table_name,
            properties=connection_props
        )

        row_count = df.count()
        print(f"   Rows read: {row_count:,}")

        # Add metadata columns
        df = df.withColumn("_ingested_at", current_timestamp()) \
               .withColumn("_source", lit(f"AzureSQL/{SQL_DATABASE}/{full_table_name}"))

        # Write as Delta table to Lakehouse
        df.write \
          .mode("overwrite") \
          .format("delta") \
          .saveAsTable(delta_table_name)

        print(f"   ✅ Written to Delta table: {delta_table_name}")
        ingestion_results.append({"table": full_table_name, "rows": row_count, "status": "SUCCESS"})

    except Exception as e:
        print(f"   ❌ FAILED: {str(e)}")
        ingestion_results.append({"table": full_table_name, "rows": 0, "status": f"FAILED: {str(e)[:60]}"})

print("\n" + "=" * 60)
print("📊 INGESTION SUMMARY:")
for r in ingestion_results:
    icon = "✅" if r["status"] == "SUCCESS" else "❌"
    print(f"  {icon} {r['table']:30s}  {r['rows']:>8,} rows  [{r['status']}]")
```

---

#### 🔷 Cell 4 — Create a Gold Layer (Sales Summary for Power BI)

```python
# ============================================================
# CELL 4: Create Gold Layer - Sales Summary for Power BI
# Joins Products + Customers + SalesFact → gold_SalesSummary
# ============================================================

print("🥇 Building Gold Layer: gold_SalesSummary")

df_sales     = spark.table("dbo_SalesFact")
df_products  = spark.table("dbo_Products")
df_customers = spark.table("dbo_Customers")

# Join into a denormalized summary
df_gold = df_sales \
    .join(df_products,  df_sales.ProductID  == df_products.ProductID,  "left") \
    .join(df_customers, df_sales.CustomerID == df_customers.CustomerID, "left") \
    .select(
        df_sales.SaleID,
        df_sales.SaleDate,
        df_customers.CustomerName,
        df_customers.Region,
        df_customers.Country,
        df_products.ProductName,
        df_products.Category,
        df_products.UnitPrice,
        df_sales.Quantity,
        df_sales.TotalAmount
    )

# Write Gold table
df_gold.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_SalesSummary")

print(f"✅ gold_SalesSummary created with {df_gold.count():,} rows")
display(df_gold.limit(10))
```

---

#### 🔷 Cell 5 — Validate All Delta Tables

```python
# ============================================================
# CELL 5: Validate - List all Delta tables in Lakehouse
# ============================================================

print("📋 Validating Lakehouse tables:")
print("-" * 50)

tables = spark.sql("SHOW TABLES").collect()
for t in tables:
    count = spark.table(t.tableName).count()
    print(f"  📁 {t.tableName:35s} → {count:>8,} rows")

print("-" * 50)
print("✅ All tables validated. Ready for Power BI!")
```

---

#### 🔷 Cell 6 (OPTIONAL) — Use Azure Key Vault Instead of Plaintext Password

```python
# ============================================================
# CELL 6 (OPTIONAL): Retrieve SQL password from Azure Key Vault
# Replace Cell 1's SQL_PASSWORD with this approach
# ============================================================

# Method: mssparkutils (recommended for Fabric notebooks)
# Requires Key Vault to also have an MPE configured in this workspace

SQL_PASSWORD_SECURE = mssparkutils.credentials.getSecret(
    "https://your-keyvault.vault.azure.net/",   # ← your Key Vault URL
    "sql-admin-password"                          # ← secret name
)

print("✅ Password retrieved securely from Key Vault")
print("   (Never log or display the actual password value!)")
```

---

## 📦 PART 4 — Data Pipeline for Scheduled Refresh

### Step 4.1 — Create a Pipeline to Orchestrate the Notebook

1. In the workspace → **+ New item** → **Data pipeline**
2. Name: `pl_IngestSalesData`
3. In the pipeline canvas → **Activities** → drag **Notebook** activity
4. In **Settings**:
   - Notebook: `01_IngestSQL_to_Lakehouse`
   - Workspace: `FabricPrivateDemo`
5. Click **Schedule** → enable schedule (e.g., daily at 6:00 AM)
6. Click **Save** and then **Run** to test

> 💡 This gives your customer the **automated refresh** story — no gateway, no on-prem agent, pure Fabric Managed VNet!

---

## 📦 PART 5 — Power BI Report on the Lakehouse

### Step 5.1 — Auto Semantic Model (No Manual Steps Needed!)

Fabric **automatically creates a default Semantic Model** from your Lakehouse tables.

1. Go to `SalesLakehouse`
2. Click the **SQL analytics endpoint** view (top-right toggle)
3. You'll see all your Delta tables: `gold_SalesSummary`, `dbo_Products`, etc.
4. Click **Reporting** → **New report** (or **New semantic model** to customize)

---

### Step 5.2 — Build the Power BI Report

1. From the Lakehouse SQL endpoint → click **New report**
2. Power BI Report Builder opens in Fabric (no desktop needed!)
3. Build these **3 visuals** for a compelling demo:

#### 📊 Visual 1 — Revenue by Region (Bar Chart)
- Field: `gold_SalesSummary[Region]` on X-axis
- Field: `gold_SalesSummary[TotalAmount]` (Sum) on Y-axis
- Title: **"Revenue by Region"**

#### 📊 Visual 2 — Sales by Product Category (Donut Chart)
- Legend: `gold_SalesSummary[Category]`
- Values: `gold_SalesSummary[TotalAmount]` (Sum)
- Title: **"Revenue by Product Category"**

#### 📊 Visual 3 — Monthly Sales Trend (Line Chart)
- X-axis: `gold_SalesSummary[SaleDate]` (Month hierarchy)
- Y-axis: `gold_SalesSummary[TotalAmount]` (Sum)
- Title: **"Monthly Sales Trend"**

#### 📊 Visual 4 — KPI Cards (top row)
- Total Revenue: `SUM(TotalAmount)`
- Total Orders: `COUNT(SaleID)`
- Avg Order Value: `AVERAGE(TotalAmount)`

4. Save report as: `Sales Dashboard - Fabric Private Demo`

---

### Step 5.3 — Connect via Power BI Desktop (Optional — for Demo Flair)

If your customer wants to see **Power BI Desktop** connecting to the Lakehouse SQL endpoint:

1. Open Power BI Desktop → **Get Data** → **Microsoft Fabric** → **Lakehouses**
2. Sign in with your org credentials (Entra ID — no password prompt!)
3. Select `SalesLakehouse` → `gold_SalesSummary`
4. Choose **DirectQuery** (to show live connectivity) or **Import**

---

## 🔐 Security Architecture Summary (for Customer Storytelling)

```
┌─────────────────────────────────────────────────────────────┐
│                    SECURITY BOUNDARY                         │
│                                                             │
│  Azure SQL DB (Public Access = DISABLED)                    │
│      │                                                       │
│      │  Private Endpoint Connection (TCP/1433)              │
│      │  Traffic never leaves Microsoft backbone             │
│      ▼                                                       │
│  Fabric Managed VNet (auto-provisioned per workspace)       │
│      │                                                       │
│      ├─ Spark Notebook (PySpark JDBC)                       │
│      │        ↓ writes Delta tables                          │
│      └─ Fabric Lakehouse (OneLake / ADLS Gen2)              │
│               ↓ auto semantic model                          │
│           Power BI Report (in-browser, no gateway!)         │
│                                                             │
│  ✅ No Data Gateway  ✅ No Public IP  ✅ No VPN required     │
└─────────────────────────────────────────────────────────────┘
```

---

## 🎯 Demo Script Talking Points

| Point | What to Show |
|---|---|
| **"No gateway needed"** | Workspace settings → Network Security → MPE approved ✅ |
| **"Data never hits public internet"** | SQL Server Networking → Public access = Disabled |
| **"Fully automated"** | Data Pipeline running the notebook on schedule |
| **"Analyst self-service"** | Lakehouse SQL endpoint → New report (no code) |
| **"Live data from private source"** | Power BI report refreshed from Delta tables |

---

## 🚧 Known Gotchas & Troubleshooting

| Issue | Fix |
|---|---|
| **MPE stuck in Pending** | Check Azure SQL → Networking → Private Access — approval may be needed |
| **Spark session slow to start** | Custom pools (required for MPE) take ~2 min to cold start vs. starter pools |
| **JDBC connection refused** | Verify public access is truly disabled (not just firewall rules) AND MPE is `Approved` |
| **`trustServerCertificate=false` error** | Use `hostNameInCertificate=*.database.windows.net` in JDBC URL |
| **Power BI can't see tables** | Ensure Notebook ran successfully and Delta tables exist in Lakehouse |
| **MPE not available in workspace** | Capacity must be F64+ or Fabric Trial — P-SKUs don't support MPE |

---

## 📚 Reference Links

- 📖 [Create and use Managed Private Endpoints – MS Learn](https://learn.microsoft.com/en-us/fabric/security/security-managed-private-endpoints-create)
- 📖 [MPE Overview & Capacity Requirements](https://learn.microsoft.com/en-us/fabric/security/security-managed-private-endpoints-overview)
- 📖 [Spark Connector for SQL Databases (Preview)](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-sql-connector)
- 📖 [Fabric Features by SKU](https://learn.microsoft.com/en-us/fabric/enterprise/fabric-features)
- 🎥 [MPE Setup Video Demo (YouTube)](https://www.youtube.com/watch?v=5vsYmhvICx8)
