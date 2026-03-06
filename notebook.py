# Fabric Notebook (PySpark) - Full load Bronze
server = "mausermsdn.database.windows.net"
database = "mauserdb"

# Tables
t_dimdate = "demo.DimDate"
t_dimproduct = "demo.DimProduct"
t_factsales = "demo.FactSales"

jdbc_url = (
    f"jdbc:sqlserver://{server}:1433;"
    f"database={database};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)

# DEMO auth (use secrets/Entra in production)
jdbc_props = {
    "user": "azureuser",
    "password": "SQL_PASSWORD",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

def load_table(dbtable: str):
    return (spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", dbtable)
            .options(**jdbc_props)
            .load())

df_date = load_table(t_dimdate)
df_prod = load_table(t_dimproduct)
df_sales = load_table(t_factsales)

(df_date.write.mode("overwrite").format("delta").saveAsTable("bronze_dimdate"))
(df_prod.write.mode("overwrite").format("delta").saveAsTable("bronze_dimproduct"))
(df_sales.write.mode("overwrite").format("delta").saveAsTable("bronze_factsales"))

display(df_sales.limit(10))