from pyspark.sql.functions import col, max as spark_max

server = "YOURSQLSERVER.database.windows.net"
database = "YOURDB"
source_table = "demo.FactSales"
watermark_col = "ModifiedAt"

jdbc_url = (
    f"jdbc:sqlserver://{server}:1433;"
    f"database={database};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)

jdbc_props = {
    "user": "SQL_USER",
    "password": "SQL_PASSWORD",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

control_table = "control_watermarks"
target_table = "bronze_factsales"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_table} (
  table_name STRING,
  last_watermark TIMESTAMP
) USING delta
""")

last_wm = spark.sql(f"""
SELECT COALESCE(MAX(last_watermark), TIMESTAMP('1900-01-01')) AS last_wm
FROM {control_table}
WHERE table_name = '{source_table}'
""").collect()[0]["last_wm"]

query = f"(SELECT * FROM {source_table} WHERE {watermark_col} > '{last_wm}') AS src"

df_inc = (spark.read.format("jdbc")
          .option("url", jdbc_url)
          .option("dbtable", query)
          .options(**jdbc_props)
          .load())

(df_inc.write.mode("append").format("delta").saveAsTable(target_table))

new_wm = df_inc.select(spark_max(col(watermark_col)).alias("m")).collect()[0]["m"]
if new_wm is not None:
    spark.sql(f"DELETE FROM {control_table} WHERE table_name = '{source_table}'")
    spark.sql(f"INSERT INTO {control_table} VALUES ('{source_table}', TIMESTAMP('{new_wm}'))")

display(df_inc.limit(10))