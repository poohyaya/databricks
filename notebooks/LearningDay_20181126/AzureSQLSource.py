# Databricks notebook source
# Azure SQL Database Connection Info
jdbcHostname = "kothkimpaasdb1.database.windows.net"
jdbcDatabase = "AdventureWorksLT"
jdbcUsername = "pfesql"
jdbcPassword = "Password1!@#"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# COMMAND ----------

# Push down query
# pushdown query need as result set alias
pushdown_query = "(SELECT * FROM SalesLT.Product WITH (NOLOCK)) product"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

# COMMAND ----------

# CREATE TABLE using JDBC Driver
%sql
CREATE TABLE IF NOT EXISTS jdbcProduct
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://kothkimpaasdb1.database.windows.net:1433;databasename=AdventureWorksLT",
  dbtable "[SalesLT].[Product]",
  user "pfesql",
  password "Password1!@#"
)

# COMMAND ----------

# Show CREATE TABLE statement
%sql
SHOW CREATE TABLE jdbcProduct