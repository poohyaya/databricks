# Databricks notebook source
# Azure SQL Database Connection Info
jdbcHostname = ""
jdbcDatabase = "AdventureWorksLT"
jdbcUsername = "pfesql"
jdbcPassword = ""
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

# MAGIC %sql
# MAGIC CREATE TABLE jdbcProduct
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlserver://kothkimpaasdb1.database.windows.net:1433;databasename=AdventureWorksLT",
# MAGIC   dbtable "[SalesLT].[Product]",
# MAGIC   user "pfesql",
# MAGIC   password "Password1!@#"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM jdbcProduct;