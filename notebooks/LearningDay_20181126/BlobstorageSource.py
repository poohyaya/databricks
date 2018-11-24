# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://targz@kothkimblob3.blob.core.windows.net",
  mount_point = "/mnt/targz",
  extra_configs = {"fs.azure.account.key.kothkimblob3.blob.core.windows.net":"cJ3e0pI/YUj0HmeW6mp8g/gkK/uvqgd1MDaCUFpWfqDDKa+J01H+bEZgvu6fYjix1hUBvXjkdE/2amCnPev/Wg=="})

# COMMAND ----------

# MAGIC %fs ls /mnt/targz/2009/

# COMMAND ----------

# File location and type
file_location = "/mnt/targz/*/*/*"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = "\t"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format("com.databricks.spark.csv") \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table
temp_table_name = "sales"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC select count(*) from sales