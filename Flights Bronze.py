# Databricks notebook source
# MAGIC %md
# MAGIC #Data Ingestion

# COMMAND ----------

!pip install kagglehub

import kagglehub

# Download latest version of the flights delay dataset and save into bronze layer
path = kagglehub.dataset_download("sherrytp/airline-delay-analysis")
dbutils.fs.mv(f"file:{path}", "dbfs:/mnt/flightsgreg/bronze/", recurse=True)
print("Path to dataset files:", path)

# COMMAND ----------

!pip install kagglehub

import kagglehub

# Download latest version of the bts-carriers dataset and save it into the bronze layer
path = kagglehub.dataset_download("gltaboada/usdpttransport-bts-carriers")
dbutils.fs.mv(f"file:{path}", "dbfs:/mnt/flightsgreg/bronze/", recurse=True)
print("Path to dataset files:", path)

# COMMAND ----------

# Download latest version of the world airports dataset and save it into the bronze layer
import kagglehub

path = kagglehub.dataset_download("arbazmohammad/world-airports-and-airlines-datasets")
dbutils.fs.mv(f"file:{path}", "dbfs:/mnt/flightsgreg/bronze/", recurse=True)

print("Path to dataset files:", path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Checking if 2009 dataset was successfuly saved and loaded

# COMMAND ----------

first_csv = spark.read.format('csv').options(header='true', infer_schema='true', delimiter=',').load('dbfs:/mnt/flightsgreg/bronze/airline delay analysis/2009.csv')
#second_csv = display(dbutils.fs.ls("dbfs:/mnt/flightsgreg/bronze/airline delay analysis/2009.csv"))

display(first_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC # Change 20 archive name to 2020

# COMMAND ----------

dbutils.fs.mv("dbfs:/mnt/flightsgreg/bronze/airline delay analysis/20.csv", "dbfs:/mnt/flightsgreg/bronze/airline delay analysis/2020.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC # Read some datasets as CSV

# COMMAND ----------

df_2019 = spark.read.format('csv').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/bronze/airline delay analysis/2019.csv")
df_2020 = spark.read.format('csv').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/bronze/airline delay analysis/2020.csv")
df_2018 = spark.read.format('csv').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/bronze/airline delay analysis/2018.csv")
display(df_2020)
#display(df_2019)

# COMMAND ----------

# MAGIC %md
# MAGIC # Find missing columns in 2019

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Checking if unique carriers dataset was successfuly saved and loaded

# COMMAND ----------

carriers = spark.read.format('csv').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/bronze/L_UNIQUE_CARRIERS.csv")

display(carriers)

# COMMAND ----------

# MAGIC %md
# MAGIC # Ver se esses códigos abaixo serão reaproveitados

# COMMAND ----------

#Performed an inner join to change the values of the flights carriers from code to its names
df_2019_r = df_2019_columns_renamed
df_2019_r_alias = df_2019_r.alias("df2019")
carriers_alias = carriers.alias("carriers")
DIVERTED
df_2019_with_operators = df_2019_r_alias.join(carriers_alias, carriers_alias["Code"] == df_2019_r_alias["Operadora_do_voo"], 'inner')

display(df_2019_with_operators)
            

# COMMAND ----------

# Droped code and description column after populate to the "Operadora_de_voo" column the values within "Description"

df_2019_op_names = df_2019_with_operators.withColumn("Operadora_do_voo", df_2019_with_operators["Description"]) \
    .drop("Code").drop("Description")

display(df_2019_op_names)

# COMMAND ----------

# Read the .dat file
airports_df = spark.read.format('csv').options(header='true', inferSchema='true', delimiter=',').load('dbfs:/mnt/flightsgreg/bronze/airports_mod.dat')

# Write the DataFrame back as a .csv file
airports_df.write.format('csv').save('dbfs:/mnt/flightsgreg/bronze/airports_mod.csv')

display(airports_df)
