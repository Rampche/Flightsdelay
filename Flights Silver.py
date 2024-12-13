# Databricks notebook source
# MAGIC %md
# MAGIC #SILVER LAYER

# COMMAND ----------

#Imports:
from pyspark.sql.functions import *
from pyspark.sql import DataFrame 
from pyspark.sql.types import *

# COMMAND ----------

#Reusable function to save files in format and necessairy layers

def write_to_layer(df_to_save, layer, file_name):
    df_to_save.write \
    .mode("overwrite") \
    .parquet(f"dbfs:/mnt/flightsgreg/{layer}/{file_name}.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC #Data treatment of Aiports dataset

# COMMAND ----------

#Find data types from .dat file
sample_df.printSchema()

# COMMAND ----------

# Read the .dat file as csv
airports_df = spark.read.format('csv').options(header='false', inferSchema='true', delimiter=',').load('dbfs:/mnt/flightsgreg/bronze/airports_mod.dat')
#Here used header as false bc when used as true it used the first line as the column names

display(airports_df)

# COMMAND ----------

#Adjust column names from airport_df and drop the unnecessairy ones
airports_df = (airports_df
 .drop('_c0')
 .withColumnRenamed('_c1', 'nome_do_aeroporto')
 .withColumnRenamed('_c2', 'nome_da_cidade')
 .withColumnRenamed('_c3', 'pais')
 .withColumnRenamed('_c4', 'codigo_aeroporto')
 .drop('_c5')
 .drop('_c6')
 .drop('_c7')
 .drop('_c8')
 .drop('_c9')
 .drop('_c10')
 .drop('_c11')
)

display(airports_df)

# COMMAND ----------

# Leave in the DF only the ones where 'codigo_iata' not null
airports_df_not_null = airports_df.dropna()
display(airports_df_not_null)

# COMMAND ----------

# Remove rows with unencoded characters
final_airports_df = airports_df_not_null.filter(
    ~(col("nome_do_aeroporto") == "PlÃ¡cido de Castro") &
    ~(col("codigo_aeroporto") == "SIA")
)

display(final_airports_df)

# COMMAND ----------

# Save final_airports_df as parquet in silver layer
write_to_layer(final_airports_df, 'silver', 'airports')

# COMMAND ----------

# MAGIC %md
# MAGIC #Treatment of Airline Carriers dataset

# COMMAND ----------

# Read and load airlines carriers dataset
carriers = spark.read.format('csv').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/bronze/L_UNIQUE_CARRIERS.csv")

# COMMAND ----------

# Rename column names from airline carriers  
carriers_updated = (
    carriers
    .withColumnRenamed('Code', 'codigo_operadora_do_voo')
    .withColumnRenamed('Description', 'nome_operadora')
)

display(carriers_updated)

# COMMAND ----------

# Drop any null values from carriers dataframe
carriers_not_null = carriers_updated.dropna()

display(carriers_not_null)

# COMMAND ----------

# Remove duplicated values

carriers_without_duplicates = carriers_not_null.dropDuplicates()

display(carriers_without_duplicates)

# COMMAND ----------

# Save carriers_final as parquet in silver layer
write_to_layer(carriers_final, 'silver', 'carriers')

# COMMAND ----------

# MAGIC %md
# MAGIC # Treatment of Flights datasets - Reading the datasets 

# COMMAND ----------

## Check if the datasets have all the same columns

# List all files in the directory
files = dbutils.fs.ls("dbfs:/mnt/flightsgreg/bronze/airline delay analysis")

# Initialize an empty list to hold column names of the first file
initial_columns = None

# Iterate through each file in the directory
for file in files:
    # Read the current CSV file
    df = spark.read.format('csv').options(header='true', infer_schema='true', delimiter=',').load(file.path)
    
    # If it's the first file, store its column names
    if initial_columns is None:
        initial_columns = df.columns
        print(f"Initial columns set from file: {file.name}")
    else:
        # Compare the columns of the current dataframe with the initial columns
        if set(df.columns) == set(initial_columns):
            print(f"Columns in {file.name} match the initial columns.")
        else:
            print(f"Columns in {file.name} do not match the initial columns.")

# Display the initial columns for reference
print("Initial columns:", initial_columns)

# COMMAND ----------

# Function created to load all datasets

def load_datasets():
    return {year: spark.read.format('csv').options(header='true', infer_schema='true').load(f"dbfs:/mnt/flightsgreg/bronze/airline delay analysis/{year}.csv") for year in range(2009, 2020)}

all_dataframes = load_datasets()

# COMMAND ----------

# Read all datasets
df_2009 = all_dataframes[2009]
df_2010 = all_dataframes[2010]
df_2011 = all_dataframes[2011]
df_2012 = all_dataframes[2012]
df_2013 = all_dataframes[2013]
df_2014 = all_dataframes[2014]
df_2015 = all_dataframes[2015]
df_2016 = all_dataframes[2016]
df_2017 = all_dataframes[2017]
df_2018 = all_dataframes[2018]
df_2019 = all_dataframes[2019]

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Treatment of flight delays - All datasets

# COMMAND ----------

#Treatment of missing columns in 2019.csv and also drop of unnecessairy columns for the analisys

# 2019 will be trated first, bc it have columns that the others don't have
df_2019 = (df_2019
    .withColumnRenamed('OP_UNIQUE_CARRIER','OP_CARRIER')
    .drop('TAXI_OUT', 'WHEELS_OFF', 'WHEELS_ON', 'TAXI_IN', 'ARR_TIME', 'ARR_DELAY', 'CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY', '_c20')
    )

df_2019.show(5)  

# COMMAND ----------

# Function to update every dataset when passed as param named 'df'
 
def dataframe_format(df): 
    df_updated = (df
        .withColumnRenamed('FL_DATE', 'data_do_voo')
        .withColumnRenamed('OP_CARRIER', 'codigo_operadora_do_voo')
        .withColumnRenamed('OP_CARRIER_FL_NUM', 'numero_de_voo_da_operadora')
        .withColumnRenamed('ORIGIN', 'aeroporto_origem')
        .withColumnRenamed('DEST', 'aeroporto_destino')
        .withColumnRenamed('DEP_TIME', 'hora_da_partida')
        .withColumnRenamed('DEP_DELAY', 'atraso_na_partida')
        .withColumnRenamed('AIR_TIME', 'tempo_de_voo')
        .withColumnRenamed('DISTANCE', 'distancia')
        .drop('CRS_DEP_TIME', 'TAXI_OUT', 'WHEELS_OFF', 'WHEELS_ON', 'TAXI_IN', 
              'CRS_ARR_TIME', 'ARR_TIME', 'ARR_DELAY', 'CANCELLED', 'CANCELLATION_CODE', 
              'DIVERTED', 'CRS_ELAPSED_TIME', 'ACTUAL_ELAPSED_TIME', 'CARRIER_DELAY', 
              'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY', 'Unnamed: 27')
    )
    
    # Transformation of 'hora_da_partida' to format HH:MM as string
    df_updated = (df_updated
        .withColumn("hora_da_partida", 
                    lpad(regexp_replace(col("hora_da_partida"), "\\.0", ""), 4, "0"))
        .withColumn("hora_da_partida", 
                    date_format(to_timestamp(col("hora_da_partida"), "HHmm"), "HH:mm"))
        .withColumn("data_do_voo", col("data_do_voo").cast("date"))
        .withColumn("codigo_operadora_do_voo", col("codigo_operadora_do_voo").cast("string"))
        .withColumn("numero_de_voo_da_operadora", col("numero_de_voo_da_operadora").cast("integer"))
        .withColumn("aeroporto_origem", col("aeroporto_origem").cast("string"))
        .withColumn("aeroporto_destino", col("aeroporto_destino").cast("string"))
        .withColumn("atraso_na_partida", col("atraso_na_partida").cast("integer"))
        .withColumn("tempo_de_voo", col("tempo_de_voo").cast("integer"))
        .withColumn("distancia", col("distancia").cast("integer"))
    )
    
    return df_updated


# COMMAND ----------

#Update datasets with the function created above
df_2009_upd = dataframe_format(df_2009)
df_2010_upd = dataframe_format(df_2010)
df_2011_upd = dataframe_format(df_2011)
df_2012_upd = dataframe_format(df_2012)
df_2013_upd = dataframe_format(df_2013)
df_2014_upd = dataframe_format(df_2014)
df_2015_upd = dataframe_format(df_2015)
df_2016_upd = dataframe_format(df_2016)
df_2017_upd = dataframe_format(df_2017)
df_2018_upd = dataframe_format(df_2018)
df_2019_upd = dataframe_format(df_2019)

# COMMAND ----------

# Drop all null rows from every df
df_2009_cleaned = df_2009_upd.dropna()
df_2010_cleaned = df_2010_upd.dropna()
df_2011_cleaned = df_2011_upd.dropna()
df_2012_cleaned = df_2012_upd.dropna()
df_2013_cleaned = df_2013_upd.dropna()
df_2014_cleaned = df_2014_upd.dropna()
df_2015_cleaned = df_2015_upd.dropna()
df_2016_cleaned = df_2016_upd.dropna()
df_2017_cleaned = df_2017_upd.dropna()
df_2018_cleaned = df_2018_upd.dropna()
df_2019_cleaned = df_2019_upd.dropna()

# COMMAND ----------

# Test to verify the time to run this query

#Test to filter the top five flights with departure date higher than the fifth day of january
test_filtered_df = df_2019_cleaned.filter(df_2019_cleaned.data_do_voo > "2009-01-05")

# Tested this to force the code to loop trough all the columns and find the last five flights. 
# The main goal was to see the time it would take to perform this simple query.
last_flights_of_year = df_2019_cleaned.filter(
    (col("data_do_voo") > "2009-12-30") & (col("hora_da_partida") > "23:00")
).orderBy(col("hora_da_partida").desc())

test_filtered_df.show(5)

last_flights_of_year.show(5)

# COMMAND ----------

# Save all dataframes as parquet in the silver layer
write_to_layer(df_2009_cleaned, "silver", "df_2009_final")
write_to_layer(df_2010_cleaned, "silver", "df_2010_final")
write_to_layer(df_2011_cleaned, "silver", "df_2011_final")
write_to_layer(df_2012_cleaned, "silver", "df_2012_final")
write_to_layer(df_2013_cleaned, "silver", "df_2013_final")
write_to_layer(df_2014_cleaned, "silver", "df_2014_final")
write_to_layer(df_2015_cleaned, "silver", "df_2015_final")
write_to_layer(df_2016_cleaned, "silver", "df_2016_final")
write_to_layer(df_2017_cleaned, "silver", "df_2017_final")
write_to_layer(df_2018_cleaned, "silver", "df_2018_final")
write_to_layer(df_2019_cleaned, "silver", "df_2019_final")

# COMMAND ----------

df_2009_final = spark.read.format('parquet').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/silver/df_2009_final.parquet")
df_2010_final = spark.read.format('parquet').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/silver/df_2010_final.parquet")
df_2011_final = spark.read.format('parquet').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/silver/df_2011_final.parquet")
df_2012_final = spark.read.format('parquet').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/silver/df_2012_final.parquet")
df_2013_final = spark.read.format('parquet').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/silver/df_2013_final.parquet")
df_2014_final = spark.read.format('parquet').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/silver/df_2014_final.parquet")
df_2015_final = spark.read.format('parquet').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/silver/df_2015_final.parquet")
df_2016_final = spark.read.format('parquet').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/silver/df_2016_final.parquet")
df_2017_final = spark.read.format('parquet').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/silver/df_2017_final.parquet")
df_2018_final = spark.read.format('parquet').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/silver/df_2018_final.parquet")
df_2019_final = spark.read.format('parquet').options(header='true', infer_schema='true').load("dbfs:/mnt/flightsgreg/silver/df_2019_final.parquet")


# COMMAND ----------

display(df_2009_final)

# COMMAND ----------

# Union all parquet dataframes into a unique dataframe

dfs_to_union = [df_2009_final, df_2010_final,df_2011_final, df_2012_final, df_2013_final, df_2014_final, df_2015_final,df_2016_final, df_2017_final, df_2018_final, df_2019_final]

unioned_df = dfs_to_union[0]
for df in dfs_to_union[1:]:
    unioned_df = unioned_df.unionAll(df)


# COMMAND ----------

unioned_df.show(5) 
#unioned_df.count()

# COMMAND ----------

last_flights_of_2019 = df_2019_final.filter(
    (df_2019_final.data_do_voo > "2019-12-30") &
    (df_2019_final.hora_da_partida > "23:00")
).orderBy(df_2019_final.hora_da_partida.desc())

last_flights_of_2019.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Count of lines for each df, to check if nº of rows are the same after the union

# COMMAND ----------

# Count the number of rows of every dataset.
df_2009_cleaned.count()

# COMMAND ----------

df_2010_cleaned.count()

# COMMAND ----------

df_2011_cleaned.count()

# COMMAND ----------

df_2012_cleaned.count()

# COMMAND ----------

df_2013_cleaned.count()

# COMMAND ----------

df_2014_cleaned.count()

# COMMAND ----------

df_2015_cleaned.count()

# COMMAND ----------

df_2016_cleaned.count()

# COMMAND ----------

df_2017_cleaned.count()

# COMMAND ----------

df_2018_cleaned.count()

# COMMAND ----------

df_2019_cleaned.count()

# COMMAND ----------

df_2020_cleaned.show(5)

# COMMAND ----------

# Save all columns together 
write_to_layer()

# COMMAND ----------

# MAGIC %md
# MAGIC - Tirar nulos ----------------------------- ok
# MAGIC - Fazer union ----------------------------- ok
# MAGIC - Fazer colunas de ano e mês ------------ 
# MAGIC - Particionar por ano e mês -------------- 
# MAGIC - Fazer queries e salvar elas na gold ----- 
# MAGIC

# COMMAND ----------


dataframes = 

# COMMAND ----------

# Leave all dataframes with the same column names
df_2009_updated = (df_2009
    .withColumnRenamed('FL_DATE','data_do_voo')
    .withColumnRenamed('OP_CARRIER','codigo_operadora_do_voo')
    .withColumnRenamed('OP_CARRIER_FL_NUM','numero_de_voo_da_operadora')
    .withColumnRenamed('ORIGIN','aeroporto_origem')
    .withColumnRenamed('DEST','aeroporto_destino')
    .withColumnRenamed('DEP_TIME','hora_da_partida')
    .withColumnRenamed('DEP_DELAY','atraso_na_partida')
    .withColumnRenamed('AIR_TIME', 'tempo_de_voo')
    .withColumnRenamed('DISTANCE', 'distancia')
    .drop('CRS_DEP_TIME', 'TAXI_OUT', 'WHEELS_OFF', 'WHEELS_ON','TAXI_IN', 'CRS_ARR_TIME', 'ARR_TIME', 'ARR_DELAY', 'CANCELLED', 'CANCELLATION_CODE','DIVERTED', 'CRS_ELAPSED_TIME', 'CRS_ELAPSED_TIME', 'ACTUAL_ELAPSED_TIME', 'CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY', 'Unnamed: 27')
    )

df_2009_updated.show()

# COMMAND ----------

# Change data types of the 2009 df

'''df_2009_updated_with_types = (df_2009_updated
    .withColumn('data_do_voo', col("data_do_voo").cast("date", "YYYY-MM-DD"))
    .withColumn('codigo_operadora_do_voo', col("codigo_operadora_do_voo").cast("string"))
    .withColumn('numero_de_voo_da_operadora', col("numero_de_voo_da_operadora").cast("integer"))
    .withColumn('ORIGIN','aeroporto_origem')
    .withColumn('DEST','aeroporto_destino')
    .withColumn('DEP_TIME','hora_da_partida', col("hora_da_partida").cast("time", "HH:MM"))
    .withColumn('DEP_DELAY','atraso_na_partida')
    .withColumn('AIR_TIME', 'tempo_de_voo')
    .withColumn('DISTANCE', 'distancia')                          
                              )'''

df_2009_updated_with_types = (df_2009_updated
    .withColumn("data_do_voo", col("data_do_voo").cast("date"))
    .withColumn("codigo_operadora_do_voo", col("codigo_operadora_do_voo").cast("string"))
    .withColumn("numero_de_voo_da_operadora", col("numero_de_voo_da_operadora").cast("integer"))
    .withColumn('aeroporto_origem', col('aeroporto_origem').cast("string"))
    .withColumn('aeroporto_destino', col('aeroporto_destino').cast("string"))
    .withColumn("hora_da_partida", to_timestamp(col("hora_da_partida"), "HH:mm"))
    .withColumn('atraso_na_partida', col('atraso_na_partida').cast("string"))
    .withColumn('tempo_de_voo', col('tempo_de_voo').cast("string"))
    .withColumn('distancia', col('distancia').cast("string"))                          
                              )
df_2009_updated_with_types.show()

# COMMAND ----------

# Change 2019 data types

# COMMAND ----------

# MAGIC %md
# MAGIC # Treatment of Flights datasets - Treatment of 2019

# COMMAND ----------

# Remove any null values in df_2019

df_2019_nulls_droped = df_2019_columns_renamed.dropna()

df_2019_nulls_droped.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Fazer inner join com o dataset de 2019

# COMMAND ----------

#Performed an inner join to change the values of the flights carriers from code to its names

df_2019_with_carriers = df_2019_columns_renamed.join(carriers_new, on="codigo_operadora_do_voo", how="inner")

display(df_2019_with_carriers)


# COMMAND ----------

# MAGIC %md
# MAGIC # Next Data treatment 

# COMMAND ----------

# Changed data types to prepare the dataframe to sent it to silver layer



# COMMAND ----------

# Fazer função que itera na lista de paths de arquivo csv
# mudança de nome de colunas dentro dessa função 
# renomear codigos 
# schema de dados 

# COMMAND ----------

# ver o que falta para fazer union e salvar particionado em delta 

# COMMAND ----------

# MAGIC %md
# MAGIC # What to do now?
# MAGIC - Change OP carrier code to name
# MAGIC - Remove columns from 2019
# MAGIC - Unite all csv files into one dataframe
# MAGIC - Change column names
# MAGIC - Change FL_date (flight date) format 
# MAGIC - Transfer cleaned and normalized data to silver layer in parquet datatype
# MAGIC - Create tables/schemas in gold layer
# MAGIC
