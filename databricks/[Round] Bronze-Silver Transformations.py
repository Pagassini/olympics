# Databricks notebook source
# MAGIC %run ./utils/dbconfig

# COMMAND ----------

# MAGIC %run ./utils/functions

# COMMAND ----------

# IMPORTS NECESSÁRIOS
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import split, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import when, col
from pyspark.sql.functions import trim, lower, split
from pyspark.sql.functions import to_date, date_format, regexp_replace

# COMMAND ----------

# CONFIGURAÇÃO DO SPARK
spark = SparkSession.builder.appName("TransformationsPìpeline").getOrCreate()

# COMMAND ----------


# CONFIGURAÇÃO DA TABELA DE ROUND

df_bronze = load_tables("bronze.OLY_athletics")

df_bronze = df_bronze.withColumn(
    "event",
    when(col("Event Type").like("%Men's%"), "Men's")
    .when(col("Event Type").like("%Women's%"), "Women's")
)

df_bronze = df_bronze.withColumn(
    "Event Type",
    when(col("Event Type").like("%100m"), "100m")
)

df_round = df_bronze.select("Round", "Date", "Location", "Event", "Event Type").distinct()

df_round = df_round \
    .withColumnRenamed("Round", "round") \
    .withColumnRenamed("Location", "location") \
    .withColumnRenamed("Event", "event") \
    .withColumnRenamed("Event Type", "event_type") \
    .withColumnRenamed("Date", "date")

display(df_round)


# COMMAND ----------

# CONVERTER DATA

df_round = df_round.withColumn(
    "date_clean", 
    regexp_replace(df_round["Date"], "^[A-Z]{3} ", "")
)

df_round = df_round.withColumn(
    "date", 
    date_format(to_date(df_round["date_clean"], "d MMM yyyy"), "dd/MM/yy")
)

df_round = df_round.drop("date_clean")

# COMMAND ----------

# CORRIGIR LOCALIZAÇÃO

df_round = df_round.withColumn(
    "location", 
    split(df_round["location"], " Athlétisme").getItem(0)
)

# COMMAND ----------

# ADICIONA O ID E SALVA NO BANCO

df_round = df_round.withColumn("id", monotonically_increasing_id())

save_tables(df_round, "silver.OLY_round")
