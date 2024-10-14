# Databricks notebook source
# MAGIC %run ./utils/dbconfig

# COMMAND ----------

# MAGIC %run ./utils/functions

# COMMAND ----------

# IMPORTS NECESSÁRIOS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

# COMMAND ----------

# CONFIGURAÇÃO DO SPARK
spark = SparkSession.builder.appName("CreateGoldLayer").getOrCreate()

# COMMAND ----------

# CRIAÇÃO DA TABELA OURO

df_athlete = load_tables("silver.OLY_athlete")

df_gold = df_athlete \
    .withColumn("insert_date", current_timestamp()) \
    .withColumn("modified_date", current_timestamp()) \
    .withColumn("active", lit(True))

# COMMAND ----------

# SALVA NO BANCO

save_tables(df_gold, "gold.OLY_DIM_athlete")
