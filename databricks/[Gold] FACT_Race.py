# Databricks notebook source
# MAGIC %run ./utils/dbconfig

# COMMAND ----------

# MAGIC %run ./utils/functions

# COMMAND ----------

# IMPORTS NECESSÁRIOS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql import functions as F

# COMMAND ----------

# CONFIGURAÇÃO DO SPARK
spark = SparkSession.builder.appName("CreateGoldLayer").getOrCreate()

# COMMAND ----------

# CRIAÇÃO DA TABELA OURO

df_race = load_tables("silver.OLY_race")

df_race = df_race.select(
    "FK_athlete_id", "FK_round_id", "heat", "lane", "position", "time", "wind", "result", "additional_info"
)

df_race = df_race.withColumnRenamed("FK_athlete_id", "NK_athlete") \
    .withColumnRenamed("FK_round_id", "NK_round")

df_race = df_race.withColumn(
    "qualified",
    F.when((col("result") == "Q") | (col("result") == "q"), True).otherwise(False)
).withColumn(
    "qualification_type",
    F.when((col("result") == "Q"), "Place")
     .when((col("result") == "q"), "Time")
     .otherwise(None)
).withColumn(
    "disqualified",
    F.when((col("result") == "DQ") | (col("result") == "DNS"), True).otherwise(False)
).withColumn(
    "disqualification_type",
    F.when((col("result") == "DQ"), "Rule")
     .when((col("result") == "DNS"), "Did Not Started")
     .otherwise(None)
).withColumn(
    "record",
    F.when(
        (col("additional_info") == "SB") | 
        (col("additional_info") == "PB") | 
        (col("additional_info") == "=SB") | 
        (col("additional_info") == "NR"), 
        True
    ).otherwise(False)
).withColumn(
    "record_type",
    F.when((col("additional_info") == "SB"), "Season Best")
     .when((col("additional_info") == "PB"), "Personal Best")
     .when((col("additional_info") == "=SB"), "Equal Season Best")
     .when((col("additional_info") == "NR"), "National Record")
     .otherwise(None)
)

df_gold = df_race.select(
    "NK_athlete", "NK_round", "heat", "lane", "position", "time", "wind", "qualified", 
    "qualification_type", "disqualified", "disqualification_type", "record", "record_type"
).withColumn("insert_date", current_timestamp()) \
 .withColumn("modified_date", current_timestamp()) \
 .withColumn("active", lit(True))

display(df_gold)

# COMMAND ----------

# SALVA NO BANCO

save_tables(df_gold, "gold.OLY_FACT_race")
