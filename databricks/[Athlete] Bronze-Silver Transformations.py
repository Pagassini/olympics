# Databricks notebook source
# MAGIC %run ./utils/dbconfig

# COMMAND ----------

# MAGIC %run ./utils/functions

# COMMAND ----------

# IMPORTS NECESSÁRIOS
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# CONFIGURAÇÃO DO SPARK
spark = SparkSession.builder.appName("TransformationsPìpeline").getOrCreate()

# COMMAND ----------

# CRIAÇÃO DA TABELA ATHLETE
df_bronze = load_tables("bronze.OLY_athletics")

athlete_df = df_bronze.select(
    "Athlete Bib", 
    "Name", 
    "NOC Code", 
    "Event Type"
).withColumnRenamed(
    "Athlete Bib", "id"
).withColumnRenamed(
    "Name", "name"
).withColumnRenamed(
    "NOC Code", "noc_code"
).withColumn(
    "gender", when(df_bronze["Event Type"] == "Men's 100m", "Male").otherwise("Female")
).drop("Event Type").dropDuplicates()


# COMMAND ----------

# SEPARAÇÃO E CORREÇÃO DOS REGISTROS INCORRETOS

athlete_df = athlete_df.filter(
    F.trim(F.col("id")).rlike("^[0-9]+$")
)

corrected_id_map = {
    "LOUIS": "1206",
    "SABINO": "1042",
    "AZU": "690",
    "MULAMBA": "514",
    "BROWN": "452",
}

for incorrect_id, correct_id in corrected_id_map.items():
    athlete_df = athlete_df.withColumn(
        "id", 
        F.when(F.col("id") == incorrect_id, correct_id).otherwise(F.col("id"))
    ).withColumn(
        "name", 
        F.when(F.col("id") == correct_id, 
                 F.concat(F.lit(incorrect_id), F.lit(" "), F.col("name"))).otherwise(F.col("name"))
    )

# COMMAND ----------

# SETANDO OS TIPOS E SALVANDO NO BANCO

df = athlete_df.withColumn("id", col("id").cast("int"))
df = df.na.drop()

save_tables(df, "silver.OLY_athlete")
