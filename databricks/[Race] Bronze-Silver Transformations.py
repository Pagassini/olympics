# Databricks notebook source
# MAGIC %run ./utils/dbconfig

# COMMAND ----------

# MAGIC %run ./utils/functions

# COMMAND ----------

# IMPORTS NECESSÁRIOS
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.functions import split, trim, regexp_replace, concat, lit

# COMMAND ----------

# CONFIGURAÇÃO DO SPARK
spark = SparkSession.builder.appName("TransformationsPìpeline").getOrCreate()

# COMMAND ----------

df_bronze = load_tables("bronze.OLY_athletics")
df_athlete = load_tables("silver.OLY_athlete") 
df_round = load_tables("silver.OLY_round")

df_race = df_bronze.select("Heat", "Lane", "Pos", "Time", "Milliseconds", "Wind", "Result", "Additional Info", "Athlete Bib", "Round", "Event Type") \
                   .withColumn("event", trim(split(df_bronze["Event Type"], " ")[0])) \
                   .withColumn("event_type", trim(split(df_bronze["Event Type"], " ")[1]))

df_race = df_race \
    .withColumnRenamed("Heat","heat") \
    .withColumnRenamed("Lane","lane") \
    .withColumnRenamed("Pos","position") \
    .withColumnRenamed("Time","time") \
    .withColumnRenamed("Milliseconds","milliseconds") \
    .withColumnRenamed("Wind","wind") \
    .withColumnRenamed("Result","result") \
    .withColumnRenamed("Additional Info","additional_info") \
    .withColumnRenamed("Athlete Bib","athlete_bib") \
    .withColumnRenamed("Round","round")

df_race_with_athlete = df_race.join(df_athlete, df_race["athlete_bib"] == df_athlete["id"], "left") \
                              .select(df_race["*"], df_athlete["id"].alias("FK_athlete_id"))

df_race_with_round = df_race_with_athlete.join(df_round, 
                                               (df_race_with_athlete["round"] == df_round["round"]) & 
                                               (df_race_with_athlete["event"] == df_round["event"]) & 
                                               (df_race_with_athlete["event_type"] == df_round["event_type"]), 
                                               "left") \
                              .select(df_race_with_athlete["*"], df_round["id"].alias("FK_round_id"))

df_race = df_race_with_round.drop("athlete_bib", "round", "event_type", "event", "Event Type")

display(df_race)

# COMMAND ----------

# CORREÇÃO DOS VALORES DE ATHLETE ID NULOS

corrections_map = {
    ("5", "5"): 514,
    ("2", "3"): 1042,
    ("1", "4"): 690,
    ("8", "7"): 452,
    ("8", "8"): 514,
    ("5", "8"): 1206,
    ("2", "5"): 2055,
}

def corrections(heat, lane):
    return corrections_map.get((heat, lane), None)

corrections_udf = udf(corrections, IntegerType())

df_race = df_race.withColumn(
    "FK_athlete_id",
    when(df_race["FK_athlete_id"].isNull(), corrections_udf(df_race["Heat"], df_race["Lane"]))
    .otherwise(df_race["FK_athlete_id"])
)

# COMMAND ----------

# CORRIGIR CAMPOS INCORRETOS

corrections_map = {
    514: 0,
    1206: 0,
    2055: 0,
    1042: 0,
    690: 0,
    452: 0,
}

df_race = df_race.withColumn(
    "result", 
    F.when(F.col("position") == "DNS", "DNS")
    .when(F.col("position") == "DQ", "DQ")
    .when(F.col("position").rlike("F1"), "DQ")
    .otherwise(F.col("result"))
)

for pos_id, pos in corrections_map.items():
    df_race = df_race.withColumn(
        "position", 
        F.when(F.col("FK_athlete_id") == pos_id, F.lit(pos)).otherwise(F.col("position"))
    ).withColumn(
        "time", 
        F.when(F.col("FK_athlete_id") == pos_id, F.lit(0)).otherwise(F.col("time"))
    ).withColumn(
        "wind", 
        F.when(F.col("FK_athlete_id") == pos_id, F.lit(0.0)).otherwise(F.col("wind"))
    )

# COMMAND ----------

# CORREÇÕES DO HEAT E POS NULOS

df_race = df_race.withColumn("heat", 
    F.when(F.col("Heat").isNull(), F.lit(1)).otherwise(F.col("Heat"))
)

df_final = df_race.filter(F.col("position").isNull())

window_spec = Window.orderBy(
    F.when(F.col("result") == "Q", 1).otherwise(2),  
    F.col("time").asc()  
)

df_final = df_final.withColumn("pos_final", F.row_number().over(window_spec))

df_race = df_race.join(
    df_final.select("FK_athlete_id", "pos_final"),  
    on="FK_athlete_id",
    how="left"
).withColumn(
    "position", 
    F.coalesce(F.col("pos_final"), F.col("position"))
).drop("pos_final")

# COMMAND ----------

# CONCATENANDO TIME E MILLISECONDS

df_race = df_race.withColumn(
    "time",
    when(col("Milliseconds").isNotNull(), concat(col("time"), col("Milliseconds"))).otherwise(concat(col("time"), lit(".000")))
)

df_race = df_race.drop("Milliseconds")

# COMMAND ----------

# COMPLETANDO A TABELA RESULTS

df_race = df_race.withColumn("result",
when(col("result").isNull(), "NQ").otherwise(col("result")))

display(df_race)

# COMMAND ----------

# CONVERTE OS TIPOS E SALVA NO BANCO

df_cleaned = df_race \
    .withColumn("heat", col("Heat").cast("int")) \
    .withColumn("lane", col("Lane").cast("int")) \
    .withColumn("position", col("position").cast("int")) \
    .withColumn("time", col("Time").cast("string")) \
    .withColumn("wind", col("Wind").cast("string")) \
    .withColumn("result", col("Result").cast("string")) \
    .withColumn("additional_info", col("additional_info").cast("string"))

save_tables(df_cleaned, "silver.OLY_race")
