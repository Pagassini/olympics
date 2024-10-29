# Databricks notebook source
# IMPORTS NECESSÁRIOS
import hashlib
from pyspark.sql.functions import col

# COMMAND ----------

#FUNÇÕES
def load_tables(tablename):
    df = (spark.read
          .format("jdbc")
          .option("url", url)
          .option("dbtable", tablename)
          .option("user", user)
          .option("password", password)
          .option("driver", driver)
          .load())
    
    return df

def save_tables(df, tablename):
  try:
    df.write \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", tablename) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", driver) \
    .save()
  except Exception as e:
    print(f"Erro ao salvar a tabela {tablename}: {e}")


def generate_hash(id_name):
    hash_id = hashlib.sha256(str(id_name).encode('utf-8'))
    return hash_id.digest()
  
def load_query(query):
      df = (spark.read
          .format("jdbc")
          .option("url", url)
          .option("query", query)
          .option("user", user)
          .option("password", password)
          .option("driver", driver)
          .load())
      return df


