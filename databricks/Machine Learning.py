# Databricks notebook source
# MAGIC %run ./utils/dbconfig

# COMMAND ----------

# MAGIC %run ./utils/functions

# COMMAND ----------

# IMPORTS NECESSÁRIOS
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import col, regexp_replace, when, avg, count, min, expr
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

# COMMAND ----------

# INICIA A SESSÃO SPARK
spark = SparkSession.builder.appName("TransformationsPìpeline").getOrCreate()

# COMMAND ----------

# CONFIGURAÇÃO DA QUERY
df = load_query('SELECT * FROM [gold].[OLY_FACT_race] WHERE NK_round != 2 AND NK_round != 3')

display(df)

# COMMAND ----------

# INFORMAÇÕES SOBRE A TABELA
df.printSchema()
df.describe()

# COMMAND ----------

# ANALISE DOS QUALIFIEDS
pandas_df = df.toPandas()
sns.countplot(x='qualified', data=pandas_df)
plt.show()

sns.countplot(x='qualification_type', data=pandas_df)
plt.show()

# COMMAND ----------

# ANALISE DOS DISQUALIFIEDS
sns.countplot(x='disqualified', data=pandas_df)
plt.show()

sns.countplot(x='disqualification_type', data=pandas_df)
plt.show()

# COMMAND ----------

# ANALISE DOS RECORDS
sns.countplot(x='record', data=pandas_df)
plt.show()

sns.countplot(x='record_type', data=pandas_df)
plt.show()

# COMMAND ----------

# CORRIGINDO O TIPO DAS VARIAVEIS

df_fixed = df.withColumn('rank', df['rank'].cast('int')) \
       .withColumn("time", regexp_replace(col("time"), r"\.", "").cast("float")) \

display(df_fixed)

# COMMAND ----------

# VERIFICAÇÃO DAS VARIÁVEIS NUMÉRICAS
num_variables = []
for i in df_fixed.columns:
    if dict(df_fixed.dtypes)[i] in ['int', 'bigint', 'double', 'float']:
        print(i, ':', dict(df.dtypes)[i])
        num_variables.append(i)

# COMMAND ----------

# ANALISE DE OUTLIERS
pandas_df = df_fixed.toPandas()
plt.rcParams["figure.figsize"] = (14, 24)
plt.rcParams["figure.autolayout"] = True
f, axes = plt.subplots(4, 4)

row = 0
column = 0

for i in num_variables:
    sns.boxplot(data=pandas_df, y=i, ax=axes[row, column])
    column += 1
    if column == 4:
        column = 0
        row += 1
    if row == 4:
        break
plt.show()

# COMMAND ----------

df_final = pandas_df.loc[pandas_df['rank'] < 452]

X = df_final[['position', 'time']]
y = (df_final['position'] == 1).astype(int)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

df_final['win_probability'] = model.predict_proba(X)[:, 1]

top_3_candidates = df_final.nlargest(3, 'win_probability')

top_3_candidates = top_3_candidates[['NK_athlete', 'position', 'time', 'win_probability']]

print("Top 3 Candidatos para a Final:")
print(top_3_candidates)

# COMMAND ----------

print(top_3_candidates)

# COMMAND ----------

#SALVA A TABELA NO BANCO
df = spark.createDataFrame(top_3_candidates)
save_tables(df, "ml.results")
