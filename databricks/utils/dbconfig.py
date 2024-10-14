# Databricks notebook source
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

database_host = dbutils.secrets.get(scope = "db", key = "server")
database_port = "1433"
database_name = dbutils.secrets.get(scope = "db", key = "database")
user = dbutils.secrets.get(scope = "db", key = "username")
password = dbutils.secrets.get(scope = "db", key = "password")


url = f'jdbc:sqlserver://{database_host}:{database_port};database={database_name};user={user}@olympicswarehouse;password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;'

