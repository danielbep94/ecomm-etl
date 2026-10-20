# Databricks notebook source
# MAGIC %md
# MAGIC # ecomm-etl
# MAGIC Ejecuta este notebook para comprobar Python y Spark en Databricks.

# COMMAND ----------

import sys

print(f"Python: {sys.version.split()[0]}")
print(f"Spark: {spark.version}")

# COMMAND ----------

spark.range(5).show()
