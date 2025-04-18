# Databricks notebook source
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

# MAGIC %run ./kafka_to_bronze

# COMMAND ----------

# MAGIC %run ./bronze_to_silver

# COMMAND ----------

# MAGIC %run ./silver_to_gold

# COMMAND ----------

BZ = Bronze()
SL = Silver()
GL = Gold()

# COMMAND ----------

BZ.process()
SL.transform()
GL.aggregate()