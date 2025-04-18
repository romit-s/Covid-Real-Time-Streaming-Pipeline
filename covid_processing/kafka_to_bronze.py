# Databricks notebook source
#Create your bronze table
# Download Maven package in your cluster: org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

# COMMAND ----------

class Bronze():
    def __init__(self):
        self.base_data_dir = "/FileStore/covid_processing"
        self.BOOTSTRAP_SERVER = "<your-server-info>"
        self.JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
        self.CLUSTER_API_KEY = "<your-user-name>"
        self.CLUSTER_API_SECRET = "<your-password>"

    def ingestFromKafka(self, startingTime = 1):
        return ( spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVER)
                    .option("kafka.security.protocol", "SASL_SSL")
                    .option("kafka.sasl.mechanism", "PLAIN")
                    .option("kafka.sasl.jaas.config", f"{self.JAAS_MODULE} required username='{self.CLUSTER_API_KEY}' password='{self.CLUSTER_API_SECRET}';")
                    .option("maxOffsetsPerTrigger", 30)
                    .option("subscribe", "hospital")
                    .option("startingTimestamp", startingTime)
                    .load()
                )
        
    def getSchema(self):
        return """country String,country_code String,continent String,population bigint,indicator String,daily_count double,case_date date,rate_14_day decimal,source string
            """
        
    def getCases(self, kafka_df):
        from pyspark.sql.functions import from_json 
        return ( kafka_df.select(kafka_df.key.cast("string").alias("key"),
                                 from_json(kafka_df.value.cast("string"), self.getSchema()).alias("value"),
                                 "topic", "timestamp")
        )

    def upsert(self, cases_df, batch_id):        
        cases_df.createOrReplaceTempView("cases_df_temp_view")
        merge_statement = """MERGE INTO total_cases s
                USING cases_df_temp_view t
                ON s.value == t.value AND s.timestamp == t.timestamp
                WHEN MATCHED THEN
                UPDATE SET *
                WHEN NOT MATCHED THEN
                INSERT *
            """
        cases_df._jdf.sparkSession().sql(merge_statement)
         
    def process(self, startingTime = 1):
        print(f"Starting Bronze Stream...", end='')
        kafka_df = self.ingestFromKafka(startingTime)
        print(kafka_df)
        cases_df =  self.getCases(kafka_df)
        sQuery = ( cases_df.writeStream
                            .queryName("bronze-ingestion")
                            .foreachBatch(self.upsert)
                            .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/cases")
                            .outputMode("append")
                            .start()
                )
        print("Done")
        return sQuery

# COMMAND ----------

bronze_layer = Bronze()
bronze_stream = bronze_layer.process()


# COMMAND ----------

# value_schema = bronze_layer.getSchema()
# dbutils.fs.rm("dbfs:/user/hive/warehouse/total_cases", recurse=True)
# spark.sql("DROP TABLe total_cases")
# spark.sql(f"CREATE TABLE total_cases (key STRING, value STRUCT<{value_schema}>, topic STRING, timestamp TIMESTAMP)")

# COMMAND ----------

bronze_stream.stop()

# COMMAND ----------

